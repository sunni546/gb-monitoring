#!/bin/bash

TARGET_DIR="/data/raw_logs/edge"
BACKUP_DIR="/data/bak_logs/edge"
LOG_FILE="/data/bak_logs/merge_edge_logs.log"
POSITION_FILE="/home/icits/log_analysis/analysis/promtail/data/promtail-positions.yaml"

echo "[$(date)] Log merging process started." >> $LOG_FILE

# 로그 파일 검색 및 처리
find "$TARGET_DIR" -type f -name "*.log" | sort | while read -r file; do
    [ -e "$file" ] || continue # 파일이 존재하지 않으면 건너뜀

    # 원본 디렉토리 구조 유지
    relative_dir=$(dirname "${file#$TARGET_DIR/}")
    merged_dir="$BACKUP_DIR/$relative_dir"
    mkdir -p "$merged_dir"

    # 파일명에서 날짜와 서비스명 추출
    filename=$(basename "$file")
    if [[ "$filename" =~ ^([0-9]{8})_[0-9]{6}_(.*)_log\.log$ ]]; then
        LOG_DATE="${BASH_REMATCH[1]}"
        SERVICE_NAME="${BASH_REMATCH[2]}"
    else
        echo "[$(date)] Skipping invalid log filename: $file" >> $LOG_FILE
        continue
    fi

    # 병합될 파일 경로 (날짜별 저장)
    merged_file="$merged_dir/bak_${LOG_DATE}_${SERVICE_NAME}.log"

    # Promtail이 다 읽었는지 확인
    LOG_SIZE=$(stat -c %s "$file") # 원본 로그 파일 크기
    # 절대 경로를 맞춰서 변경
    FILE_PATH=$(echo "$file" | sed 's|/home/log_analysis/analysis/logs/|/var/log/edge/|')
    # 위치 정보 추출
    READ_OFFSET=$(grep "$FILE_PATH" "$POSITION_FILE" | awk -F '"' '{print $2}')


    if [[ "$LOG_SIZE" -gt 0 && "$LOG_SIZE" -eq "$READ_OFFSET" ]]; then
        cat "$file" >> "$merged_file"
        rm -f "$file"  # 원본 삭제
        echo "[$(date)] Merged: $file → $merged_file" >> $LOG_FILE
    else
        echo "[$(date)] Promtail is still reading: $file" >> $LOG_FILE
    fi
done

# 병합된 파일 크기 출력
find "$BACKUP_DIR" -type f -name "bak_*.log" | while read -r merged_file; do
    FILE_SIZE=$(du -h "$merged_file" | cut -f1)
    echo "[$(date)] Final merged file: $merged_file ($FILE_SIZE)" >> $LOG_FILE
done

echo "[$(date)] Log merging process completed." >> $LOG_FILE
