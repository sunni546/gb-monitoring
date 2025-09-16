#!/bin/bash

TARGET_DIR="/data/raw_logs/edge"   # 원본 로그 디렉토리
BACKUP_DIR="/data/bak_logs/edge"   # 백업 파일 저장 디렉토리
LOG_FILE="/data/bak_logs/backup_edge_log.log"  # 백업 과정 기록 파일

echo "[$(date)] Backup process started." >> $LOG_FILE

# 어제부터 30일 전까지 날짜 계산
for i in {1..30}; do
    TARGET_DATE=$(date -d "$i days ago" +"%Y%m%d")  # 날짜 계산

    echo "[$(date)] Backup process started for $TARGET_DATE." >> $LOG_FILE

    # 해당 날짜로 시작하는 모든 로그 파일 찾기
    log_files=$(find "$TARGET_DIR" -type f -name "${TARGET_DATE}_*.log")

    # 백업할 파일이 하나라도 있는지 확인
    if [[ -z "$log_files" ]]; then
        echo "[$(date)] No logs found for backup: $TARGET_DIR" >> $LOG_FILE
        echo "[$(date)] Backup process finished for $TARGET_DATE." >> $LOG_FILE
        continue
    fi

    # 로그 파일이 있는 경우만 실행
    echo "$log_files" | while read -r file; do
        # 원본 디렉토리 구조 유지
        relative_dir=$(dirname "${file#$TARGET_DIR/}")
        backup_subdir="$BACKUP_DIR/$relative_dir"
        mkdir -p "$backup_subdir"

        # 파일 이름에서 날짜, 시간, 확장자 제거
        filename=$(basename "$file")
        clean_filename=$(echo "$filename" | sed -E "s/^${TARGET_DATE}_[0-9]{6}_//; s/\.log$//")
        backup_file="$backup_subdir/bak_${TARGET_DATE}_${clean_filename}.tar.gz"

        # 동일한 로그 유형의 파일들을 찾기
        files_to_compress=()
        while IFS= read -r match; do
            files_to_compress+=("$match")
        done < <(find "$(dirname "$file")" -type f -name "${TARGET_DATE}_[0-9][0-9][0-9][0-9][0-9][0-9]_${clean_filename}.log")

        # 파일이 존재하면 압축 후 원본 삭제
        if [[ ${#files_to_compress[@]} -gt 0 ]]; then
            tar -czf "$backup_file" -C "$(dirname "$file")" "${files_to_compress[@]##*/}" && \
            rm -f "${files_to_compress[@]}"

            # 압축 파일 크기와 파일 수 출력
            FILE_SIZE=$(du -h "$backup_file" | cut -f1)
            echo "[$(date)] Backup completed: $backup_file ($FILE_SIZE, ${#files_to_compress[@]} files)" >> $LOG_FILE
        fi
    done

    echo "[$(date)] Backup process finished for $TARGET_DATE." >> $LOG_FILE
done

echo "[$(date)] Backup process finished." >> $LOG_FILE
