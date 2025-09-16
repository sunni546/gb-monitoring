#!/bin/bash

TARGET_DIR="/data/raw_logs/edge"    # 원본 로그 디렉토리
BACKUP_DIR="/data/bak_logs/edge"    # 백업 파일 저장 디렉토리
RAW_LOG_FILE="/data/bak_logs/raw_logs.log"   # 원본 로그 삭제 기록
BACKUP_LOG_FILE="/data/bak_logs/backup_logs.log"  # 백업 로그 삭제 기록
YESTERDAY=$(date -d "yesterday" +"%Y%m%d")  # 어제 날짜 (YYYYMMDD)
DAYS_AGO=$(date -d "30 days ago" +"%Y%m%d")  # 30일 전 날짜 (YYYYMMDD)

echo "[$(date)] Starting backup log management..." >> "$BACKUP_LOG_FILE"

# 1. BACKUP_DIR에서 어제 날짜의 .log 파일을 .tar.gz로 압축
echo "[$(date)] Starting compression of backup logs..." >> "$BACKUP_LOG_FILE"
find "$BACKUP_DIR" -type f \( -name "bak_${YESTERDAY}_*_log.log" \) | while read FILE; do
    # 압축 파일 경로 생성
    TAR_FILE="${FILE%.log}.tar.gz"

    # 압축 시도
    if ! tar -czf "$TAR_FILE" -C "$(dirname "$FILE")" "$(basename "$FILE")"; then
        echo "[$(date)] Failed to compress: $FILE" >> "$BACKUP_LOG_FILE"
    else
        # 압축이 성공하면 원본 .log 파일 삭제
        if ! rm -f "$FILE"; then
            echo "[$(date)] Failed to delete: $FILE after compression" >> "$BACKUP_LOG_FILE"
        fi
    fi
done
echo "[$(date)] Completed compression of backup logs." >> "$BACKUP_LOG_FILE"

# 2. TARGET_DIR에서 어제 이전의 원본 로그 삭제
echo "[$(date)] Starting raw log deletion process..." >> "$RAW_LOG_FILE"

find "$TARGET_DIR" -type f \( -name "*_log.log" \) | while read FILE; do
    FILE_DATE=$(basename "$FILE" | grep -oE '[0-9]{8}')
    if [[ "$FILE_DATE" -le "$YESTERDAY" ]]; then
        if ! rm -f "$FILE"; then
            echo "[$(date)] Failed to delete raw log: $FILE" >> "$RAW_LOG_FILE"
        fi
    fi
done

echo "[$(date)] Raw log deletion process completed." >> "$RAW_LOG_FILE"

# 3. BACKUP_DIR에서 30일 전의 .tar.gz 백업 로그 삭제
echo "[$(date)] Starting deletion of old backup tar.gz logs..." >> "$BACKUP_LOG_FILE"
find "$BACKUP_DIR" -type f \( -name "bak_*_log.tar.gz" \) | while read FILE; do
    FILE_DATE=$(basename "$FILE" | grep -oE '[0-9]{8}')
    if [[ "$FILE_DATE" -le "$DAYS_AGO" ]]; then
        if ! rm -f "$FILE"; then
            echo "[$(date)] Failed to delete backup tar.gz log: $FILE" >> "$BACKUP_LOG_FILE"
        fi
    fi
done
echo "[$(date)] Completed deletion of old backup tar.gz logs." >> "$BACKUP_LOG_FILE"

echo "[$(date)] Backup log management process completed." >> "$BACKUP_LOG_FILE"
