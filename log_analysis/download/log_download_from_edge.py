import os
import re
import shutil
import hashlib
import logging

from datetime import datetime
from scp import SCPClient
from pathlib import Path


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def get_timestamped_filename(filename):
    """
    파일 이름에 타임스탬프 추가
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base, ext = os.path.splitext(filename)
    return f"{timestamp}_{base}{ext}"


def calculate_md5(file_path):
    """
    파일의 MD5 체크섬 계산
    """
    try:
        with open(file_path, "rb") as file:
            md5_hash = hashlib.md5()
            for chunk in iter(lambda: file.read(4096), b""):
                md5_hash.update(chunk)
            return md5_hash.hexdigest()
    except Exception as e:
        logging.error(f"Error calculating MD5 for {file_path}: {e}")
        return None


def remote_md5(ssh, file_path):
    """
    원격 서버에서 MD5 체크섬 계산
    """
    command = f"md5sum '{file_path}' | awk '{{print $1}}'"
    stdin, stdout, stderr = ssh.exec_command(command)
    result = stdout.read().decode().strip()
    error = stderr.read().decode().strip()
    if error:
        logging.error(f"Error calculating remote MD5 for {file_path}: {error}")
        return None
    return result


def rename_remote_file(ssh, host, remote_file_path, new_file_path, file_name):
    """
    원격 파일 이름 변경
    """
    try:
        rename_command = f"mv '{remote_file_path}' '{new_file_path}'"
        stdin, stdout, stderr = ssh.exec_command(rename_command, timeout=10)
        rename_error = stderr.read().decode().strip()
        if rename_error:
            logging.error(f"Error renaming file {file_name} on {host}: {rename_error}")
            return False
        logging.info(f"Renamed file: {file_name} -> {new_file_path} on {host}")
        return True
    except Exception as e:
        logging.error(f"Error renaming file {file_name} on {host}: {e}")
        return False


def download_file(ssh, host, remote_file_path, local_file_path):
    """
    파일 다운로드
    """
    try:
        with SCPClient(ssh.get_transport(), socket_timeout=10 * 60) as scp:
            scp.get(remote_file_path, local_file_path)
            logging.info(f"Downloaded file to {local_file_path}")
            return True
    except Exception as e:
        logging.error(f"Error downloading file {remote_file_path} from {host}: {e}")
        return False


def verify_md5(local_md5_hash, remote_md5_hash, file_name, host):
    """
    MD5 체크섬 검증
    """
    try:
        if local_md5_hash != remote_md5_hash:
            logging.error(f"local_md5_hash: {local_md5_hash}\nremote_md5_hash: {remote_md5_hash}")
            logging.error(f"MD5 mismatch for {file_name} on {host}. Skipping deletion.")
            return False
        logging.info(f"MD5 verified for {file_name} on {host}.")
        return True
    except Exception as e:
        logging.error(f"Error verifying MD5 for {file_name} on {host}: {e}")
        return False


def delete_remote_file(ssh, remote_file_path, file_name, host):
    """
    원격 파일 삭제
    """
    try:
        delete_command = f"rm '{remote_file_path}'"
        ssh.exec_command(delete_command, timeout=10)
        logging.info(f"Deleted file: {file_name} on {host}")
        return True
    except Exception as e:
        logging.error(f"Error deleting file {file_name} on {host}: {e}")
        return False


def move_local_file(source_path: Path, destination_path: Path):
    """
    로컬 파일을 지정된 경로로 이동
    """
    try:
        shutil.move(str(source_path), str(destination_path))
        logging.info(f"File {source_path.name} successfully moved to {destination_path}.")
    except Exception as e:
        logging.error(f"Failed to move file {source_path.name} to {destination_path}: {e}")


def create_backup_dir(final_local_dir: Path) -> Path:
    """
    raw_logs 경로를 bak_logs 경로로 변환 후 백업 디렉토리 생성
    """
    backup_dir = Path(str(final_local_dir).replace('raw_logs', 'bak_logs'))
    backup_dir.mkdir(parents=True, exist_ok=True)
    return backup_dir


def parse_file_name(file_name: str) -> tuple[str, str] | None:
    """
    파일명에서 날짜(YYYYMMDD)와 원본 파일명 추출
    """
    match = re.match(r"(\d{8})_\d{6}_(.*)", file_name)
    return match.groups() if match else None


def copy_file_to_backup(src_path: Path, dest_path: Path):
    """
    원본 파일을 백업 파일로 복사 (대용량 파일 고려하여 4KB 단위로 복사)
    """
    with open(dest_path, "ab") as backup_file, open(src_path, "rb") as original_file:
        while chunk := original_file.read(4096):  # 4KB
            backup_file.write(chunk)

    backup_file.write(b"\n")


def backup_log_file(file_path: Path, final_local_dir: Path, file_name: str):
    """
    로그 파일을 백업 디렉토리로 저장 (bak_YYYYMMDD_파일명.log)
    """
    try:
        # 백업 디렉토리 생성
        backup_dir = create_backup_dir(final_local_dir)

        # 파일명에서 날짜 및 원본 파일명 추출
        parsed_data = parse_file_name(file_name)
        if not parsed_data:
            logging.error(f"Invalid file name format: {file_name}")
            return

        extracted_date, original_name = parsed_data
        backup_file_path = backup_dir / f"bak_{extracted_date}_{original_name}"

        # 파일을 백업 디렉토리로 복사
        copy_file_to_backup(file_path, backup_file_path)

        logging.info(f"Successfully backed up {file_path} to {backup_file_path}")

    except Exception as e:
        logging.error(f"Failed to backup file {file_path}: {e}")


def process_single_file(ssh, host, remote_dir, local_dir, file_name, final_local_dir, mapped_folder):
    """
    단일 파일 처리
    """
    try:
        # 원격 파일 경로 정의
        remote_file_path = f"{remote_dir}/{file_name}"

        # '_RF_' 포함 시 삭제 단계부터 실행
        if '_RF_' in file_name:
            logging.info(f"File {file_name} contains '_RF_'. Proceeding with deletion.")
            new_file_name = file_name.split("_RF_")[-1]
            local_file_path = Path(local_dir) / new_file_name

            if not retry_action(delete_remote_file, ssh, remote_file_path, file_name, host):
                handle_failed_action(ssh, host, remote_file_path, file_name, f"{mapped_folder}_RF", remote_dir)
                return

        else:
            # '_DF_' 포함 시 이름 변경 후 'MD5 계산 (원격 파일)' 단계부터 실행
            if '_DF_' in file_name:
                logging.info(f"File {file_name} contains '_DF_'. Renaming and continuing from MD5 calculation.")
                new_file_name = file_name.split("_DF_")[-1]
                new_file_path = f"{remote_dir}/{new_file_name}"
                if not retry_action(rename_remote_file, ssh, host, remote_file_path, new_file_path, file_name):
                    return
            else:
                # 일반 파일 처리: 이름 변경
                new_file_name = get_timestamped_filename(file_name)
                new_file_path = f"{remote_dir}/{new_file_name}"
                if not retry_action(rename_remote_file, ssh, host, remote_file_path, new_file_path, file_name):
                    return

            # MD5 계산 (원격 파일)
            remote_md5_hash = retry_action(remote_md5, ssh, new_file_path)
            if not remote_md5_hash:
                handle_failed_action(ssh, host, new_file_path, new_file_name, f"{mapped_folder}_DF", remote_dir)
                return

            # 파일 다운로드
            local_file_path = Path(local_dir) / new_file_name
            if not retry_action(download_file, ssh, host, new_file_path, local_file_path):
                handle_failed_action(ssh, host, new_file_path, new_file_name, f"{mapped_folder}_DF", remote_dir)
                return

            # MD5 검증
            local_md5_hash = retry_action(calculate_md5, local_file_path)
            if not local_md5_hash:
                handle_failed_action(ssh, host, new_file_path, new_file_name, f"{mapped_folder}_DF", remote_dir)
                return

            if not verify_md5(local_md5_hash, remote_md5_hash, new_file_name, host):
                handle_failed_action(ssh, host, new_file_path, new_file_name, f"{mapped_folder}_DF", remote_dir)
                return

            # 원격 파일 삭제
            if not retry_action(delete_remote_file, ssh, new_file_path, new_file_name, host):
                handle_failed_action(ssh, host, new_file_path, file_name, f"{mapped_folder}_RF", remote_dir)
                return

        # 로컬 파일 이동
        final_path = Path(final_local_dir) / new_file_name
        move_local_file(local_file_path, final_path)

        # 로그 파일 백업 (병합)
        backup_log_file(final_path, final_local_dir, new_file_name)

    except Exception as e:
        logging.error(f"Error processing file {file_name} on {host}: {e}")
        logging.error("Continuing with next file...")


def retry_action(action, *args, retries=1, **kwargs):
    """
    주어진 작업(action)을 재시도(retries) 횟수만큼 실행
    """
    for attempt in range(retries + 1):
        if action(*args, **kwargs):
            return True
        logging.warning(f"Attempt {attempt + 1} failed for action {action.__name__}. Retrying...")
    return False


def handle_failed_action(ssh, host, remote_file_path, file_name, prefix, remote_dir):
    """
    실패한 작업에 대해 원격 파일 이름을 변경
    """
    logging.error(f"Failed action for file {file_name} on {host}. Renaming with '{prefix}_'.")
    new_failed_name = f"{prefix}_{file_name}"
    new_failed_path = f"{remote_dir}/{new_failed_name}"
    if not rename_remote_file(ssh, host, remote_file_path, new_failed_path, file_name):
        logging.error(f"Renaming failed for {file_name}. Manual intervention may be required.")
