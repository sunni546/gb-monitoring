import os
import time
import json
import paramiko
import logging
import signal
import threading

from paramiko.ssh_exception import SSHException, AuthenticationException, NoValidConnectionsError

from log_download_from_edge import process_single_file


stop_event = threading.Event()  # 종료 플래그


def handle_signal(signum, frame):
    logging.info("Received termination signal. Gracefully shutting down...")
    stop_event.set()  # 종료 플래그 설정


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def timestamp(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logging.info(f"Function {func.__name__} took {end - start} seconds.")
        return result
    return wrapper


def make_local_dir(local_dir):
    """
    로컬 디렉토리 확인/생성
    """
    os.makedirs(local_dir, exist_ok=True)


def load_config(config_file="./edge-server-config.json"):
    """
    설정 파일(edge-server-config.json) 로드
    """
    try:
        with open(config_file, "r", encoding="utf-8") as file:
            config = json.load(file)
            return config

    except FileNotFoundError:
        logging.error(f"설정 파일 '{config_file}'을(를) 찾을 수 없습니다.")
        logging.error(f"Current working directory: {os.getcwd()}")
        return {}
    except json.JSONDecodeError:
        logging.error(f"설정 파일 '{config_file}'의 구문이 잘못되었습니다.")
        return {}
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        return {}


def get_file_list(ssh, host, remote_dir):
    """
    원격 디렉토리 내 파일 목록 수집
    """
    try:
        command = f"ls {remote_dir}"
        stdin, stdout, stderr = ssh.exec_command(command)
        file_list = stdout.read().decode().splitlines()
        logging.info(f"Found {len(file_list)} files in {remote_dir} on {host}")
        return file_list
    except Exception as e:
        logging.error(f"Error retrieving file list from {remote_dir} on {host}: {e}")
        return []


def process_files_on_server(ssh, host, remote_dir, local_dir, folder_mapping, work_dirs, final_local_dir):
    """
    서버의 파일을 처리하며, 필요한 로컬 디렉토리를 생성
    """
    # 원격 디렉토리 내 파일 목록 가져오기
    file_list = get_file_list(ssh, host, remote_dir)

    for file_name in file_list:
        # 매핑된 폴더 확인
        mapped_folder = next(
            (folder_name for prefix, folder_name in folder_mapping.items() if file_name.startswith(prefix)),
            None,
        )

        # 매핑된 폴더가 없는 파일 처리
        if not mapped_folder:
            logging.warning(f"File {file_name} does not match any folder mapping. Skipping.")
            continue

        # 'message' 폴더에 속하는 파일 스킵
        if mapped_folder == "message":
            logging.info(f"Skipping file {file_name} as it belongs to 'message' folder.")
            continue

        # 'grpc', 'redis', 'api' 폴더에 속하는 파일만 처리
        if mapped_folder in work_dirs:
            # 매핑된 폴더 내에서 파일 처리
            logging.info(f"Processing file {file_name} in folder {mapped_folder}.")

            local_folder_path = os.path.join(local_dir, mapped_folder)
            final_folder_path = os.path.join(final_local_dir, mapped_folder)

            make_local_dir(local_folder_path)
            make_local_dir(final_folder_path)

            process_single_file(ssh, host, remote_dir, local_folder_path, file_name, final_folder_path, mapped_folder)


@timestamp
def process_server(host, username, password, remote_dir, local_dir, folder_mapping, work_dirs, final_local_dir):
    """
    서버 연결
    """
    ssh = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=username, password=password, timeout=30)

        process_files_on_server(ssh, host, remote_dir, local_dir, folder_mapping, work_dirs, final_local_dir)

    except (AuthenticationException, NoValidConnectionsError) as e:
        logging.error(f"Connection error to {host}: {e}")
    except SSHException as e:
        logging.error(f"SSH error with {host}: {e}")
    except Exception as e:
        logging.error(f"Error processing server {host}: {e}")
    finally:
        if ssh:
            try:
                ssh.close()
            except Exception:
                pass


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    config_file = input("Enter the configuration file name: ").strip()
    if config_file:
        config = load_config(config_file)
    else:
        config = load_config()

    remote_log_dir = config.get("remote_log_dir", "")
    local_log_dir = config.get("local_log_dir", "")
    final_local_dir = config.get("final_local_dir", "")
    interval_seconds = config.get("interval_seconds", 0)

    folder_mapping = config.get("folder_mapping", {})
    work_dirs = set(config.get("work_dir", []))

    servers = config.get("servers", {})
    username = servers.get("username", "")
    password = servers.get("password", "")
    edge_2k_ips = servers.get("edge_2k_ip", [])

    if not edge_2k_ips:
        logging.warning("No servers to process. Exiting.")
        return

    make_local_dir(local_log_dir)
    make_local_dir(final_local_dir)

    while not stop_event.is_set():  # 종료 플래그 확인
        logging.info("Starting batch processing of servers...")

        for edge_2k_ip in edge_2k_ips:
            local_edge_dir = os.path.join(local_log_dir, edge_2k_ip)
            final_edge_dir = os.path.join(final_local_dir, edge_2k_ip)

            make_local_dir(local_edge_dir)
            make_local_dir(final_edge_dir)

            process_server(edge_2k_ip, username, password, remote_log_dir, local_edge_dir, folder_mapping, work_dirs, final_edge_dir)

        logging.info(f"Batch processing completed. Waiting for {interval_seconds} seconds...")

        for _ in range(interval_seconds):  # 1초 마다 종료 플래그 확인
            if stop_event.is_set():
                break
            time.sleep(1)

    logging.info("Shutdown complete.")


if __name__ == "__main__":
    main()
