"""Windowsタスクスケジューラ登録ヘルパー

Usage:
    python scheduler.py install     # タスクを登録
    python scheduler.py uninstall   # タスクを削除
    python scheduler.py status      # タスクの状態確認
"""

import subprocess
import sys
from pathlib import Path

TASK_NAME = "MinpakuRentalSearch"
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent
PYTHON_EXE = PROJECT_ROOT / "venv" / "Scripts" / "python.exe"
MAIN_SCRIPT = BASE_DIR / "main.py"
SCHEDULE_TIME = "02:00"  # 毎日午前2時


def install_task():
    """タスクスケジューラにタスクを登録"""
    command = (
        f'cmd /c "cd /d {PROJECT_ROOT} && '
        f'{PYTHON_EXE} {MAIN_SCRIPT}"'
    )
    result = subprocess.run(
        [
            "schtasks", "/create",
            "/tn", TASK_NAME,
            "/tr", command,
            "/sc", "daily",
            "/st", SCHEDULE_TIME,
            "/f",  # 既存タスクを上書き
        ],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        print(f"Task '{TASK_NAME}' registered successfully.")
        print(f"  Schedule: Daily at {SCHEDULE_TIME}")
        print(f"  Command: {command}")
    else:
        print(f"Failed to register task: {result.stderr}")
    return result.returncode


def uninstall_task():
    """タスクを削除"""
    result = subprocess.run(
        ["schtasks", "/delete", "/tn", TASK_NAME, "/f"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        print(f"Task '{TASK_NAME}' deleted.")
    else:
        print(f"Failed to delete task: {result.stderr}")
    return result.returncode


def status_task():
    """タスクの状態を確認"""
    result = subprocess.run(
        ["schtasks", "/query", "/tn", TASK_NAME, "/v", "/fo", "list"],
        capture_output=True, text=True, encoding="cp932",
    )
    if result.returncode == 0:
        print(result.stdout)
    else:
        print(f"Task not found or error: {result.stderr}")
    return result.returncode


def main():
    if len(sys.argv) < 2:
        print("Usage: python scheduler.py [install|uninstall|status]")
        return

    action = sys.argv[1].lower()
    if action == "install":
        install_task()
    elif action == "uninstall":
        uninstall_task()
    elif action == "status":
        status_task()
    else:
        print(f"Unknown action: {action}")
        print("Usage: python scheduler.py [install|uninstall|status]")


if __name__ == "__main__":
    main()
