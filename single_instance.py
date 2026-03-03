# -*- coding: utf-8 -*-
"""
프로그램 중복 실행 방지 (싱글 인스턴스).
PID 파일 기반: 동일 앱이 이미 실행 중이면 (False, 기존_PID) 반환, 아니면 lock 생성 후 (True, None) 반환.
"""
import atexit
import os
import subprocess
import sys
import time
from typing import Optional, Tuple

if sys.platform == "win32":
    import signal as _signal
else:
    import signal as _signal


def _is_process_running(pid: int) -> bool:
    """해당 PID의 프로세스가 살아 있으면 True."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def kill_process_forcefully(pid: int, wait_after_sec: float = 1.5) -> bool:
    """
    프로세스를 강제 종료. Windows에서는 taskkill /F /PID 사용, 그 외는 SIGKILL.
    wait_after_sec 동안 대기 후 해당 PID가 없어졌는지 확인해 True/False 반환.
    """
    if pid <= 0:
        return True
    try:
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/F", "/PID", str(pid)],
                capture_output=True,
                timeout=10,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
        else:
            os.kill(pid, getattr(_signal, "SIGKILL", 9))
    except Exception:
        pass
    time.sleep(wait_after_sec)
    return not _is_process_running(pid)


def _lock_file_path(app_name: str, script_dir: str) -> str:
    """앱별 lock 파일 경로. app_name에는 .lock 이 붙지 않은 식별자만 사용."""
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in app_name)
    return os.path.join(script_dir, f"{safe}.lock")


def force_remove_lock(app_name: str, script_dir: Optional[str] = None) -> None:
    """lock 파일을 강제 삭제 (기존 프로세스 종료 후 재시도 시 사용)."""
    if script_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    path = _lock_file_path(app_name, script_dir)
    try:
        if os.path.isfile(path):
            os.remove(path)
    except OSError:
        pass


def try_acquire_single_instance(app_name: str, script_dir: Optional[str] = None) -> Tuple[bool, Optional[int]]:
    """
    중복 실행 방지 획득 시도.
    - 이번 프로세스가 유일하면 (True, None) 반환하고 lock 파일 생성. 정상 종료 시 atexit으로 삭제.
    - 이미 다른 인스턴스가 실행 중이면 (False, 기존_PID) 반환. 호출 측에서 문의 후 해당 PID 종료 가능.
    script_dir이 None이면 이 모듈이 있는 디렉터리 사용.
    """
    if script_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    path = _lock_file_path(app_name, script_dir)

    for _ in range(2):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, str(os.getpid()).encode())
            finally:
                os.close(fd)
            atexit.register(release_single_instance, app_name, script_dir)
            return (True, None)
        except FileExistsError:
            pass
        # 기존 lock 파일: 해당 PID가 아직 살아 있는지 확인
        try:
            with open(path, "r", encoding="utf-8") as f:
                s = f.read().strip()
            pid = int(s)
        except (ValueError, OSError):
            pid = None
        if pid is not None and _is_process_running(pid):
            return (False, pid)
        try:
            os.remove(path)
        except OSError:
            return (False, None)
    return (False, None)


def release_single_instance(app_name: str, script_dir: Optional[str] = None) -> None:
    """lock 파일 삭제 (정상 종료 시 atexit에서 호출)."""
    if script_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    path = _lock_file_path(app_name, script_dir)
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                if f.read().strip() == str(os.getpid()):
                    os.remove(path)
    except OSError:
        pass
