@echo off
:: 파일이 있는 폴더로 작업 경로 이동
cd /d "%~dp0"

:: 파이썬 GUI를 별도 프로세스로 실행 (창 없음)
start "" pythonw ktr_order_reservation_gui.py

:: 현재 명령 프롬프트 창 종료
exit
