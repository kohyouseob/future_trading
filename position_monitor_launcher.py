# -*- coding: utf-8 -*-
"""
포지션 모니터(1H 레벨 청산)를 시작/정지하고 결과를 표시하는 Windows GUI 런처. (v2)
경로·DB는 이 파일이 있는 v2 폴더 기준(db_config → v2/scheduler.db).
입출금 버튼: mt5_deposit_withdrawal.py 실행 (파일 없으면 클릭 시 로그에 경로·안내 출력). 노션 미연동 시 KTR은 DB만 저장.
실행: python position_monitor_launcher.py  또는  pythonw position_monitor_launcher.py (콘솔 숨김)

pythonw 로 실행 시 창이 안 뜨면:
  1) python position_monitor_launcher.py 로 실행해 콘솔에 나오는 에러 확인
  2) 같은 폴더의 position_monitor_launcher_startup.log 에 기록된 에러 확인
"""
import os
import queue
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, date

# 스크립트가 있는 디렉터리(v2)로 이동 (pythonw/바로가기 실행 시 CWD 안정화)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.isdir(SCRIPT_DIR):
    try:
        os.chdir(SCRIPT_DIR)
    except Exception:
        pass

import tkinter as tk
from tkinter import scrolledtext, font as tkfont, messagebox, ttk
MONITOR_SCRIPT = os.path.join(SCRIPT_DIR, "position_monitoring_closing.py")
DEPOSIT_SCRIPT = os.path.join(SCRIPT_DIR, "mt5_deposit_withdrawal.py")
try:
    from db_config import UNIFIED_DB_PATH
    KTR_DB_PATH = UNIFIED_DB_PATH
    PM_DB_PATH = UNIFIED_DB_PATH
except ImportError:
    _FALLBACK_DB = os.path.join(SCRIPT_DIR, "scheduler.db")
    KTR_DB_PATH = _FALLBACK_DB
    PM_DB_PATH = _FALLBACK_DB
LOG_FILE_PATH = os.path.join(SCRIPT_DIR, "position_monitor_launcher.log")
LOG_RETENTION_DAYS = 7
# 입출금 자동 실행: 해당 날짜(YYYY-MM-DD)에 이미 실행했으면 스킵 (런처 재시작해도 하루 1회 유지)
DEPOSIT_LAST_RUN_FILE = os.path.join(SCRIPT_DIR, "position_monitor_deposit_last_run.txt")
BB_TF_FILE = os.path.join(SCRIPT_DIR, "position_monitor_bb_tf.txt")
BB_REFRESH_SCRIPT = os.path.join(SCRIPT_DIR, "position_monitor_bb_refresh.py")
BAR_BACKFILL_SCRIPT = os.path.join(SCRIPT_DIR, "position_monitor_bar_backfill.py")
# 청산 시작/정지: "1"=청산 실행, "0"=청산만 중지(포지션/DB/BB 등은 모니터가 계속 실행)
CLOSING_ENABLED_FILE = os.path.join(SCRIPT_DIR, "position_monitor_closing_enabled.txt")
# 손실율/마진레벨: 런처에서 선택한 값을 모니터가 매 점검 시 파일에서 읽음 (실행 중 버튼 변경 시에도 반영)
STOP_LOSS_PCT_FILE = os.path.join(SCRIPT_DIR, "position_monitor_stop_loss_pct.txt")
MARGIN_LEVEL_CLOSE_FILE = os.path.join(SCRIPT_DIR, "position_monitor_margin_level_close.txt")

# 포지션 모니터 DB (High/Low 갱신용)
try:
    import position_monitor_db as pm_db
except ImportError:
    pm_db = None

# 모니터에서 사용하는 심볼 (session_high_low 갱신 대상)
PM_SYMBOLS = ("XAUUSD+", "NAS100+")


def _bb_interval_sec_for_tf(tf_value: str) -> int:
    """선택된 BB 타임프레임에 따른 갱신 주기(초). M5=300, M10=600, H1=3600."""
    return {"M5": 300, "M10": 600, "H1": 3600}.get(tf_value, 3600)


def run_in_thread(target, daemon=True):
    t = threading.Thread(target=target, daemon=daemon)
    t.start()
    return t


class MonitorLauncherApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("포지션 모니터 (1H 레벨 청산) [v2]")
        self.root.minsize(650, 646)
        self.root.geometry("650x646")

        self.process = None
        self.log_queue = queue.Queue()
        self._pos_update_buf = []
        self._pos_update_collecting = False
        self._pos_update_rows = []  # 마지막 파싱된 포지션 목록 (합계 병합용)
        self._pos_summary_buf = []
        self._pos_summary_collecting = False
        self._bb_collecting = False
        self._bb_buf = []
        self._bb_last_update_time = None  # 1시간마다 BB 패널 갱신용
        self._log_file = None
        self._log_file_start_time = None
        self._log_file_lock = threading.Lock()
        self._last_deposit_run_date = None  # 매일 1회 입출금 실행용 (메모리 캐시; 파일이 우선)
        self._deposit_skip_telegram_sent = False  # 오늘 이미 실행되어 스킵 시 텔레그램 1회만 전송
        self._build_ui()
        self._poll_log_queue()
        # 창이 뜨면 자동으로 모니터 시작
        self.root.after(0, self._on_start)
        # 매일 08:04 입출금 자동 실행 (1분마다 시각 확인)
        self.root.after(10000, self._check_deposit_schedule)

    def _build_ui(self):
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        # ----- 탭 1: 포지션 모니터 -----
        tab_monitor = tk.Frame(notebook, padx=4, pady=4)
        notebook.add(tab_monitor, text="포지션 모니터")

        # BB/캔들 관련 변수는 로그 파싱에서 참조되므로 유지 (표시만 제거)
        self._bb_tf_var = tk.StringVar(value="H1")
        self._bb_update_interval_sec = 3600
        self._bb_sym_var = tk.StringVar(value="XAUUSD")
        self._bb_xau_ohlc = self._bb_xau_20 = self._bb_xau_4 = "—"
        self._bb_nas_ohlc = self._bb_nas_20 = self._bb_nas_4 = "—"
        self._bb_20_var = tk.StringVar(value="—")
        self._bb_4_var = tk.StringVar(value="—")
        self._bb_refresh_btn = None
        self._bb_last_update_time = None

        # 상단: 버튼 + 상태 (두 줄)
        top = tk.Frame(tab_monitor, padx=8, pady=6)
        top.pack(fill=tk.X)

        # 1행: 로그 지우기, MT5 재시작, 입출금, 강제청산 심볼 선택, 강제청산(맨 오른쪽) — 청산은 항상 실행
        row1 = tk.Frame(top)
        row1.pack(fill=tk.X, pady=(0, 4))
        tk.Button(row1, text="로그 지우기", command=self._clear_log, width=8, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        self.btn_restart_mt5 = tk.Button(
            row1, text="MT5 재시작", command=self._on_restart_mt5,
            width=8, font=tkfont.Font(size=9, weight="bold"),
        )
        self.btn_restart_mt5.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_deposit = tk.Button(
            row1, text="입출금", command=self._on_deposit_withdrawal,
            width=6, font=tkfont.Font(size=9, weight="bold"),
        )
        self._btn_deposit.pack(side=tk.LEFT, padx=(0, 4))
        tk.Button(
            row1, text="High/Low", command=self._on_high_low_update,
            width=8, font=tkfont.Font(size=9),
        ).pack(side=tk.LEFT, padx=(0, 4))
        tk.Button(
            row1, text="Supabase 동기화", command=self._on_supabase_sync,
            width=14, font=tkfont.Font(size=9),
        ).pack(side=tk.LEFT, padx=(0, 4))
        tk.Button(
            row1, text="프로그램 종료", command=self._on_close,
            width=10, font=tkfont.Font(size=9, weight="bold"), fg="#c00",
        ).pack(side=tk.LEFT, padx=(12, 0))

        # 2행: 청산 손실율 + 상태
        row2 = tk.Frame(top)
        row2.pack(fill=tk.X)
        tk.Label(row2, text="청산 손실율:", font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        self.stop_loss_pct_var = tk.StringVar(value="20")
        for val, label in (("7", "7%"), ("10", "10%"), ("20", "20%"), ("30", "30%"), ("50", "50%")):
            tk.Radiobutton(row2, text=label, variable=self.stop_loss_pct_var, value=val, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        tk.Label(row2, text="  |  ", font=tkfont.Font(size=9), fg="gray").pack(side=tk.LEFT, padx=(4, 0))
        self.status_var = tk.StringVar(value="중지됨")
        self.status_label = tk.Label(row2, textvariable=self.status_var, font=tkfont.Font(size=9))
        self.status_label.pack(side=tk.LEFT, padx=(12, 0))

        # 3행: 청산 마진레벨% (청산 손실율 아래)
        row2b = tk.Frame(top)
        row2b.pack(fill=tk.X, pady=(2, 0))
        tk.Label(row2b, text="청산 마진레벨%:", font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        self.margin_level_close_var = tk.StringVar(value="200")
        for val, label in (("100", "100"), ("200", "200"), ("300", "300")):
            tk.Radiobutton(row2b, text=label, variable=self.margin_level_close_var, value=val, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        # 손실율/마진레벨 라디오 변경 시 파일에 저장 → 실행 중인 모니터가 다음 점검 시 반영
        self.stop_loss_pct_var.trace_add("write", lambda *a: self._write_stop_params_to_files())
        self.margin_level_close_var.trace_add("write", lambda *a: self._write_stop_params_to_files())

        # 4행: 강제청산
        row3 = tk.Frame(top)
        row3.pack(fill=tk.X, pady=(2, 0))
        self.force_close_symbol_var = tk.StringVar(value="전체")
        tk.Label(row3, text="강제청산:", font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        for val, label in (("전체", "전체"), ("XAUUSD", "XAUUSD"), ("NAS100", "NAS100")):
            tk.Radiobutton(
                row3, text=label, variable=self.force_close_symbol_var, value=val,
                font=tkfont.Font(size=9),
            ).pack(side=tk.LEFT, padx=(0, 4))
        self.btn_force_close = tk.Button(
            row3, text="강제청산", command=self._on_force_close,
            width=6, font=tkfont.Font(size=9, weight="bold"), fg="#c00",
        )
        self.btn_force_close.pack(side=tk.LEFT, padx=(8, 0))

        # 현재 보유 포지션 (5분마다 모니터에서 갱신)
        pos_frame = tk.LabelFrame(tab_monitor, text="현재 보유 포지션 (5분마다 갱신)", padx=4, pady=4)
        pos_frame.pack(fill=tk.BOTH, expand=False, padx=8, pady=(0, 4))

        tree_container = tk.Frame(pos_frame)
        tree_container.pack(fill=tk.BOTH, expand=True)

        cols = ("symbol", "profit", "roi", "elapsed")
        self.position_tree = ttk.Treeview(tree_container, columns=cols, show="headings", height=3)
        self.position_tree.heading("symbol", text="심볼")
        self.position_tree.heading("profit", text="수익금 ($)")
        self.position_tree.heading("roi", text="수익률 (%)")
        self.position_tree.heading("elapsed", text="진입 후 경과")
        for c in cols:
            self.position_tree.column(c, width=100)
        self.position_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        pos_scroll = ttk.Scrollbar(tree_container, orient=tk.VERTICAL, command=self.position_tree.yview)
        pos_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.position_tree.configure(yscrollcommand=pos_scroll.set)

        self._pos_placeholder = tk.Label(pos_frame, text="모니터를 시작하면 여기에 포지션이 표시됩니다.", fg="gray")
        self._pos_placeholder.pack(pady=2)

        # 로그 영역
        log_frame = tk.LabelFrame(tab_monitor, text="실행 결과", padx=4, pady=4)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        self.log_text = scrolledtext.ScrolledText(
            log_frame, wrap=tk.WORD, state=tk.DISABLED,
            font=("Consolas", 8), bg="#1e1e1e", fg="#d4d4d4",
            insertbackground="#d4d4d4", height=10
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # 태그: 에러는 빨간색
        self.log_text.tag_configure("stderr", foreground="#f48771")
        self.log_text.tag_configure("stdout", foreground="#d4d4d4")

        # ----- 탭 2: KTR DB / 업데이트 -----
        tab_ktr = tk.Frame(notebook, padx=8, pady=8)
        notebook.add(tab_ktr, text="KTR DB / 업데이트")

        ktr_db_frame = tk.LabelFrame(tab_ktr, text="KTR DB 최근 기록", padx=4, pady=4)
        ktr_db_frame.pack(fill=tk.BOTH, expand=True)
        btn_row_ktr = tk.Frame(ktr_db_frame)
        btn_row_ktr.pack(fill=tk.X, pady=(0, 4))
        tk.Button(btn_row_ktr, text="새로고침", command=self._on_refresh_ktr_db, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 8))
        tk.Button(btn_row_ktr, text="선택 삭제", command=self._on_delete_ktr_record, font=tkfont.Font(size=9), fg="#c00").pack(side=tk.LEFT, padx=(0, 4))
        tk.Button(btn_row_ktr, text="중복 삭제", command=self._on_delete_ktr_duplicates, font=tkfont.Font(size=9), fg="#c00").pack(side=tk.LEFT, padx=(0, 4))
        tk.Button(btn_row_ktr, text="누락업데이트", command=self._on_fill_missing_ktr, font=tkfont.Font(size=9), fg="#0a0").pack(side=tk.LEFT, padx=(0, 4))
        ktr_cols = ("symbol", "session", "timeframe", "record_date", "ktr_value", "balance", "lot_1st", "lot_2nd", "lot_3rd", "created_at")
        self.ktr_tree = ttk.Treeview(ktr_db_frame, columns=ktr_cols, show="headings", height=8)
        head_text = {"record_date": "측정일", "created_at": "입력일시"}
        for c in ktr_cols:
            self.ktr_tree.heading(c, text=head_text.get(c, c))
            self.ktr_tree.column(c, width=72)
        self.ktr_tree.column("record_date", width=88)
        self.ktr_tree.column("created_at", width=140)
        ktr_scroll = ttk.Scrollbar(ktr_db_frame, orient=tk.VERTICAL, command=self.ktr_tree.yview)
        self.ktr_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ktr_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.ktr_tree.configure(yscrollcommand=ktr_scroll.set)

        # KTR 수동 입력 (DB + 노션 저장)
        manual_frame = tk.LabelFrame(tab_ktr, text="KTR 수동 입력 (DB + 노션 저장)", padx=4, pady=4)
        manual_frame.pack(fill=tk.X, pady=(8, 4))
        row1 = tk.Frame(manual_frame)
        row1.pack(fill=tk.X, pady=2)
        tk.Label(row1, text="측정일", width=6, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        self._ktr_manual_date = tk.StringVar(value=time.strftime("%Y-%m-%d", time.localtime()))
        tk.Entry(row1, textvariable=self._ktr_manual_date, width=12, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 12))
        tk.Label(row1, text="심볼", width=6, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        self._ktr_manual_symbol = tk.StringVar(value="NAS100")
        ttk.Combobox(row1, textvariable=self._ktr_manual_symbol, values=["NAS100", "XAUUSD"], width=10, state="readonly", font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 12))
        tk.Label(row1, text="세션", width=6, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        self._ktr_manual_session = tk.StringVar(value="Asia")
        ttk.Combobox(row1, textvariable=self._ktr_manual_session, values=["Asia", "Europe", "US"], width=8, state="readonly", font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 12))
        tk.Label(row1, text="타임프레임", width=8, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        self._ktr_manual_tf = tk.StringVar(value="5M")
        ttk.Combobox(row1, textvariable=self._ktr_manual_tf, values=["5M", "10M", "1H"], width=6, state="readonly", font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 12))
        row2 = tk.Frame(manual_frame)
        row2.pack(fill=tk.X, pady=2)
        tk.Label(row2, text="KTR 값", width=6, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        self._ktr_manual_value = tk.StringVar(value="")
        tk.Entry(row2, textvariable=self._ktr_manual_value, width=10, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 12))
        tk.Label(row2, text="Balance", width=6, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 4))
        self._ktr_manual_balance = tk.StringVar(value="")
        tk.Entry(row2, textvariable=self._ktr_manual_balance, width=10, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 12))
        tk.Label(row2, text="1st", width=4, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 2))
        self._ktr_manual_lot1 = tk.StringVar(value="")
        tk.Entry(row2, textvariable=self._ktr_manual_lot1, width=8, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 8))
        tk.Label(row2, text="2nd", width=4, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 2))
        self._ktr_manual_lot2 = tk.StringVar(value="")
        tk.Entry(row2, textvariable=self._ktr_manual_lot2, width=8, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 8))
        tk.Label(row2, text="3rd", width=4, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 2))
        self._ktr_manual_lot3 = tk.StringVar(value="")
        tk.Entry(row2, textvariable=self._ktr_manual_lot3, width=8, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 8))
        row3 = tk.Frame(manual_frame)
        row3.pack(fill=tk.X, pady=(4, 0))
        tk.Button(row3, text="저장 (DB + 노션)", command=self._on_ktr_manual_save, width=14, font=tkfont.Font(size=9)).pack(side=tk.LEFT, padx=(0, 8))
        tk.Label(row3, text="KTR만 입력 시 Balance=MT5 잔고, 1st/2nd/3rd=자동 계산(리스크10%, 구간2.5).", font=tkfont.Font(size=8), fg="gray").pack(side=tk.LEFT, padx=(8, 0))

        self._refresh_ktr_db()

    def _on_refresh_ktr_db(self):
        for i in self.ktr_tree.get_children():
            self.ktr_tree.delete(i)
        try:
            sys.path.insert(0, SCRIPT_DIR)
            from ktr_db_utils import KTRDatabase
            db = KTRDatabase(db_name=KTR_DB_PATH)
            rows = db.get_recent_records(limit=80)
            for idx, r in enumerate(rows):
                rec_id = r.get("id")
                iid = str(rec_id) if rec_id is not None else f"_supabase_{idx}"
                self.ktr_tree.insert("", tk.END, iid=iid, values=(
                    r.get("symbol", ""),
                    r.get("session", ""),
                    r.get("timeframe", ""),
                    r.get("record_date", "") or (r.get("created_at") or "")[:10],
                    r.get("ktr_value", 0),
                    r.get("balance"),
                    r.get("lot_1st"),
                    r.get("lot_2nd"),
                    r.get("lot_3rd"),
                    (r.get("created_at") or "")[:19],
                ))
        except Exception as e:
            self._log(f"[KTR DB] 조회 오류: {e}\n", "stderr")

    def _on_delete_ktr_record(self):
        sel = self.ktr_tree.selection()
        if not sel:
            messagebox.showinfo("삭제", "삭제할 레코드를 선택하세요.")
            return
        item_id = sel[0]
        vals = self.ktr_tree.item(item_id, "values")
        try:
            rec_id = int(item_id)
        except (ValueError, TypeError):
            rec_id = 0
        if not rec_id or rec_id <= 0:
            rec_id = 0
        symbol = vals[0] if len(vals) > 0 else ""
        session = vals[1] if len(vals) > 1 else ""
        tf = vals[2] if len(vals) > 2 else ""
        record_date = vals[3] if len(vals) > 3 else ""
        preview = f"{symbol} {session} {tf} 측정일 {record_date} KTR {vals[4] if len(vals) > 4 else ''}"
        if not messagebox.askokcancel("삭제", f"다음 레코드를 삭제하시겠습니까?\n\n{preview}"):
            return
        try:
            sys.path.insert(0, SCRIPT_DIR)
            from ktr_db_utils import KTRDatabase
            db = KTRDatabase(db_name=KTR_DB_PATH)
            if rec_id and rec_id > 0:
                ok = db.delete_by_id(rec_id)
            else:
                ok = db.delete_by_natural_key(symbol, session, tf, record_date)
            if ok:
                self.ktr_tree.delete(item_id)
                self._log(f"[KTR DB] 레코드 삭제됨: " + (f"id={rec_id}" if rec_id else f"{symbol} {session} {tf} {record_date}") + "\n", "stdout")
                messagebox.showinfo("삭제", "삭제되었습니다.")
            else:
                messagebox.showerror("삭제", "삭제에 실패했습니다.")
        except Exception as e:
            messagebox.showerror("삭제", f"오류: {e}")
            self._log(f"[KTR DB] 삭제 오류: {e}\n", "stderr")

    def _on_delete_ktr_duplicates(self):
        if not messagebox.askokcancel("중복 삭제", "동일 (심볼, 세션, 타임프레임, 측정일) 기준으로 중복을 제거합니다.\n각 그룹에서 가장 최근 레코드 1건만 남깁니다. 계속할까요?"):
            return
        try:
            sys.path.insert(0, SCRIPT_DIR)
            from ktr_db_utils import KTRDatabase
            db = KTRDatabase(db_name=KTR_DB_PATH)
            deleted = db.delete_duplicate_records()
            self._log(f"[KTR DB] 중복 삭제됨: {deleted}건\n", "stdout")
            messagebox.showinfo("중복 삭제", f"{deleted}건 중복 삭제됨.")
            self._on_refresh_ktr_db()
        except Exception as e:
            messagebox.showerror("중복 삭제", f"오류: {e}")
            self._log(f"[KTR DB] 중복 삭제 오류: {e}\n", "stderr")

    def _on_fill_missing_ktr(self):
        """오늘자 KTR 누락 슬롯을 MT5(및 포지션모니터 DB)로 자동 측정해 DB에 반영. 백그라운드 스레드에서 실행 후 트리 갱신."""
        def _run():
            filled = 0
            try:
                sys.path.insert(0, SCRIPT_DIR)
                from ktr_measure_calculator import run_fill_missing_ktr_for_today
                filled = run_fill_missing_ktr_for_today(ktr_db_path=KTR_DB_PATH, quiet=True)
            except Exception as e:
                err_msg = str(e)
                self.root.after(0, lambda: self._after_fill_missing_ktr(0, err_msg))
                return
            self.root.after(0, lambda: self._after_fill_missing_ktr(filled, None))

        self._log("[KTR DB] 누락 KTR 자동 입력 중... (잠시 기다려 주세요)\n", "stdout")
        threading.Thread(target=_run, daemon=True).start()

    def _after_fill_missing_ktr(self, filled_count: int, error_message: str | None):
        """누락업데이트 완료 후 트리 갱신 및 메시지 표시."""
        self._refresh_ktr_db()
        if error_message:
            messagebox.showerror("누락업데이트", f"오류: {error_message}")
            self._log(f"[KTR DB] 누락업데이트 오류: {error_message}\n", "stderr")
        elif filled_count > 0:
            messagebox.showinfo("누락업데이트", f"오늘자 누락 {filled_count}개 슬롯을 자동 입력했습니다.")
            self._log(f"[KTR DB] 누락업데이트 완료: {filled_count}개 슬롯\n", "stdout")
        else:
            messagebox.showinfo("누락업데이트", "누락된 KTR 슬롯이 없습니다.")

    def _refresh_ktr_db(self):
        """KTR 탭의 트리뷰만 갱신 (스레드에서 호출 시 root.after로 감쌀 것)."""
        for i in self.ktr_tree.get_children():
            self.ktr_tree.delete(i)
        try:
            sys.path.insert(0, SCRIPT_DIR)
            from ktr_db_utils import KTRDatabase
            db = KTRDatabase(db_name=KTR_DB_PATH)
            rows = db.get_recent_records(limit=80)
            for r in rows:
                rec_id = r.get("id")
                iid = str(rec_id) if rec_id is not None else ""
                self.ktr_tree.insert("", tk.END, iid=iid, values=(
                    r.get("symbol", ""),
                    r.get("session", ""),
                    r.get("timeframe", ""),
                    r.get("record_date", "") or (r.get("created_at") or "")[:10],
                    r.get("ktr_value", 0),
                    r.get("balance"),
                    r.get("lot_1st"),
                    r.get("lot_2nd"),
                    r.get("lot_3rd"),
                    (r.get("created_at") or "")[:19],
                ))
        except Exception:
            pass

    def _on_ktr_manual_save(self):
        symbol = (self._ktr_manual_symbol.get() or "").strip()
        session = (self._ktr_manual_session.get() or "").strip()
        tf = (self._ktr_manual_tf.get() or "").strip()
        ktr_str = (self._ktr_manual_value.get() or "").strip()
        date_str = (self._ktr_manual_date.get() or "").strip()
        if not symbol or not session or not tf or not ktr_str:
            messagebox.showwarning("입력", "심볼, 세션, 타임프레임, KTR 값을 모두 입력하세요.")
            return
        if not date_str:
            date_str = time.strftime("%Y-%m-%d", time.localtime())
        else:
            import re
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
                messagebox.showerror("입력", "측정일은 YYYY-MM-DD 형식으로 입력하세요 (예: 2026-02-17).")
                return
        try:
            ktr_value = float(ktr_str)
        except ValueError:
            messagebox.showerror("입력", "KTR 값은 숫자로 입력하세요.")
            return

        def _parse_float(s):
            s = (s or "").strip()
            if not s:
                return None
            try:
                return float(s)
            except ValueError:
                return None

        balance = _parse_float(self._ktr_manual_balance.get())
        lot_1st = _parse_float(self._ktr_manual_lot1.get())
        lot_2nd = _parse_float(self._ktr_manual_lot2.get())
        lot_3rd = _parse_float(self._ktr_manual_lot3.get())

        # Balance 미입력 시 MT5에서 조회
        if balance is None:
            try:
                sys.path.insert(0, SCRIPT_DIR)
                import mt5_trade_utils as tr
                acc = tr.get_account_info()
                if acc and acc.get("balance") is not None:
                    balance = float(acc["balance"])
                else:
                    messagebox.showerror("Balance", "MT5 잔고를 가져올 수 없습니다. MT5를 실행한 뒤 다시 시도하거나, Balance를 직접 입력하세요.")
                    return
            except Exception as e:
                messagebox.showerror("Balance", f"MT5 잔고 조회 실패: {e}\nBalance를 직접 입력하세요.")
                return

        # 1st/2nd/3rd 미입력 시 KTR·Balance로 계산 (리스크 10%, 구간 2.5 = 3레그)
        if lot_1st is None and lot_2nd is None and lot_3rd is None:
            try:
                sys.path.insert(0, SCRIPT_DIR)
                from ktr_lots import get_ktrlots_lots
                lots = get_ktrlots_lots(balance, 10, 2.5, ktr_value, symbol, use_local=True)
                if lots:
                    lot_1st = lots.get("1st") or 0.0
                    lot_2nd = lots.get("2nd") or 0.0
                    lot_3rd = lots.get("3rd") or 0.0
                else:
                    lot_1st, lot_2nd, lot_3rd = 0.0, 0.0, 0.0
            except Exception as e:
                self._log(f"[KTR 수동] 랏 계산 오류: {e}\n", "stderr")
                lot_1st, lot_2nd, lot_3rd = 0.0, 0.0, 0.0

        try:
            sys.path.insert(0, SCRIPT_DIR)
            from ktr_db_utils import KTRDatabase
            db = KTRDatabase(db_name=KTR_DB_PATH)
            db.update_ktr(
                symbol, session, tf, ktr_value,
                balance=balance, lot_1st=lot_1st, lot_2nd=lot_2nd, lot_3rd=lot_3rd,
                record_date=date_str,
            )
            notion_ok = False
            try:
                from ktr_notion_utils import upload_ktr_to_notion
                notion_ok = upload_ktr_to_notion(
                    date_str, symbol, session, tf, ktr_value,
                    balance, lot_1st, lot_2nd, lot_3rd,
                )
            except ImportError:
                pass
            self._refresh_ktr_db()
            msg = f"측정일: {date_str}\nBalance ${balance:,.2f}, 1st={lot_1st} 2nd={lot_2nd} 3rd={lot_3rd}\nDB 저장 완료."
            if notion_ok:
                msg += " 노션 업로드 완료."
            else:
                msg += " 노션 미연동(v2 기본: DB만 저장)."
            messagebox.showinfo("KTR 수동 저장", msg)
            self._log(f"[KTR 수동] 측정일={date_str} {symbol} {session} {tf} KTR={ktr_value} 저장됨.\n", "stdout")
        except Exception as e:
            messagebox.showerror("저장 오류", str(e))
            self._log(f"[KTR 수동] 오류: {e}\n", "stderr")

    def _update_position_table(self, rows: list) -> None:
        """rows: [(symbol, profit_str, roi_str, elapsed_str), ...]"""
        for i in self.position_tree.get_children():
            self.position_tree.delete(i)
        if not rows:
            self._pos_placeholder.pack(pady=2)
            return
        self._pos_placeholder.pack_forget()
        for r in rows:
            self.position_tree.insert("", tk.END, values=(r[0], r[1], r[2], r[3]))

    def _ensure_log_file(self):
        """로그 파일이 없거나 1주일이 지났으면 새로 연다. 호출 전에 _log_file_lock 확보 필요."""
        now = time.time()
        if self._log_file is not None:
            if now - self._log_file_start_time >= LOG_RETENTION_DAYS * 86400:
                try:
                    self._log_file.close()
                except Exception:
                    pass
                self._log_file = None
                try:
                    if os.path.isfile(LOG_FILE_PATH):
                        os.remove(LOG_FILE_PATH)
                except Exception:
                    pass
        if self._log_file is None:
            try:
                if os.path.isfile(LOG_FILE_PATH) and (now - os.path.getmtime(LOG_FILE_PATH)) >= LOG_RETENTION_DAYS * 86400:
                    os.remove(LOG_FILE_PATH)
                self._log_file = open(LOG_FILE_PATH, "a", encoding="utf-8")
                self._log_file_start_time = now
            except Exception:
                pass

    def _log(self, msg: str, tag: str = "stdout"):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, msg, tag)
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)
        with self._log_file_lock:
            self._ensure_log_file()
            if self._log_file is not None:
                try:
                    if tag == "stderr":
                        self._log_file.write("[stderr] ")
                    self._log_file.write(msg)
                    self._log_file.flush()
                except Exception:
                    pass

    def _write_stop_params_to_files(self, stop_loss_pct=None, margin_level_close=None):
        """손실율/마진레벨을 파일에 저장. 모니터가 매 점검 시 이 파일을 읽어 적용. 인자 없으면 현재 라디오 값 사용."""
        if stop_loss_pct is None:
            stop_loss_pct = (self.stop_loss_pct_var.get() or "").strip() or "20"
        if margin_level_close is None:
            margin_level_close = (self.margin_level_close_var.get() or "").strip() or "200"
        if stop_loss_pct not in ("7", "10", "20", "30", "50"):
            stop_loss_pct = "20"
        if margin_level_close not in ("100", "200", "300"):
            margin_level_close = "200"
        try:
            with open(STOP_LOSS_PCT_FILE, "w", encoding="utf-8") as f:
                f.write(stop_loss_pct)
            with open(MARGIN_LEVEL_CLOSE_FILE, "w", encoding="utf-8") as f:
                f.write(margin_level_close)
        except Exception:
            pass

    def _on_start(self):
        if not os.path.isfile(MONITOR_SCRIPT):
            self._log(f"오류: 스크립트를 찾을 수 없습니다.\n  {MONITOR_SCRIPT}\n", "stderr")
            return

        # 정지 후 손실율/마진레벨 변경 시 적용하려면 기동 중인 프로세스는 종료 후 새로 기동
        if self.process is not None:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    self.process.kill()
                    self.process.wait()
                except Exception:
                    pass
            except Exception:
                pass
            self.process = None

        # 모니터 프로세스가 없으면 기동 (있으면 방금 None으로 만든 경우)
        if self.process is None:
            stop_loss_pct = self.stop_loss_pct_var.get().strip() or "20"
            if stop_loss_pct not in ("7", "10", "20", "30", "50"):
                stop_loss_pct = "20"
            margin_level_close = self.margin_level_close_var.get().strip() or "200"
            if margin_level_close not in ("100", "200", "300"):
                margin_level_close = "200"
            self._write_stop_params_to_files(stop_loss_pct, margin_level_close)
            self._log(f"[시작] {MONITOR_SCRIPT} (청산 손실율 {stop_loss_pct}%, 청산 마진레벨 {margin_level_close}%)\n", "stdout")
            env_extra = {
                "PYTHONUNBUFFERED": "1",
                "POSITION_MONITOR_STOP_LOSS_PCT": stop_loss_pct,
                "POSITION_MONITOR_MARGIN_LEVEL_CLOSE_PCT": margin_level_close,
            }
            try:
                self.process = subprocess.Popen(
                    [sys.executable, "-u", MONITOR_SCRIPT],
                    cwd=SCRIPT_DIR,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env={**os.environ, **env_extra},
                )
                run_in_thread(self._read_stdout)
                run_in_thread(self._read_stderr)
                self._log("모니터 프로세스 기동됨. 로그 수신 대기 중...\n", "stdout")
            except Exception as e:
                self._log(f"실행 오류: {e}\n", "stderr")
                self._set_stopped()
                return

        self.status_var.set("실행 중...")

    def _read_stdout(self):
        if self.process is None:
            return
        try:
            for line in iter(self.process.stdout.readline, ""):
                self.log_queue.put(("out", line))
        except (ValueError, BrokenPipeError):
            pass
        self.log_queue.put(("out", None))

    def _read_stderr(self):
        if self.process is None:
            return
        try:
            for line in iter(self.process.stderr.readline, ""):
                self.log_queue.put(("err", line))
        except (ValueError, BrokenPipeError):
            pass
        self.log_queue.put(("err", None))

    def _poll_log_queue(self):
        try:
            while True:
                kind, line = self.log_queue.get_nowait()
                if line is None:
                    continue
                if kind == "out":
                    s = line.strip()
                    if s == "[POSITION_UPDATE]":
                        self._pos_update_collecting = True
                        self._pos_update_buf = []
                        continue
                    if s == "[/POSITION_UPDATE]":
                        if self._pos_update_collecting:
                            rows = []
                            for ln in self._pos_update_buf:
                                parts = ln.split("|")
                                if len(parts) >= 4:
                                    try:
                                        profit_val = float(parts[1])
                                        rows.append((parts[0], f"{profit_val:+,.2f}", f"{parts[2]}%", parts[3]))
                                    except ValueError:
                                        pass
                            self._pos_update_rows = rows
                            if not rows:
                                self._update_position_table([])
                            self._pos_update_collecting = False
                        continue
                    if s == "[BB_BANDS]":
                        self._bb_collecting = True
                        self._bb_buf = []
                        continue
                    if s == "[/BB_BANDS]":
                        if self._bb_collecting:
                            now_sec = time.time()
                            interval = getattr(self, "_bb_update_interval_sec", 3600)
                            if self._bb_last_update_time is None or (now_sec - self._bb_last_update_time) >= interval:
                                for ln in self._bb_buf:
                                    parts = ln.split("|")
                                    if len(parts) < 3:
                                        continue
                                    sym = (parts[0] or "").strip()
                                    def _f(i):
                                        return float(parts[i]) if i < len(parts) and parts[i].strip() else None
                                    try:
                                        if len(parts) >= 9:
                                            o, h, l_, c = _f(1), _f(2), _f(3), _f(4)
                                            b20u, b20l = _f(5), _f(6)
                                            b4u, b4l = _f(7), _f(8)
                                            ohlc_str = f"O: {o:.2f} H: {h:.2f} L: {l_:.2f} C: {c:.2f}" if o is not None and h is not None and l_ is not None and c is not None else "—"
                                            s20 = (f"상: {b20u:.2f} 하: {b20l:.2f}" if b20u is not None and b20l is not None else "—")
                                            s4 = (f"상: {b4u:.2f} 하: {b4l:.2f}" if b4u is not None and b4l is not None else "—")
                                            if "XAUUSD" in sym:
                                                self._bb_xau_ohlc, self._bb_xau_20, self._bb_xau_4 = ohlc_str, s20, s4
                                            elif "NAS100" in sym:
                                                self._bb_nas_ohlc, self._bb_nas_20, self._bb_nas_4 = ohlc_str, s20, s4
                                            self._refresh_bb_display()
                                        else:
                                            upper, lower = _f(1), _f(2)
                                            s20 = (f"상: {upper:.2f} 하: {lower:.2f}" if upper is not None and lower is not None else "—")
                                            if "XAUUSD" in sym:
                                                self._bb_xau_20 = s20
                                            elif "NAS100" in sym:
                                                self._bb_nas_20 = s20
                                            self._refresh_bb_display()
                                    except (ValueError, IndexError):
                                        pass
                                self._bb_last_update_time = now_sec
                        self._bb_collecting = False
                        continue
                    if self._bb_collecting and "|" in s:
                        if s.startswith("TF|"):
                            continue
                        self._bb_buf.append(s)
                        continue
                    if s == "[POSITION_SUMMARY]":
                        rows = []
                        for ln in self._pos_update_buf:
                            parts = ln.split("|")
                            if len(parts) >= 4:
                                try:
                                    profit_val = float(parts[1])
                                    rows.append((parts[0], f"{profit_val:+,.2f}", f"{parts[2]}%", parts[3]))
                                except ValueError:
                                    pass
                        self._pos_update_rows = rows
                        self._pos_update_collecting = False
                        self._pos_summary_collecting = True
                        self._pos_summary_buf = []
                        continue
                    if self._pos_summary_collecting and s == "[/POSITION_SUMMARY]":
                        summary_rows = []
                        for ln in self._pos_summary_buf:
                            parts = ln.split("|")
                            if len(parts) >= 4:
                                try:
                                    cnt = int(parts[1])
                                    tp = float(parts[2])
                                    tr = parts[3]
                                    summary_rows.append((parts[0], cnt, tp, tr))
                                except (ValueError, IndexError):
                                    pass
                        merged = []
                        for sym, cnt, tp, tr in summary_rows:
                            merged.append((f"{sym} (합계)", f"{tp:+,.2f}", f"{tr}%", f"{cnt}건"))
                            for r in self._pos_update_rows:
                                if r[0] == sym:
                                    merged.append(r)
                        self._update_position_table(merged)
                        self._pos_summary_collecting = False
                        continue
                    if self._pos_summary_collecting and "|" in s:
                        parts = s.split("|")
                        if len(parts) >= 4:
                            try:
                                int(parts[1].strip())
                                float(parts[2].strip())
                                float(parts[3].strip())
                                self._pos_summary_buf.append(s)
                                continue
                            except (ValueError, IndexError):
                                pass
                    if self._pos_update_collecting and "|" in s:
                        parts = s.split("|")
                        if len(parts) >= 4:
                            try:
                                float(parts[1].strip())
                                self._pos_update_buf.append(s)
                                continue
                            except (ValueError, IndexError):
                                pass
                if kind == "force_close":
                    ok_count, fail_count, msgs = line
                    self._log(f"\n[강제청산] 성공 {ok_count}건, 실패 {fail_count}건\n", "stdout")
                    for m in msgs:
                        self._log(m + "\n", "stderr" if m.strip().startswith("❌") else "stdout")
                    self._update_position_table([])
                    if hasattr(self, "btn_force_close"):
                        self.btn_force_close.configure(state=tk.NORMAL)
                    continue
                if kind == "restart_mt5":
                    success, msg = line
                    if success:
                        self._log(f"[MT5 재시작] 완료. 터미널을 다시 실행했습니다.\n", "stdout")
                    else:
                        self._log(f"[MT5 재시작] 실패: {msg}\n", "stderr")
                    if hasattr(self, "btn_restart_mt5"):
                        self.btn_restart_mt5.configure(state=tk.NORMAL)
                    continue
                tag = "stderr" if kind == "err" else "stdout"
                self._log(line, tag)
        except queue.Empty:
            pass
        if self.process is not None:
            ret = self.process.poll()
            if ret is not None:
                self._log(f"\n[프로세스 종료] 코드: {ret}\n", "stdout")
                self._set_stopped()
        self.root.after(100, self._poll_log_queue)

    def _set_stopped(self):
        self.process = None
        self.status_var.set("중지됨")

    def _on_bb_tf_changed(self):
        try:
            tf = self._bb_tf_var.get().strip().upper()
            if tf not in ("M5", "M10", "H1"):
                return
            with open(BB_TF_FILE, "w", encoding="utf-8") as f:
                f.write(tf)
            self._bb_update_interval_sec = _bb_interval_sec_for_tf(tf)
            self._bb_last_update_time = None
        except Exception:
            pass

    def _on_bb_sym_changed(self):
        self._refresh_bb_display()

    def _refresh_bb_display(self):
        sym = self._bb_sym_var.get()
        if sym == "NAS100":
            self._bb_20_var.set(getattr(self, "_bb_nas_20", "—"))
            self._bb_4_var.set(getattr(self, "_bb_nas_4", "—"))
        else:
            self._bb_20_var.set(getattr(self, "_bb_xau_20", "—"))
            self._bb_4_var.set(getattr(self, "_bb_xau_4", "—"))

    def _parse_and_apply_bb_lines(self, lines):
        for ln in lines:
            if ln.strip().startswith("TF|"):
                continue
            parts = ln.split("|")
            if len(parts) < 3:
                continue
            sym = (parts[0] or "").strip()
            try:
                def _f(i):
                    return float(parts[i]) if i < len(parts) and parts[i].strip() else None
                if len(parts) >= 9:
                    o, h, l_, c = _f(1), _f(2), _f(3), _f(4)
                    b20u, b20l = _f(5), _f(6)
                    b4u, b4l = _f(7), _f(8)
                    ohlc_str = f"O: {o:.2f} H: {h:.2f} L: {l_:.2f} C: {c:.2f}" if o is not None and h is not None and l_ is not None and c is not None else "—"
                    s20 = (f"상: {b20u:.2f} 하: {b20l:.2f}" if b20u is not None and b20l is not None else "—")
                    s4 = (f"상: {b4u:.2f} 하: {b4l:.2f}" if b4u is not None and b4l is not None else "—")
                    if "XAUUSD" in sym:
                        self._bb_xau_ohlc, self._bb_xau_20, self._bb_xau_4 = ohlc_str, s20, s4
                    elif "NAS100" in sym:
                        self._bb_nas_ohlc, self._bb_nas_20, self._bb_nas_4 = ohlc_str, s20, s4
                else:
                    upper, lower = _f(1), _f(2)
                    s20 = (f"상: {upper:.2f} 하: {lower:.2f}" if upper is not None and lower is not None else "—")
                    if "XAUUSD" in sym:
                        self._bb_xau_20 = s20
                    elif "NAS100" in sym:
                        self._bb_nas_20 = s20
            except (ValueError, IndexError):
                pass
        self._refresh_bb_display()

    def _on_bb_refresh(self):
        try:
            tf = self._bb_tf_var.get().strip().upper()
            if tf in ("M5", "M10", "H1"):
                with open(BB_TF_FILE, "w", encoding="utf-8") as f:
                    f.write(tf)
        except Exception:
            pass
        if self._bb_refresh_btn is not None:
            self._bb_refresh_btn.configure(state=tk.DISABLED)

        def _run():
            stdout = ""
            if os.path.isfile(BB_REFRESH_SCRIPT):
                try:
                    result = subprocess.run(
                        [sys.executable, "-u", BB_REFRESH_SCRIPT],
                        cwd=SCRIPT_DIR,
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        timeout=30,
                    )
                    stdout = result.stdout or ""
                except Exception:
                    pass
            self.root.after(0, lambda: self._apply_bb_refresh_result(stdout))

        run_in_thread(_run, daemon=True)

    def _apply_bb_refresh_result(self, stdout_str: str):
        if getattr(self, "_bb_refresh_btn", None):
            self._bb_refresh_btn.configure(state=tk.NORMAL)
        lines = stdout_str.splitlines()
        buf = []
        collecting = False
        for s in lines:
            s = s.strip()
            if s == "[BB_BANDS]":
                buf = []
                collecting = True
                continue
            if s == "[/BB_BANDS]":
                if collecting:
                    self._parse_and_apply_bb_lines(buf)
                return
            if collecting and "|" in s:
                buf.append(s)

    def _clear_log(self):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _on_force_close(self):
        sym_sel = getattr(self, "force_close_symbol_var", None)
        sym_sel = sym_sel.get() if sym_sel else "전체"
        if sym_sel == "전체":
            if not messagebox.askokcancel("강제청산", "모든 포지션을 조건 없이 청산하고, 모든 예약 주문을 삭제합니다.\n계속하시겠습니까?"):
                return
        else:
            if not messagebox.askokcancel(
                "강제청산",
                f"{sym_sel} 포지션을 조건 없이 청산하고, {sym_sel} 예약 주문을 모두 삭제합니다.\n계속하시겠습니까?",
            ):
                return
        self.btn_force_close.configure(state=tk.DISABLED)
        symbol_arg = None if sym_sel == "전체" else (sym_sel + "+")
        self._log(f"\n[강제청산] {'전체' if not symbol_arg else symbol_arg} 실행 중...\n", "stdout")

        def _do_force_close():
            try:
                sys.path.insert(0, SCRIPT_DIR)
                import mt5_trade_utils as tr
                ok, fail, msgs = tr.close_all_positions_force(symbol=symbol_arg)
                self.log_queue.put(("force_close", (ok, fail, msgs)))
            except Exception as e:
                self.log_queue.put(("force_close", (0, 0, [f"오류: {e}"])))
        run_in_thread(_do_force_close, daemon=True)

    def _on_restart_mt5(self):
        if not messagebox.askokcancel("MT5 재시작", "MT5를 종료한 뒤 terminal64.exe를 강제 종료하고\n다시 실행합니다. 계속하시겠습니까?"):
            return
        self.btn_restart_mt5.configure(state=tk.DISABLED)
        self._log("\n[MT5 재시작] shutdown 및 프로세스 종료 중...\n", "stdout")

        def _do_restart_mt5():
            try:
                sys.path.insert(0, SCRIPT_DIR)
                import mt5_trade_utils as tr
                tr.shutdown_mt5()
                tr.remove_mt5_terminal_data_folder()
                time.sleep(2)
                if tr.start_mt5():
                    self.log_queue.put(("restart_mt5", (True, None)))
                else:
                    self.log_queue.put(("restart_mt5", (False, "터미널 실행 실패 (경로 확인)")))
            except Exception as e:
                self.log_queue.put(("restart_mt5", (False, str(e))))
        run_in_thread(_do_restart_mt5, daemon=True)

    def _read_pipe_to_queue(self, pipe, kind):
        try:
            for line in iter(pipe.readline, ""):
                self.log_queue.put((kind, line))
        except (ValueError, BrokenPipeError):
            pass

    def _check_deposit_schedule(self):
        try:
            now = datetime.now()
            today = now.date()
            today_str = today.isoformat()
            # 파일에 오늘 날짜가 있으면 이미 오늘 실행됨 → 스킵 (런처 재시작해도 하루 1회)
            try:
                if os.path.isfile(DEPOSIT_LAST_RUN_FILE):
                    with open(DEPOSIT_LAST_RUN_FILE, "r", encoding="utf-8") as f:
                        saved = (f.read() or "").strip()
                    if saved == today_str:
                        self._last_deposit_run_date = today
                        # 프로그램 재시작 후 스킵 시 텔레그램으로 한 번 알림
                        if not getattr(self, "_deposit_skip_telegram_sent", True):
                            self._deposit_skip_telegram_sent = True
                            try:
                                from telegram_sender_utils import send_telegram_msg
                                msg = (
                                    f"⏭️ **MT5 입출금일지** 업데이트 스킵\n"
                                    f"오늘({today_str}) 이미 실행되어 자동 실행하지 않습니다."
                                )
                                run_in_thread(lambda: send_telegram_msg(msg), daemon=True)
                            except Exception:
                                pass
                        self.root.after(60000, self._check_deposit_schedule)
                        return
            except Exception:
                pass
            if self._last_deposit_run_date != today and os.path.isfile(DEPOSIT_SCRIPT):
                # 08:04에 1회 실행. 1분 간격 체크라 08:04를 놓칠 수 있으므로 08:05~08:59 보충 구간 추가
                if now.hour == 8 and now.minute == 4:
                    self._last_deposit_run_date = today
                    self.log_queue.put(("out", "\n[입출금 자동] 08:04 실행\n"))
                    self._on_deposit_withdrawal()
                elif now.hour == 8 and 5 <= now.minute <= 59:
                    self._last_deposit_run_date = today
                    self.log_queue.put(("out", f"\n[입출금 자동] 08:04 보충 실행 ({now.hour:02d}:{now.minute:02d})\n"))
                    self._on_deposit_withdrawal()
        except Exception:
            pass
        self.root.after(60000, self._check_deposit_schedule)

    def _on_deposit_withdrawal(self):
        if not os.path.isfile(DEPOSIT_SCRIPT):
            self._log(f"오류: 스크립트를 찾을 수 없습니다.\n  {DEPOSIT_SCRIPT}\n", "stderr")
            return
        # 실행한 날짜 기록 → 자동 스케줄이 같은 날 다시 실행하지 않음 (하루 1회)
        try:
            today_str = date.today().isoformat()
            with open(DEPOSIT_LAST_RUN_FILE, "w", encoding="utf-8") as f:
                f.write(today_str)
        except Exception:
            pass
        self._last_deposit_run_date = date.today()
        self.log_queue.put(("out", "\n[입출금] mt5_deposit_withdrawal.py 실행 중...\n"))

        def _run():
            try:
                proc = subprocess.Popen(
                    [sys.executable, "-u", DEPOSIT_SCRIPT],
                    cwd=SCRIPT_DIR,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env={**os.environ, "PYTHONUNBUFFERED": "1"},
                )
                run_in_thread(lambda: self._read_pipe_to_queue(proc.stdout, "out"), daemon=True)
                run_in_thread(lambda: self._read_pipe_to_queue(proc.stderr, "err"), daemon=True)
                proc.wait()
                self.log_queue.put(("out", f"[입출금] 완료 (종료코드 {proc.returncode}).\n"))
            except Exception as e:
                self.log_queue.put(("err", f"[입출금] 실행 오류: {e}\n"))

        run_in_thread(_run, daemon=True)

    def _on_bar_backfill(self):
        """BAR 테이블 보충 + 볼린저밴드 재계산 실행 (로그는 스크립트에서 텔레그램 전송)."""
        if not os.path.isfile(BAR_BACKFILL_SCRIPT):
            self._log(f"오류: 스크립트를 찾을 수 없습니다.\n  {BAR_BACKFILL_SCRIPT}\n", "stderr")
            return
        self.log_queue.put(("out", "\n[BAR 보충·BB 갱신] 실행 중... (완료 시 텔레그램 전송)\n"))

        def _run():
            try:
                proc = subprocess.Popen(
                    [sys.executable, "-u", BAR_BACKFILL_SCRIPT],
                    cwd=SCRIPT_DIR,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env={**os.environ, "PYTHONUNBUFFERED": "1"},
                )
                run_in_thread(lambda: self._read_pipe_to_queue(proc.stdout, "out"), daemon=True)
                run_in_thread(lambda: self._read_pipe_to_queue(proc.stderr, "err"), daemon=True)
                proc.wait()
                self.log_queue.put(("out", f"[BAR 보충·BB 갱신] 완료 (종료코드 {proc.returncode}).\n"))
            except Exception as e:
                self.log_queue.put(("err", f"[BAR 보충·BB 갱신] 실행 오류: {e}\n"))

        run_in_thread(_run, daemon=True)

    def _on_high_low_update(self):
        """과거 4개 세션의 High/Low를 DB bars에서 집계해 session_high_low 테이블 갱신."""
        if pm_db is None:
            self._log("오류: position_monitor_db를 불러올 수 없습니다.\n", "stderr")
            return
        self.log_queue.put(("out", "\n[High/Low] 과거 4세션 갱신 중...\n"))

        def _run():
            try:
                conn = pm_db.get_connection(PM_DB_PATH)
                log_lines = pm_db.update_past_4_sessions_high_low(conn, list(PM_SYMBOLS))
                conn.close()
                self.log_queue.put(("out", "\n".join(log_lines) + "\n"))
                self.log_queue.put(("out", "[High/Low] 완료.\n"))
            except Exception as e:
                self.log_queue.put(("err", f"[High/Low] 오류: {e}\n"))

        run_in_thread(_run, daemon=True)

    def _on_supabase_sync(self):
        """로컬 DB(ktr_records, bars, session_high_low) 전체를 Supabase로 수동 동기화."""
        self._log("\n[Supabase 동기화] 로컬 DB → Supabase 업로드 중...\n", "stdout")

        def _run():
            try:
                sys.path.insert(0, SCRIPT_DIR)
                from supabase_sync import get_supabase_missing_counts, sync_all_from_local
                def _log_sync(line: str) -> None:
                    self.log_queue.put(("out", line + "\n"))
                # 누락 건수 먼저 표시 (과거 24시간 구간 기준)
                counts = get_supabase_missing_counts(KTR_DB_PATH, PM_DB_PATH, log_fn=_log_sync)
                if counts is not None:
                    self.log_queue.put(("out", f"  [Supabase 누락 건수] ktr_records {counts['ktr_records']}건, bars {counts['bars']}건, session_high_low {counts['session_high_low']}건 (과거 24시간 기준)\n"))
                ok, msg = sync_all_from_local(KTR_DB_PATH, PM_DB_PATH, log_fn=_log_sync)
                if ok:
                    self.log_queue.put(("out", "[Supabase 동기화] 완료.\n"))
                    self.log_queue.put(("out", msg + "\n"))
                else:
                    self.log_queue.put(("err", f"[Supabase 동기화] 실패: {msg}\n"))
            except Exception as e:
                self.log_queue.put(("err", f"[Supabase 동기화] 오류: {e}\n"))

        run_in_thread(_run, daemon=True)

    def _on_stop(self):
        """청산만 중지. 모니터 프로세스는 유지(포지션/DB/BB 갱신 계속)."""
        self._log("\n[청산 중지] 청산 기능만 중지합니다. 모니터(포지션/DB/BB)는 계속 실행됩니다.\n", "stdout")
        try:
            with open(CLOSING_ENABLED_FILE, "w", encoding="utf-8") as f:
                f.write("0")
        except Exception:
            pass
        self.status_var.set("청산 중지 (모니터 실행 중)")

    def _terminate_process(self):
        """모니터 프로세스 종료 (창 닫을 때만 호출)."""
        if self.process is None:
            return
        try:
            self.process.terminate()
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        except Exception:
            pass
        self.process = None
        self._set_stopped()

    def run(self):
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.mainloop()

    def _on_close(self):
        if self.process is not None:
            self._terminate_process()
        with self._log_file_lock:
            if self._log_file is not None:
                try:
                    self._log_file.close()
                except Exception:
                    pass
                self._log_file = None
        try:
            from single_instance import release_single_instance
            release_single_instance("position_monitor_launcher", SCRIPT_DIR)
        except Exception:
            pass
        self.root.destroy()
        os._exit(0)


def main():
    if hasattr(sys.stdout, "reconfigure") and sys.stdout is not None:
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    # 중복 실행 방지
    try:
        from single_instance import (
            try_acquire_single_instance,
            kill_process_forcefully,
            force_remove_lock,
        )
        root = tk.Tk()
        root.withdraw()
        acquired, existing_pid = try_acquire_single_instance("position_monitor_launcher", SCRIPT_DIR)
        if not acquired:
            if existing_pid is not None:
                if messagebox.askyesno(
                    "중복 실행",
                    "포지션 모니터가 이미 실행 중입니다.\n기존 인스턴스를 종료하고 새로 시작할까요?",
                ):
                    kill_process_forcefully(existing_pid, wait_after_sec=1.5)
                    force_remove_lock("position_monitor_launcher", SCRIPT_DIR)
                    time.sleep(2)
                    acquired, _ = try_acquire_single_instance("position_monitor_launcher", SCRIPT_DIR)
                    if not acquired:
                        messagebox.showerror(
                            "오류",
                            "기존 인스턴스를 종료했습니다.\n잠시 후 다시 실행해 주세요.",
                        )
                        root.destroy()
                        sys.exit(1)
                else:
                    root.destroy()
                    sys.exit(0)
            else:
                messagebox.showwarning("중복 실행", "포지션 모니터가 이미 실행 중입니다.\n다른 창을 확인해 주세요.")
                root.destroy()
                sys.exit(1)
        root.destroy()
    except ImportError:
        pass
    app = MonitorLauncherApp()
    app.run()


if __name__ == "__main__":
    startup_log = os.path.join(SCRIPT_DIR, "position_monitor_launcher_startup.log")
    try:
        main()
    except Exception as e:
        msg = traceback.format_exc()
        try:
            with open(startup_log, "a", encoding="utf-8") as f:
                f.write(f"\n--- {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n{msg}\n")
        except Exception:
            pass
        if sys.stdout is not None and getattr(sys, "stderr", None) is not None:
            print(msg)
        raise
