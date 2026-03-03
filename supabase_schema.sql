-- Supabase SQL Editor에서 "아래 전체"를 한 번에 실행하세요.
-- 로컬 scheduler.db와 동일한 3개 테이블이 모두 생성됩니다.
--   (1) ktr_records  (2) bars  (3) session_high_low

-- 1) KTR 기록 (로컬 ktr_records)
CREATE TABLE IF NOT EXISTS ktr_records (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    session TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    record_date TEXT,
    ktr_value REAL NOT NULL,
    balance REAL,
    lot_1st REAL,
    lot_2nd REAL,
    lot_3rd REAL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, session, timeframe, record_date)
);

-- 2) 봉 데이터 (로컬 bars)
CREATE TABLE IF NOT EXISTS bars (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    bar_time TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    bb20_upper REAL,
    bb20_lower REAL,
    bb4_upper REAL,
    bb4_lower REAL,
    sma20 REAL,
    sma120 REAL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (symbol, timeframe, bar_time)
);

-- 3) 세션별 고/저 (로컬 session_high_low)
CREATE TABLE IF NOT EXISTS session_high_low (
    symbol TEXT NOT NULL,
    session TEXT NOT NULL,
    session_date TEXT NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (symbol, session, session_date)
);

-- 동기화용 anon 키로 쓰기 허용: RLS 비활성화 (필요 시 대시보드에서 다시 켜고 정책 설정)
ALTER TABLE ktr_records DISABLE ROW LEVEL SECURITY;
ALTER TABLE bars DISABLE ROW LEVEL SECURITY;
ALTER TABLE session_high_low DISABLE ROW LEVEL SECURITY;

-- 4) 예약 오더 (로컬 ktr_reservations.json → Supabase 관리용, 선택 적용)
CREATE TABLE IF NOT EXISTS ktr_reservations (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    timeframe_label TEXT NOT NULL,
    mt5_timeframe BIGINT,
    conditions JSONB NOT NULL DEFAULT '[]',
    weight_pct REAL NOT NULL,
    n_value REAL NOT NULL,
    n_display TEXT,
    num_positions INT NOT NULL,
    sl_from_n BOOLEAN DEFAULT true,
    session TEXT,
    ktr_tf TEXT,
    tp_option TEXT,
    sl_option TEXT,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE ktr_reservations DISABLE ROW LEVEL SECURITY;

-- 5) 봉 마감 텔레그램 전송 이력 (중복 전송 방지용. 로컬 telegram_bar_sent와 동일)
CREATE TABLE IF NOT EXISTS telegram_bar_sent (
    tf_label TEXT NOT NULL,
    bar_key TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    PRIMARY KEY (tf_label, bar_key)
);
ALTER TABLE telegram_bar_sent DISABLE ROW LEVEL SECURITY;

-- 6) 포지션 점검 결과 텔레그램 전송 이력 (매시 :00/:15/:30/:45 중복 전송 방지)
CREATE TABLE IF NOT EXISTS position_status_sent (
    slot_name TEXT PRIMARY KEY,
    sent_at TEXT NOT NULL
);
ALTER TABLE position_status_sent DISABLE ROW LEVEL SECURITY;
