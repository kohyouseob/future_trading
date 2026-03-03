-- Supabase SQL Editor에서 실행: session_high_low 테이블이 없을 때만.
-- (PGRST205 Could not find the table 'public.session_high_low' 오류 해결)

CREATE TABLE IF NOT EXISTS session_high_low (
    symbol TEXT NOT NULL,
    session TEXT NOT NULL,
    session_date TEXT NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (symbol, session, session_date)
);

ALTER TABLE session_high_low DISABLE ROW LEVEL SECURITY;
