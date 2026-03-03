-- Supabase에 position_status_sent 테이블이 없을 때만 실행하세요.
-- Dashboard → SQL Editor → New query에 붙여넣고 Run.

CREATE TABLE IF NOT EXISTS position_status_sent (
    slot_name TEXT PRIMARY KEY,
    sent_at TEXT NOT NULL
);
ALTER TABLE position_status_sent DISABLE ROW LEVEL SECURITY;
