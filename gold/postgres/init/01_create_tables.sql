-- TripClick Gold Mart Tables
-- Superset 시각화용 데이터 마트 (Hot/Cold 2계층)

-- =========================================================
-- HOT GOLD: Near Real-Time 마트 (1~5분 마이크로배치)
-- =========================================================

-- 1. 실시간 트래픽 마트 (분 단위)
CREATE TABLE IF NOT EXISTS mart_realtime_traffic_minute (
    event_minute TIMESTAMP NOT NULL,
    total_clicks INT DEFAULT 0,
    unique_sessions INT DEFAULT 0,
    unique_docs INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_minute)
);

COMMENT ON TABLE mart_realtime_traffic_minute IS '분 단위 실시간 트래픽 (Hot Gold)';
COMMENT ON COLUMN mart_realtime_traffic_minute.event_minute IS '분 단위 버킷';
COMMENT ON COLUMN mart_realtime_traffic_minute.total_clicks IS '총 클릭 수';
COMMENT ON COLUMN mart_realtime_traffic_minute.unique_sessions IS '유니크 세션 수';
COMMENT ON COLUMN mart_realtime_traffic_minute.unique_docs IS '유니크 문서 수';

-- 2. 실시간 인기 문서 TOP N (최근 1시간)
CREATE TABLE IF NOT EXISTS mart_realtime_top_docs_1h (
    snapshot_ts TIMESTAMP NOT NULL,
    rank INT NOT NULL,
    document_id INT NOT NULL,
    title VARCHAR(500),
    click_count INT DEFAULT 0,
    unique_sessions INT DEFAULT 0,
    PRIMARY KEY (snapshot_ts, rank)
);

COMMENT ON TABLE mart_realtime_top_docs_1h IS '최근 1시간 인기 문서 TOP N (Hot Gold)';
COMMENT ON COLUMN mart_realtime_top_docs_1h.snapshot_ts IS '스냅샷 시각';
COMMENT ON COLUMN mart_realtime_top_docs_1h.rank IS '순위 (1~20)';

-- 3. 실시간 임상영역 트렌드 (최근 24시간)
CREATE TABLE IF NOT EXISTS mart_realtime_clinical_trend_24h (
    snapshot_ts TIMESTAMP NOT NULL,
    clinical_area VARCHAR(100) NOT NULL,
    click_count INT DEFAULT 0,
    unique_sessions INT DEFAULT 0,
    trend_pct DECIMAL(5,2) DEFAULT 0.00,
    PRIMARY KEY (snapshot_ts, clinical_area)
);

COMMENT ON TABLE mart_realtime_clinical_trend_24h IS '최근 24시간 임상영역 트렌드 (Hot Gold)';
COMMENT ON COLUMN mart_realtime_clinical_trend_24h.trend_pct IS '전일 대비 증감률 (%)';

-- 4. 이상징후 감지 마트
CREATE TABLE IF NOT EXISTS mart_realtime_anomaly_sessions (
    detected_ts TIMESTAMP NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    click_count INT DEFAULT 0,
    severity VARCHAR(20) DEFAULT 'WARNING',
    PRIMARY KEY (detected_ts, session_id)
);

COMMENT ON TABLE mart_realtime_anomaly_sessions IS '세션 클릭 폭증 이상징후 감지 (Hot Gold)';
COMMENT ON COLUMN mart_realtime_anomaly_sessions.severity IS '심각도 (WARNING/CRITICAL)';

-- Hot Gold 인덱스
CREATE INDEX IF NOT EXISTS idx_realtime_traffic_minute ON mart_realtime_traffic_minute(event_minute DESC);
CREATE INDEX IF NOT EXISTS idx_realtime_top_docs_snapshot ON mart_realtime_top_docs_1h(snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS idx_realtime_clinical_snapshot ON mart_realtime_clinical_trend_24h(snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS idx_realtime_anomaly_detected ON mart_realtime_anomaly_sessions(detected_ts DESC);
CREATE INDEX IF NOT EXISTS idx_realtime_anomaly_severity ON mart_realtime_anomaly_sessions(severity);

-- =========================================================
-- COLD GOLD: Daily Batch 마트 (T+1 정합성)
-- =========================================================

-- 1. 세션 분석 마트
-- =========================
CREATE TABLE IF NOT EXISTS mart_session_analysis (
    session_id VARCHAR(50) NOT NULL,
    event_date DATE NOT NULL,
    click_count INT,
    unique_docs INT,
    first_click_ts TIMESTAMP,
    last_click_ts TIMESTAMP,
    session_duration_sec INT,
    PRIMARY KEY (session_id, event_date)
);

COMMENT ON TABLE mart_session_analysis IS '세션별 행동 분석 마트';
COMMENT ON COLUMN mart_session_analysis.session_id IS '세션 ID';
COMMENT ON COLUMN mart_session_analysis.click_count IS '클릭 수';
COMMENT ON COLUMN mart_session_analysis.unique_docs IS '조회 문서 수';
COMMENT ON COLUMN mart_session_analysis.session_duration_sec IS '세션 지속 시간 (초)';

-- =========================
-- 2. 일별 트래픽 마트
-- =========================
CREATE TABLE IF NOT EXISTS mart_daily_traffic (
    event_date DATE PRIMARY KEY,
    total_events INT,
    unique_sessions INT,
    unique_documents INT,
    peak_hour INT
);

COMMENT ON TABLE mart_daily_traffic IS '일별 트래픽 현황';
COMMENT ON COLUMN mart_daily_traffic.total_events IS '총 이벤트 수';
COMMENT ON COLUMN mart_daily_traffic.unique_sessions IS '유니크 세션 수';
COMMENT ON COLUMN mart_daily_traffic.unique_documents IS '조회된 문서 수';
COMMENT ON COLUMN mart_daily_traffic.peak_hour IS '피크 시간대 (0-23)';

-- =========================
-- 3. 임상 분야 마트
-- =========================
CREATE TABLE IF NOT EXISTS mart_clinical_areas (
    event_date DATE NOT NULL,
    clinical_area VARCHAR(100) NOT NULL,
    search_count INT,
    unique_sessions INT,
    PRIMARY KEY (event_date, clinical_area)
);

COMMENT ON TABLE mart_clinical_areas IS '임상 분야별 관심도 분석';
COMMENT ON COLUMN mart_clinical_areas.clinical_area IS '임상 분야명';
COMMENT ON COLUMN mart_clinical_areas.search_count IS '검색 수';

-- =========================
-- 4. 인기 문서 마트
-- =========================
CREATE TABLE IF NOT EXISTS mart_popular_documents (
    event_date DATE NOT NULL,
    document_id INT NOT NULL,
    title VARCHAR(500),
    view_count INT,
    unique_sessions INT,
    PRIMARY KEY (event_date, document_id)
);

COMMENT ON TABLE mart_popular_documents IS '인기 문서 순위';
COMMENT ON COLUMN mart_popular_documents.view_count IS '조회 수';
COMMENT ON COLUMN mart_popular_documents.unique_sessions IS '조회 세션 수';

-- =========================
-- 인덱스
-- =========================
CREATE INDEX IF NOT EXISTS idx_session_date ON mart_session_analysis(event_date);
CREATE INDEX IF NOT EXISTS idx_session_duration ON mart_session_analysis(session_duration_sec);

CREATE INDEX IF NOT EXISTS idx_clinical_date ON mart_clinical_areas(event_date);
CREATE INDEX IF NOT EXISTS idx_clinical_area ON mart_clinical_areas(clinical_area);

CREATE INDEX IF NOT EXISTS idx_popular_date ON mart_popular_documents(event_date);
CREATE INDEX IF NOT EXISTS idx_popular_views ON mart_popular_documents(view_count DESC);

-- =========================
-- 권한 설정
-- =========================
GRANT SELECT ON ALL TABLES IN SCHEMA public TO gold;
