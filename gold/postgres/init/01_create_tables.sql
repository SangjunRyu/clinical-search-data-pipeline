-- TripClick Gold Mart Tables
-- Superset 시각화용 데이터 마트

-- =========================
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
