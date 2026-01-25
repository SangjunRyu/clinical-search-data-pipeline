# 현재 진행한 내용들

t3.small,  8gb ebs로 서버 띄움

여기에 superset과 pgsql 설치할 예정





# Gold Layer

분석용 데이터 마트 및 시각화 레이어

## 개요

| 항목 | 내용 |
|------|------|
| 입력 | S3 Silver Layer (Parquet) |
| 출력 | PostgreSQL (마트 테이블) |
| 시각화 | Apache Superset |
| 목적 | 비즈니스 분석 및 대시보드 |

> **Note**: Gold 레이어는 S3가 아닌 PostgreSQL 테이블로 직접 적재합니다. BI 도구(Superset) 연동 및 실시간 쿼리 성능을 위해 RDB를 선택했습니다.

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                      GOLD LAYER                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐     ┌──────────────────────────────────────┐  │
│  │  S3 Silver  │────▶│  Spark ETL (via SSHOperator)         │  │
│  │  (Parquet)  │     │  - 집계/변환                         │  │
│  └─────────────┘     │  - 마트 테이블 생성                  │  │
│                      └──────────────────────────────────────┘  │
│                                   │                             │
│                                   ▼                             │
│                           ┌─────────────┐                       │
│                           │  PostgreSQL │                       │
│                           │  (Gold Mart)│                       │
│                           │  - session  │                       │
│                           │  - traffic  │                       │
│                           │  - clinical │                       │
│                           │  - popular  │                       │
│                           └──────┬──────┘                       │
│                                  │                              │
│                                  ▼                              │
│                          ┌─────────────┐                        │
│                          │  Superset   │                        │
│                          │  Dashboard  │                        │
│                          └─────────────┘                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 디렉터리 구조

```
gold/
├── gold.md                   # 이 문서
├── docker-compose.yaml       # PostgreSQL + Superset 통합 구성
├── postgres/
│   └── init/
│       └── 01_create_tables.sql    # 테이블 초기화
└── superset/
    └── superset_config.py          # Superset 설정

# ETL 코드는 processing 레이어에 위치 (Spark 서버에서 SSHOperator로 실행)
processing/spark/jobs/
├── etl_to_gold.py          # Silver → PostgreSQL 마트 적재
└── load_to_postgres.py     # Silver → PostgreSQL 마트 적재 (대안)
```

---

## 데이터 마트 정의

### 1. 세션 분석 마트 (`mart_session_analysis`)

세션별 행동 분석

| 컬럼 | 타입 | 설명 |
|------|------|------|
| session_id | VARCHAR | 세션 ID |
| event_date | DATE | 이벤트 날짜 |
| click_count | INT | 클릭 수 |
| unique_docs | INT | 조회 문서 수 |
| first_click_ts | TIMESTAMP | 첫 클릭 시간 |
| last_click_ts | TIMESTAMP | 마지막 클릭 시간 |
| session_duration_sec | INT | 세션 지속 시간 (초) |

### 2. 일별 트래픽 마트 (`mart_daily_traffic`)

일별 트래픽 현황

| 컬럼 | 타입 | 설명 |
|------|------|------|
| event_date | DATE | 날짜 |
| total_events | INT | 총 이벤트 수 |
| unique_sessions | INT | 유니크 세션 수 |
| unique_documents | INT | 조회된 문서 수 |
| peak_hour | INT | 피크 시간대 |

### 3. 임상 분야 마트 (`mart_clinical_areas`)

임상 분야별 관심도 분석

| 컬럼 | 타입 | 설명 |
|------|------|------|
| event_date | DATE | 날짜 |
| clinical_area | VARCHAR | 임상 분야 |
| search_count | INT | 검색 수 |
| unique_sessions | INT | 유니크 세션 수 |

### 4. 인기 문서 마트 (`mart_popular_documents`)

인기 문서 순위

| 컬럼 | 타입 | 설명 |
|------|------|------|
| event_date | DATE | 날짜 |
| document_id | INT | 문서 ID |
| title | VARCHAR | 문서 제목 |
| view_count | INT | 조회 수 |
| unique_sessions | INT | 조회 세션 수 |

---

## Spark ETL Jobs

Airflow SSHOperator를 통해 Spark 서버에서 직접 실행됩니다.

### etl_to_gold.py

Silver 데이터를 집계하여 PostgreSQL 마트 테이블로 적재

```python
# 주요 변환 로직
# 1. 세션 분석 마트
session_mart = (
    silver_df
    .groupBy("session_id", "event_date")
    .agg(
        count("*").alias("click_count"),
        countDistinct("document_id").alias("unique_docs"),
        min("event_ts").alias("first_click_ts"),
        max("event_ts").alias("last_click_ts"),
    )
)

# 2. 일별 트래픽 마트
daily_mart = (
    silver_df
    .groupBy("event_date")
    .agg(
        count("*").alias("total_events"),
        countDistinct("session_id").alias("unique_sessions"),
        countDistinct("document_id").alias("unique_documents"),
    )
)
```

### load_to_postgres.py

Silver 데이터를 PostgreSQL 마트 테이블에 적재 (JDBC)

```python
# JDBC를 통한 PostgreSQL 적재
(
    mart_df
    .write
    .format("jdbc")
    .option("url", f"jdbc:postgresql://{host}:{port}/{db}")
    .option("dbtable", table_name)
    .option("user", user)
    .option("password", password)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)
```

---

## PostgreSQL 설정

PostgreSQL과 Superset은 `gold/docker-compose.yaml`에 통합 구성되어 있습니다.

### PostgreSQL 서비스

```yaml
postgres-gold:
  image: postgres:15
  container_name: postgres-gold
  environment:
    POSTGRES_USER: gold
    POSTGRES_PASSWORD: gold_password
    POSTGRES_DB: tripclick_gold
  ports:
    - "5433:5432"
  volumes:
    - postgres-gold-data:/var/lib/postgresql/data
    - ./postgres/init:/docker-entrypoint-initdb.d
  networks:
    - gold-network
```

### 테이블 초기화 (01_create_tables.sql)

```sql
-- 세션 분석 마트
CREATE TABLE IF NOT EXISTS mart_session_analysis (
    session_id VARCHAR(50),
    event_date DATE,
    click_count INT,
    unique_docs INT,
    first_click_ts TIMESTAMP,
    last_click_ts TIMESTAMP,
    session_duration_sec INT,
    PRIMARY KEY (session_id, event_date)
);

-- 일별 트래픽 마트
CREATE TABLE IF NOT EXISTS mart_daily_traffic (
    event_date DATE PRIMARY KEY,
    total_events INT,
    unique_sessions INT,
    unique_documents INT,
    peak_hour INT
);

-- 임상 분야 마트
CREATE TABLE IF NOT EXISTS mart_clinical_areas (
    event_date DATE,
    clinical_area VARCHAR(100),
    search_count INT,
    unique_sessions INT,
    PRIMARY KEY (event_date, clinical_area)
);

-- 인기 문서 마트
CREATE TABLE IF NOT EXISTS mart_popular_documents (
    event_date DATE,
    document_id INT,
    title VARCHAR(500),
    view_count INT,
    unique_sessions INT,
    PRIMARY KEY (event_date, document_id)
);

-- 인덱스
CREATE INDEX idx_session_date ON mart_session_analysis(event_date);
CREATE INDEX idx_clinical_date ON mart_clinical_areas(event_date);
CREATE INDEX idx_popular_date ON mart_popular_documents(event_date);
```

---

## Apache Superset 설정

Superset은 `gold/docker-compose.yaml`에 통합 구성되어 있으며, `gold-network`를 통해 postgres-gold와 연결됩니다.

### Superset 서비스

```yaml
superset:
  image: apache/superset:3.1.0
  container_name: superset
  ports:
    - "8088:8088"
  volumes:
    - ./superset/superset_config.py:/app/pythonpath/superset_config.py
  depends_on:
    - superset-db
    - postgres-gold
  networks:
    - gold-network
```

### Superset Database 연결

Superset UI에서 PostgreSQL Gold DB 연결:

```
Database: PostgreSQL
Host: postgres-gold
Port: 5432
Database: tripclick_gold
Username: gold
Password: gold_password
```

---

## 대시보드 구성

### 1. 일별 트래픽 개요
- 일별 이벤트 수 추이 (Line Chart)
- 일별 유니크 세션 수 (Bar Chart)
- 피크 시간대 분포 (Heatmap)

### 2. 임상 분야 분석
- 분야별 검색 비율 (Pie Chart)
- 분야별 트렌드 (Area Chart)
- Top 10 임상 분야 (Table)

### 3. 세션 행동 분석
- 세션 지속 시간 분포 (Histogram)
- 클릭 수 분포 (Box Plot)
- 리텐션 분석 (Cohort Chart)

### 4. 인기 문서
- Top 20 인기 문서 (Table)
- 문서별 조회 추이 (Line Chart)

---

## 실행 방법

```bash
cd gold

# 1. PostgreSQL + Superset 시작 (통합)
docker-compose up -d

# 2. ETL 실행 (processing 서버의 Spark에서 실행)
# Airflow DAG 또는 수동 실행
spark-submit /opt/spark/jobs/etl_to_gold.py
spark-submit /opt/spark/jobs/load_to_postgres.py

# 3. Superset 접속
# http://localhost:8088 (admin/admin)
```

---

## TODO

- [x] Spark ETL 코드 작성 (etl_to_gold.py)
- [x] PostgreSQL 적재 코드 작성 (load_to_postgres.py)
- [x] 자동화 스케줄링 (Airflow SSHOperator 연동)
- [ ] Superset 대시보드 템플릿
- [ ] 데이터 품질 검증 로직
