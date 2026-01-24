# Gold Layer

분석용 데이터 마트 및 시각화 레이어

## 개요

| 항목 | 내용 |
|------|------|
| 입력 | S3 Silver Layer |
| 출력 | S3 Gold / PostgreSQL |
| 시각화 | Apache Superset |
| 목적 | 비즈니스 분석 및 대시보드 |

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                      GOLD LAYER                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐     ┌──────────────────────────────────────┐  │
│  │  S3 Silver  │────▶│  Spark ETL                           │  │
│  │  (Parquet)  │     │  - 집계/변환                         │  │
│  └─────────────┘     │  - 마트 테이블 생성                  │  │
│                      └──────────────────────────────────────┘  │
│                                   │                             │
│                      ┌────────────┴────────────┐               │
│                      ▼                         ▼               │
│              ┌─────────────┐           ┌─────────────┐         │
│              │  S3 Gold    │           │  PostgreSQL │         │
│              │  (Parquet)  │           │  (RDB)      │         │
│              └─────────────┘           └──────┬──────┘         │
│                                               │                 │
│                                               ▼                 │
│                                       ┌─────────────┐          │
│                                       │  Superset   │          │
│                                       │  Dashboard  │          │
│                                       └─────────────┘          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 디렉터리 구조

```
gold/
├── gold.md                   # 이 문서
├── spark/
│   └── jobs/
│       ├── etl_to_gold.py          # Silver → Gold 변환
│       └── load_to_postgres.py     # Gold → PostgreSQL 적재
├── postgres/
│   ├── docker-compose.yaml         # PostgreSQL 서버
│   └── init/
│       └── 01_create_tables.sql    # 테이블 초기화
├── superset/
│   ├── docker-compose.yaml         # Superset 서버
│   └── superset_config.py          # Superset 설정
└── config/
    └── config.yaml                 # 공통 설정
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

### etl_to_gold.py

Silver 데이터를 Gold 마트로 변환

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

Gold 마트를 PostgreSQL에 적재

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

### docker-compose.yaml

```yaml
version: "3.8"

services:
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
      - ./init:/docker-entrypoint-initdb.d

volumes:
  postgres-gold-data:
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

### docker-compose.yaml

```yaml
version: "3.8"

services:
  superset:
    image: apache/superset:3.1.0
    container_name: superset
    ports:
      - "8088:8088"
    environment:
      SUPERSET_SECRET_KEY: "your-secret-key"
      SUPERSET_LOAD_EXAMPLES: "false"
    volumes:
      - ./superset_config.py:/app/pythonpath/superset_config.py
      - superset-data:/app/superset_home
    depends_on:
      - superset-db
    command: >
      bash -c "
        superset db upgrade &&
        superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin &&
        superset init &&
        superset run -h 0.0.0.0 -p 8088
      "

  superset-db:
    image: postgres:15
    container_name: superset-db
    environment:
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: superset
      POSTGRES_DB: superset
    volumes:
      - superset-db-data:/var/lib/postgresql/data

volumes:
  superset-data:
  superset-db-data:
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

# 1. PostgreSQL 시작
docker-compose -f postgres/docker-compose.yaml up -d

# 2. Superset 시작
docker-compose -f superset/docker-compose.yaml up -d

# 3. ETL 실행 (Airflow DAG 또는 수동)
spark-submit /opt/spark/jobs/etl_to_gold.py
spark-submit /opt/spark/jobs/load_to_postgres.py

# 4. Superset 접속
# http://localhost:8088 (admin/admin)
```

---

## TODO

- [ ] Spark ETL 코드 작성 (etl_to_gold.py)
- [ ] PostgreSQL 적재 코드 작성 (load_to_postgres.py)
- [ ] Superset 대시보드 템플릿
- [ ] 데이터 품질 검증 로직
- [ ] 자동화 스케줄링 (Airflow 연동)
