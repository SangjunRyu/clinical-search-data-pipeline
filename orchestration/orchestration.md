# Orchestration Layer

전체 파이프라인을 오케스트레이션하는 별도 Airflow 서버

## 개요

| 항목 | 내용 |
|------|------|
| 역할 | 전체 DAG 관리 및 스케줄링 |
| 위치 | 별도 EC2 인스턴스 |
| 연동 | Remote Docker API (Producer), SSHOperator (Spark) |

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                   ORCHESTRATION SERVER                          │
│                   (Airflow on EC2)                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                  Airflow Scheduler                       │   │
│  │                                                          │   │
│  │  ┌──────────────────────────────────────────────────┐   │   │
│  │  │  DAG: tripclick_daily_pipeline                   │   │   │
│  │  │                                                   │   │   │
│  │  │  15:00 ─┬─▶ DockerOperator (Producer server0)    │   │   │
│  │  │         └─▶ DockerOperator (Producer server1)    │   │   │
│  │  │               ↓                                   │   │   │
│  │  │  15:00 ────▶ SSHOperator (Streaming → Silver)    │   │   │
│  │  │               ↓                                   │   │   │
│  │  │  17:00 ────▶ SSHOperator (Batch → Bronze)        │   │   │
│  │  │               ↓                                   │   │   │
│  │  │  18:00 ────▶ SSHOperator (ETL → PostgreSQL)      │   │   │
│  │  └──────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└──────────────────────────────┬──────────────────────────────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
           ▼                   ▼                   ▼
    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │  WebServer  │     │   Spark     │     │  WebServer  │
    │  (server0)  │     │  Cluster    │     │  (server1)  │
    │  Docker API │     │  SSH: 22    │     │  Docker API │
    └─────────────┘     └─────────────┘     └─────────────┘
```

---

## 디렉터리 구조

```
orchestration/
├── orchestration.md          # 이 문서
├── docker-compose.yaml       # Airflow 클러스터
├── Dockerfile                # Airflow 커스텀 이미지
├── requirements.txt          # Python 의존성
├── dags/
│   ├── tripclick_daily_pipeline.py   # 메인 DAG
│   └── common/
│       └── config.py         # 공통 설정
├── config/
│   ├── airflow.cfg           # Airflow 설정 (선택)
│   └── connections.yaml      # Connection 정보 (Git 제외)
└── scripts/
    └── init_connections.sh   # Connection 초기화 스크립트
```

---

## DAG 구성

### tripclick_daily_pipeline.py

전체 파이프라인을 일일 단위로 실행하는 메인 DAG

```
┌────────────────────────────────────────────────────────────────┐
│                  tripclick_daily_pipeline                      │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  [15:00]                                                       │
│     │                                                          │
│     ├──▶ producer_server0 (DockerOperator)                    │
│     │         │                                                │
│     ├──▶ producer_server1 (DockerOperator)                    │
│     │         │                                                │
│     └──▶ streaming_to_silver (SSHOperator)                    │
│                   │                                            │
│  [17:00]          ▼                                            │
│     └──▶ batch_to_bronze (SSHOperator)                        │
│                   │                                            │
│  [18:00]          ▼                                            │
│     └──▶ etl_to_gold (SSHOperator)                            │
│                   │                                            │
│                   ▼                                            │
│     └──▶ load_to_postgres (SSHOperator)                       │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

## SSHOperator 사용 이유

SparkSubmitOperator의 Client Mode 네트워크 문제로 인해 SSHOperator로 전환:

### 문제 상황

- Airflow와 Spark가 각각 다른 서버의 Docker 컨테이너에서 실행
- Client Mode에서 Driver는 Airflow 컨테이너에서 실행
- Spark Worker들이 Driver로 결과 반환 시 Docker 내부 IP(172.x.x.x)로 통신 시도
- 서로 다른 Docker Bridge Network 간 직접 통신 불가

### 검토한 대안

| 대안 | 장점 | 단점 |
|------|------|------|
| spark.driver.host 설정 | 네이티브 방식 | 호스트 IP 지정 + 포트 매핑 필요, 설정 복잡 |
| Host Network Mode | 단순한 네트워크 | 양쪽 모두 변경 필요, 기존 환경 영향 큼 |
| **SSHOperator** | 네트워크 문제 완전 회피 | SSH 키 관리 필요 |

### 선택 이유

1. 가장 빠르고 안정적인 해결책
2. 수동으로 Spark 서버에서 실행할 때와 동일한 동작 보장
3. 기존 인프라 변경 없이 SSH 연결만으로 구현 가능

### 실행 흐름

```
┌─────────────────────┐         SSH          ┌─────────────────────┐
│   Airflow Server    │ ───────────────────▶ │   Spark Server      │
│                     │                       │                     │
│  SSHOperator        │                       │  docker exec        │
│  (spark_ssh conn)   │                       │  spark-master       │
│                     │                       │  spark-submit ...   │
└─────────────────────┘                       └─────────────────────┘
```

> **프로덕션 개선 방향**: Spark on Kubernetes + KubernetesPodOperator 권장

---

## Remote Docker API

외부 웹서버에서 Docker 컨테이너를 실행하기 위한 설정

### 웹서버 측 설정 (server0, server1)

```bash
# /etc/docker/daemon.json
{
  "hosts": ["unix:///var/run/docker.sock", "tcp://0.0.0.0:2375"]
}

# 또는 systemd override
# /etc/systemd/system/docker.service.d/override.conf
[Service]
ExecStart=
ExecStart=/usr/bin/dockerd -H fd:// -H tcp://0.0.0.0:2375
```

### Airflow Connection 설정

```bash
# Docker Connection (server0)
airflow connections add docker_server0 \
  --conn-type docker \
  --conn-host tcp://<WEBSERVER0_IP>:2375

# Docker Connection (server1)
airflow connections add docker_server1 \
  --conn-type docker \
  --conn-host tcp://<WEBSERVER1_IP>:2375

# Spark SSH Connection (SSHOperator용)
airflow connections add spark_ssh \
  --conn-type ssh \
  --conn-host <SPARK_SERVER_IP> \
  --conn-login ubuntu \
  --conn-extra '{"key_file": "/opt/airflow/config/spark_key.pem"}'

# AWS S3 Connection
airflow connections add aws_s3 \
  --conn-type aws \
  --conn-login <ACCESS_KEY> \
  --conn-password <SECRET_KEY> \
  --conn-extra '{"region_name": "ap-northeast-2"}'

# PostgreSQL Gold Connection
airflow connections add postgres_gold \
  --conn-type postgres \
  --conn-host <POSTGRES_HOST> \
  --conn-port 5432 \
  --conn-login gold \
  --conn-password <POSTGRES_PASSWORD> \
  --conn-schema tripclick_gold
```

---

airflow/env/airflow_variables.env 속의 variables 추가하는 작업 필요
init_airflow_variables.sh 실행필요

## Docker Compose

### docker-compose.yaml

```yaml
version: "3.8"

x-airflow-common: &airflow-common
  build: .
  environment:
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./config:/opt/airflow/config
  depends_on:
    - postgres
    - redis

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data

  redis:
    image: redis:7.2

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

  airflow-worker:
    <<: *airflow-common
    command: celery worker

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db init
        airflow users create \
          --username admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com \
          --password admin

volumes:
  postgres-data:
```

---

## 의존성

### requirements.txt

```
apache-airflow==2.10.5
apache-airflow-providers-docker==3.8.0
apache-airflow-providers-ssh==3.10.0
apache-airflow-providers-amazon==8.16.0
apache-airflow-providers-postgres==5.10.0
```

---

## 실행 방법

```bash
cd orchestration
sudo chown -R 50000:0 logs dags plugins config
로 권한 바꾸어야 도커 compose 가능
# 초기화 및 시작

docker compose up -d

# Connection 설정
./scripts/init_connections.sh

# 웹 UI 접속
# http://localhost:8080 (admin/admin)
```

---

## 보안 고려사항

- [ ] Docker API TLS 인증 설정
- [ ] Airflow Fernet Key 설정
- [ ] Connection 정보 암호화 (AWS Secrets Manager)
- [ ] 네트워크 방화벽 규칙 설정

---

## TODO

- [x] Docker Compose 파일 완성
- [x] DAG 코드 작성 (SSHOperator 방식)
- [ ] Connection 초기화 스크립트 완성
- [ ] 모니터링 대시보드 (Grafana)
- [ ] 알림 설정 (Slack/Email)
