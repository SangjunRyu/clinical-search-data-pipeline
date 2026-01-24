# Ingestion Layer

원천 데이터를 수집하여 Kafka 브로커에 전달하는 레이어

## 개요

| 항목 | 내용 |
|------|------|
| 데이터 소스 | `ingestion/data/server0`, `server1` 하위 JSON 로그 |
| 대상 | Kafka 토픽 (`tripclick_raw_logs`) |
| 전송 방식 | 실시간 스트리밍 (timestamp 기반) |
| 중복 처리 | xxhash 기반 dedup_key 생성 |

---

## 디렉터리 구조

```
ingestion/
├── Dockerfile               # Producer Docker 이미지
├── docker-compose.yaml      # 로컬 테스트 및 웹서버 배포용
├── requirements.txt         # Python 의존성
├── producer/
│   └── producer.py          # Kafka Producer 메인 코드
├── config/
│   ├── config.yaml          # Kafka 브로커 설정 (Git 제외)
│   └── config.yaml.example  # 설정 예시
├── utils/
│   └── logging.py           # 로깅 유틸리티
├── logs/                    # 런타임 로그 (Git 제외)
├── data/                    # 실제 전송할 로그 데이터
│   ├── server0/
│   │   └── date=YYYY-MM-DD/
│   │       └── events.json
│   └── server1/
│       └── date=YYYY-MM-DD/
│           └── events.json
└── sample_data/             # 샘플 데이터 및 전처리 스크립트
```

---

## 데이터 설명

### 데이터 기간
- **범위**: 2026-01-01 ~ 2026-02-01 (daily)
- **시간대**: 15:00 ~ 16:00 (1시간으로 압축)
- **목적**: 실시간 스트리밍 시뮬레이션

### 데이터 스키마
```json
{
  "DateCreated": "/Date(1357004977650)/",
  "SessionId": "smyrvv450lybhv55hrh0pp3m",
  "DocumentId": 1332132,
  "Url": "http://...",
  "Title": "...",
  "DOI": "",
  "ClinicalAreas": ",Cardiology,Emergency Medicine",
  "Keywords": "Stroke",
  "Documents": [],
  "event_ts": "2026-01-01T15:00:01.902456+00:00",
  "event_date": "2026-01-01",
  "dedup_key": "a1b2c3d4e5f6..."  // producer가 추가
}
```

---

## producer.py

### 전송 모드

| 모드 | 설명 | 사용 시점 |
|------|------|----------|
| `realtime` | timestamp 기반 실시간 전송 | 스트리밍 시뮬레이션 |
| `batch` | 즉시 전체 전송 | 초기 적재, 테스트 |

### 실행 방법

```bash
cd ingestion

# 실시간 모드 (기본)
python3 -m producer.producer \
  --file data/server0/date=2026-01-01/events.json \
  --topic tripclick_raw_logs \
  --mode realtime

# 배치 모드
python3 -m producer.producer \
  --file data/server1/date=2026-01-01/events.json \
  --topic tripclick_raw_logs \
  --mode batch
```

### 주요 기능

#### 1. 실시간 전송 로직
- 파일 내 레코드를 `event_ts` 기준으로 정렬
- 첫 번째 이벤트 시간을 기준으로 상대 시간 계산
- 실제 경과 시간에 맞춰 Kafka로 전송

#### 2. Dedup Key 생성
- **알고리즘**: xxhash64 (fallback: md5)
- **조합**: `session_id | document_id | event_ts`
- **용도**: Spark Streaming에서 중복 제거

```python
dedup_key = xxhash.xxh64(f"{session_id}|{document_id}|{event_ts}").hexdigest()
```

#### 3. At-Least-Once 전송
- 비동기 fire-and-forget 방식
- dedup_key로 Spark에서 중복 제거
- 실패 시 재전송 가능 (retries=1)

---

## 설정

### config/config.yaml.example
```yaml
kafka:
  brokers:
    - <KAFKA_BROKER_IP>:9092
    - <KAFKA_BROKER_IP>:9093
    - <KAFKA_BROKER_IP>:9094
```

> `config.yaml.example`을 `config.yaml`로 복사 후 실제 IP로 수정

### 환경변수 오버라이드
```bash
export KAFKA_BROKERS="broker1:9092,broker2:9092"
```

---

## Docker 이미지

### 이미지 빌드

```bash
cd ingestion

# 이미지 빌드
docker build -t tripclick-producer:latest .

# 빌드 확인
docker images | grep tripclick-producer
```

### 로컬 테스트 실행

```bash
# 네트워크 생성 (최초 1회)
docker network create tripclick-network

# 환경변수 설정
export KAFKA_BROKERS=<KAFKA_BROKER_IP>:9092

# 실행 (realtime 모드)
docker run --rm \
  --network tripclick-network \
  -v $(pwd)/data:/app/data:ro \
  -v $(pwd)/config:/app/config:ro \
  -e KAFKA_BROKERS=$KAFKA_BROKERS \
  tripclick-producer:latest \
  --file /app/data/server0/date=2026-01-01/events.json \
  --topic tripclick_raw_logs \
  --mode realtime

# 또는 docker-compose 사용
docker-compose up producer
```

---

## 웹서버 노드 배포

Airflow DockerOperator로 실행하기 위해 각 웹서버(server0, server1)에 배포

### 1. 웹서버에 파일 복사

```bash
# server0에 배포
scp -r ingestion/ ubuntu@<WEBSERVER0_IP>:/home/ubuntu/tripclick/

# server1에 배포
scp -r ingestion/ ubuntu@<WEBSERVER1_IP>:/home/ubuntu/tripclick/
```

### 2. Remote Docker API 활성화

각 웹서버에서 Docker API를 외부에서 접근 가능하도록 설정:

# systemd override 사용
cd /lib/systemd/system
sudo vi docker.service

-- 설정 변경
[Service]
ExecStart=/usr/bin/dockerd -H fd:// -H tcp://airflow_serverip:2375


# Docker 재시작
sudo systemctl daemon-reload
sudo systemctl restart docker

# 확인
curl http://localhost:2375/version
```

### 3. 웹서버에서 이미지 빌드

```bash
# 웹서버 접속 후
cd /home/ubuntu/tripclick/ingestion

# 이미지 빌드
docker build -t tripclick-producer:latest .
```

### 4. 데이터 디렉터리 구조

웹서버의 실제 로그 파일 위치와 마운트 경로 매핑:

```
웹서버 호스트                          컨테이너 내부
─────────────────────────────────────────────────────────
/home/ubuntu/tripclick/ingestion/data  →  /app/data
└── server0/                               └── server0/
    └── date=2026-01-01/                       └── date=2026-01-01/
        └── events.json                            └── events.json
```

---

## 외부 Airflow 연동

외부 Airflow 노드에서 DockerOperator를 통해 실행

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
```

### DAG에서 DockerOperator 사용

```python
from airflow.providers.docker.operators.docker import DockerOperator

# Server0 Producer
producer_server0 = DockerOperator(
    task_id="producer_server0",
    image="tripclick-producer:latest",
    api_version="auto",
    docker_url="tcp://<WEBSERVER0_IP>:2375",  # Remote Docker API
    network_mode="tripclick-network",

    # 볼륨 마운트 (호스트 경로 → 컨테이너 경로)
    mounts=[
        {
            "source": "/home/ubuntu/tripclick/ingestion/data",
            "target": "/app/data",
            "type": "bind",
            "read_only": True,
        },
        {
            "source": "/home/ubuntu/tripclick/ingestion/config",
            "target": "/app/config",
            "type": "bind",
            "read_only": True,
        },
    ],

    # 환경변수
    environment={
        "KAFKA_BROKERS": "{{ var.value.kafka_brokers }}",
    },

    # 명령어 인자
    command=[
        "--file", "/app/data/server0/date={{ ds }}/events.json",
        "--topic", "tripclick_raw_logs",
        "--mode", "realtime",
    ],

    # 컨테이너 설정
    auto_remove=True,
    mount_tmp_dir=False,
)

# Server1 Producer (동일 패턴)
producer_server1 = DockerOperator(
    task_id="producer_server1",
    image="tripclick-producer:latest",
    docker_url="tcp://<WEBSERVER1_IP>:2375",
    # ... (server1 경로로 변경)
)
```

### 전체 DAG 흐름

```
┌─────────────────────────────────────────────────────────────┐
│  Airflow Orchestration Server                               │
│                                                             │
│  [15:00] DAG 시작                                           │
│     │                                                       │
│     ├──▶ DockerOperator ──(tcp://server0:2375)──▶ Producer │
│     │         │                                             │
│     └──▶ DockerOperator ──(tcp://server1:2375)──▶ Producer │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 의존성

```
kafka-python>=2.0.2
PyYAML>=6.0
xxhash>=3.0.0  # 선택사항 (없으면 md5 사용)
```

---

## 로깅

- **파일**: `logs/tripclick_producer.log`
- **크기**: 최대 50MB (5개 백업)
- **콘솔**: WARNING 이상만 출력

---

## TODO

- [x] Docker 이미지 빌드 (Dockerfile 추가)
- [x] 웹서버 배포 가이드 (Remote Docker API)
- [x] Airflow DockerOperator 연동 가이드
- [ ] 멀티 서버 동시 실행 스크립트
- [ ] 에러 핸들링 강화 (재시도 로직)
- [ ] Docker TLS 인증 설정 (보안 강화)
