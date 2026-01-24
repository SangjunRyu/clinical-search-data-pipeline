### 수정사항 존재.

docker run --rm \
  -v $(pwd)/data:/app/data:ro \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/logs:/app/logs \
  -v /home/ubuntu/server0:/app/data/server0:ro \
  tripclick-producer:latest \
  --file /app/data/server0/date=2026-01-02/events.json \
  --mode batch

로 실행할 수 있도록 수정하였음. 

Dockerfile에서 data를 마운트 하는 부분제거함
COPY config/config.yaml ./config/config.yaml 이 부분 놔두어서 kafka 내용은 옵션으로 주지 않아도 자동으로
3개의 브로커에 분산분배되도록 구성하였음.

producer 로직을 base, batch, realtime으로 나누고,
그냥 --mode batch로 실행, 딱히 entrypoint.sh은 사용하지 않을 예정.
이에 맞게 airflow dags도 --mode 옵션에 batch를 주냐, realtime을 주냐로 수정할 예정임.

ingestion 서버의 docker-compose.yaml에 대한 내용 설명좀 해주고 필요한건지도 검토예정.

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
├── entrypoint.sh            # Docker 엔트리포인트 (모드 선택)
├── requirements.txt         # Python 의존성
├── producer/
│   ├── producer_base.py     # 공통 유틸리티 (config, dedup, kafka)
│   ├── producer_realtime.py # 실시간 스트리밍 Producer
│   ├── producer_batch.py    # 배치 전송 Producer
│   └── producer.py          # (deprecated) 기존 통합 코드
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

## Producer 구조

### 파일 구성

| 파일 | 설명 |
|------|------|
| `producer_base.py` | 공통 유틸리티 (config loader, dedup key, kafka producer) |
| `producer_realtime.py` | 실시간 스트리밍 전송 |
| `producer_batch.py` | 배치 전송 (고속 처리) |

### 전송 모드

| 모드 | 파일 | 설명 | 사용 시점 |
|------|------|------|----------|
| `realtime` | `producer_realtime.py` | event_ts 기준 시간 경과에 따라 전송 | 스트리밍 시뮬레이션 |
| `batch` | `producer_batch.py` | 대기 없이 즉시 전송 | 초기 적재, 백필 |

### 실행 방법

```bash
cd ingestion

# 실시간 모드
python3 -m producer.producer_realtime \
  --file data/server0/date=2026-01-01/events.json \
  --topic tripclick_raw_logs

# 배치 모드
python3 -m producer.producer_batch \
  --file data/server1/date=2026-01-01/events.json \
  --topic tripclick_raw_logs
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
  realtime \
  --file /app/data/server0/date=2026-01-01/events.json \
  --topic tripclick_raw_logs

# 실행 (batch 모드)
docker run --rm \
  --network tripclick-network \
  -v $(pwd)/data:/app/data:ro \
  -v $(pwd)/config:/app/config:ro \
  -e KAFKA_BROKERS=$KAFKA_BROKERS \
  tripclick-producer:latest \
  batch \
  --file /app/data/server0/date=2026-01-01/events.json \
  --topic tripclick_raw_logs

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

### DAG 구성

Airflow DAG는 두 가지로 분리되어 있습니다:

| DAG ID | 파일 | 모드 | 용도 |
|--------|------|------|------|
| `tripclick_producer_realtime` | `tripclick_producer_realtime_dag.py` | realtime | 스트리밍 시뮬레이션 |
| `tripclick_producer_batch` | `tripclick_producer_batch_dag.py` | batch | 백필/일괄 적재 |

### DAG에서 DockerOperator 사용

```python
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Batch 모드 Producer (과거 데이터 백필)
producer_server0_batch = DockerOperator(
    task_id="producer_server0_batch",
    image="tripclick-producer:latest",
    docker_conn_id="docker_server0",
    network_mode="host",

    # 볼륨 마운트 (호스트 경로 → 컨테이너 경로)
    mounts=[
        Mount(
            source="/home/ubuntu/server0",
            target="/app/data",
            type="bind",
            read_only=True,
        ),
        Mount(
            source="/home/ubuntu/ingestion/config",
            target="/app/config",
            type="bind",
            read_only=True,
        ),
    ],

    # 환경변수
    environment={
        "KAFKA_BROKERS": "{{ var.value.KAFKA_BROKERS }}",
    },

    # 명령어 인자 (batch 모드)
    command=[
        "--file", "/app/data/date={{ ds }}/events.json",
        "--topic", "tripclick_raw_logs",
        "--mode", "batch",
    ],

    # 컨테이너 설정
    auto_remove="success",
    mount_tmp_dir=False,
)

# Realtime 모드 Producer (스트리밍 테스트)
producer_server0_realtime = DockerOperator(
    task_id="producer_server0_realtime",
    # ... 동일, --mode realtime으로 변경
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
