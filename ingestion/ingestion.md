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
python producer/producer.py \
  --file data/server0/date=2026-01-01/events.json \
  --topic tripclick_raw_logs \
  --mode realtime

# 배치 모드
python producer/producer.py \
  --file data/server0/date=2026-01-01/events.json \
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

## 외부 Airflow 연동

외부 Airflow 노드에서 DockerOperator를 통해 실행 예정:

```python
DockerOperator(
    task_id="run_producer_server0",
    image="tripclick-producer:latest",
    command="python producer/producer.py --file /data/server0/date={{ ds }}/events.json",
    docker_url="tcp://webserver0:2375",  # Remote Docker API
    mount_tmp_dir=False,
    ...
)
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

- [ ] Docker 이미지 빌드 (Dockerfile 추가)
- [ ] 멀티 서버 동시 실행 스크립트
- [ ] 에러 핸들링 강화 (재시도 로직)
