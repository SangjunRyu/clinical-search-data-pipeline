# Messaging Layer

Kafka 기반 메시징 인프라 구성

## 개요

| 항목 | 내용 |
|------|------|
| 플랫폼 | Apache Kafka (Confluent) |
| 버전 | 7.5.1 |
| 브로커 수 | 3개 |
| Zookeeper | 1개 (단일) |
| 모니터링 | Kafka UI |

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────────────────┐
│                       MESSAGING LAYER                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────┐                                                    │
│  │  Zookeeper  │◄──── 클러스터 메타데이터 관리                       │
│  │  :2181      │                                                    │
│  └──────┬──────┘                                                    │
│         │                                                            │
│         ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    Kafka Cluster                             │    │
│  │  ┌───────────┐   ┌───────────┐   ┌───────────┐              │    │
│  │  │ Broker 1  │   │ Broker 2  │   │ Broker 3  │              │    │
│  │  │ :9092     │   │ :9093     │   │ :9094     │              │    │
│  │  │ (2GB)     │   │ (2GB)     │   │ (2GB)     │              │    │
│  │  └───────────┘   └───────────┘   └───────────┘              │    │
│  └─────────────────────────────────────────────────────────────┘    │
│         │                                                            │
│         ▼                                                            │
│  ┌─────────────┐                                                    │
│  │  Kafka UI   │◄──── 웹 기반 모니터링                              │
│  │  :8080      │                                                    │
│  └─────────────┘                                                    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 클러스터 스펙

### Zookeeper

| 항목 | 값 | 설명 |
|------|-----|------|
| 이미지 | `confluentinc/cp-zookeeper:7.5.1` | Confluent 공식 이미지 |
| 포트 | 2181 | 클라이언트 연결 포트 |
| 메모리 | 1GB | 메타데이터 관리용으로 충분 |
| Tick Time | 2000ms | 하트비트 간격 |

**단일 인스턴스 사용 이유:**
- 개발/테스트 환경에서는 단일 Zookeeper로 충분
- Zookeeper 앙상블(3대 이상)은 프로덕션에서 고가용성 필요 시 구성
- 리소스 절약 (t3.medium 서버 기준)

### Kafka Brokers

| 항목 | Broker 1 | Broker 2 | Broker 3 |
|------|----------|----------|----------|
| Broker ID | 1 | 2 | 3 |
| External Port | 9092 | 9093 | 9094 |
| Internal Port | 29092 | 29092 | 29092 |
| 메모리 제한 | 2GB | 2GB | 2GB |

**3개 브로커 구성 이유:**

1. **고가용성 (High Availability)**
   - 1대 브로커 장애 시에도 서비스 지속 가능
   - Replication Factor 2 설정으로 데이터 이중화

2. **파티션 분산**
   - 토픽 파티션이 3개 브로커에 균등 분산
   - 부하 분산 및 처리량 향상

3. **최소 권장 구성**
   - Kafka 공식 권장: 프로덕션 최소 3대
   - 2대는 split-brain 문제 발생 가능

### 메모리 할당 근거

| 컴포넌트 | 메모리 | 근거 |
|----------|--------|------|
| Zookeeper | 1GB | 메타데이터만 관리, 1GB 충분 |
| Kafka Broker | 2GB | 페이지 캐시 + JVM 힙 고려 |
| **총합** | **7GB** | t3.medium(4GB) 2대 또는 t3.large(8GB) 1대 |

**Kafka 메모리 2GB 설정 이유:**
- JVM Heap: ~1GB (기본값)
- OS Page Cache: ~1GB (디스크 I/O 성능)
- TripClick 데이터 규모(일 수십만 건)에 적합

---

## 리스너 구성

### Dual Listener 패턴

```
┌──────────────────────────────────────────────────────────────┐
│                        Kafka Broker                           │
│  ┌────────────────────┐    ┌────────────────────┐            │
│  │  INTERNAL Listener │    │  EXTERNAL Listener │            │
│  │  kafka-N:29092     │    │  0.0.0.0:909X      │            │
│  └─────────┬──────────┘    └─────────┬──────────┘            │
│            │                         │                        │
└────────────┼─────────────────────────┼────────────────────────┘
             │                         │
             ▼                         ▼
    ┌────────────────┐        ┌────────────────┐
    │ Docker 내부    │        │ Docker 외부    │
    │ (브로커 간)    │        │ (Producer/     │
    │                │        │  Consumer)     │
    └────────────────┘        └────────────────┘
```

### 리스너 설정 상세

```yaml
KAFKA_LISTENERS: INTERNAL://kafka-1:29092,EXTERNAL://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:29092,EXTERNAL://${KAFKA_HOST:-localhost}:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```

| 설정 | 값 | 설명 |
|------|-----|------|
| `INTERNAL` | `kafka-N:29092` | Docker 네트워크 내부 통신 (브로커 간) |
| `EXTERNAL` | `0.0.0.0:909X` | Docker 외부 클라이언트 접속 |
| `KAFKA_HOST` | 환경변수 | 외부 접속 시 advertise할 호스트 |

**Dual Listener 사용 이유:**
- Docker 컨테이너 내부와 외부의 네트워크 분리
- 브로커 간 통신은 내부 네트워크 사용 (낮은 레이턴시)
- 외부 Producer/Consumer는 호스트 포트로 접속

---

## Replication 설정

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 2
```

| 설정 | 값 | 설명 |
|------|-----|------|
| Replication Factor | 2 | 각 파티션 2개 복제본 |
| Min ISR | 1 (기본값) | 최소 동기화 복제본 수 |

**Replication Factor 2 선택 이유:**

1. **데이터 안정성**
   - 1대 브로커 장애 시 데이터 손실 없음
   - Factor 3은 리소스 대비 과도함 (개발/테스트 환경)

2. **쓰기 성능**
   - Factor 3 대비 쓰기 오버헤드 감소
   - acks=all 설정 시 2대만 확인하면 됨

3. **저장 용량**
   - 데이터 2배 저장 (3배 대비 33% 절약)

---

## 토픽 설계

### tripclick-logs 토픽

```bash
# 토픽 생성 (권장)
kafka-topics.sh --create \
  --bootstrap-server kafka-1:29092 \
  --topic tripclick-logs \
  --partitions 3 \
  --replication-factor 2
```

| 설정 | 값 | 근거 |
|------|-----|------|
| Partitions | 3 | 브로커 수와 동일, 균등 분산 |
| Replication | 2 | 1대 장애 허용 |
| Retention | 7일 (기본) | 배치 재처리 여유 |

**파티션 3개 선택 이유:**
- Consumer 병렬 처리 가능 (최대 3개 Consumer)
- 브로커 수와 일치시켜 균등 분산
- TripClick 데이터 규모에 적합

---

## Kafka UI

| 항목 | 값 |
|------|-----|
| 이미지 | `provectuslabs/kafka-ui:latest` |
| 포트 | 8080 |
| 접속 | http://localhost:8080 |

### 주요 기능

- 토픽 목록 및 메시지 조회
- Consumer Group 모니터링
- 브로커 상태 확인
- 토픽 생성/삭제

---

## 환경별 설정

### 개발 환경 (로컬)

```bash
# 기본 설정으로 실행
docker-compose -f messaging/kafka-compose.yaml up -d
```

### 운영 환경 (EC2)

```bash
# KAFKA_HOST를 EC2 Private IP로 설정
KAFKA_HOST=10.0.1.100 docker-compose -f messaging/kafka-compose.yaml up -d
```

| 환경 | KAFKA_HOST | 용도 |
|------|------------|------|
| 로컬 | localhost | 개발/테스트 |
| EC2 | Private IP | 같은 VPC 내 접속 |
| EC2 (외부) | Public IP | VPC 외부 접속 (비권장) |

---

## 성능 튜닝 포인트

### Producer 설정 (권장)

```python
producer_config = {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9093,kafka-3:9094',
    'acks': 'all',           # 모든 복제본 확인 (안정성)
    'linger.ms': 5,          # 배치 대기 시간
    'batch.size': 16384,     # 배치 크기 (16KB)
    'compression.type': 'lz4', # 압축 (네트워크 절약)
}
```

### Consumer 설정 (권장)

```python
consumer_config = {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9093,kafka-3:9094',
    'group.id': 'tripclick-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # 수동 커밋 (정확성)
}
```

---

## 트러블슈팅

### 1. 브로커 연결 실패

```bash
# 브로커 상태 확인
docker logs kafka-1

# Zookeeper 연결 확인
docker exec kafka-1 kafka-broker-api-versions --bootstrap-server kafka-1:29092
```

### 2. 리더 선출 실패

```bash
# 토픽 상태 확인
docker exec kafka-1 kafka-topics --describe --topic tripclick-logs --bootstrap-server kafka-1:29092
```

### 3. Consumer Lag 확인

```bash
# Consumer Group Lag 조회
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --describe --group tripclick-consumer
```

---

## 실행 방법

```bash
cd messaging

# 클러스터 시작
docker-compose -f kafka-compose.yaml up -d

# 상태 확인
docker-compose -f kafka-compose.yaml ps

# 로그 확인
docker-compose -f kafka-compose.yaml logs -f kafka-1

# 클러스터 종료
docker-compose -f kafka-compose.yaml down
```

---

## 스펙 요약

| 구성 요소 | 수량 | 메모리 | 포트 |
|-----------|------|--------|------|
| Zookeeper | 1 | 1GB | 2181 |
| Kafka Broker | 3 | 2GB x 3 | 9092-9094 |
| Kafka UI | 1 | - | 8080 |
| **Total** | **5** | **7GB** | - |

**권장 서버 스펙:**
- CPU: 2 vCPU 이상
- Memory: 8GB 이상 (t3.large)
- Storage: 50GB SSD (로그 보관용)
