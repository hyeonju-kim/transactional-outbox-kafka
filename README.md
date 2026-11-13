# Transactional Outbox + Kafka — DB 커밋과 메시지 발행의 정합성

> 주문을 저장하고 Kafka로 이벤트를 보낼 때 생기는 **유실·중복·순서 꼬임**을 Transactional Outbox와 멱등 소비로 해결하고,
> **Kafka 장애·중복 전달·동시 실행·독성 메시지 상황을 테스트로 재현해 증명**한 예제입니다.

![Java](https://img.shields.io/badge/Java_21-007396?style=flat&logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring_Boot_3.3-6DB33F?style=flat&logo=springboot&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL_16-4169E1?style=flat&logo=postgresql&logoColor=white)
![MyBatis](https://img.shields.io/badge/MyBatis-000000?style=flat)
![Testcontainers](https://img.shields.io/badge/Testcontainers-2496ED?style=flat&logo=docker&logoColor=white)

실무에서 서로 다른 DB를 쓰는 두 서비스를 Kafka로 연결하며 같은 문제를 겪었고(운영 이벤트 558건 100% 발송, 유실 0건), 그 구조를 개인 코드로 다시 구현했습니다.
과정 정리: [블로그 — Transactional Outbox 패턴: DB 트랜잭션과 Kafka 발행 정합성](https://thenicole.tistory.com/40)

---

## 문제

"주문 저장 → Kafka 발행"을 각각 따로 하면 둘 중 하나만 성공하는 경우가 생깁니다.

| 순서 | 실패 지점 | 결과 |
|---|---|---|
| DB 커밋 → Kafka 발행 | 커밋 후 발행 실패 (Kafka 장애·네트워크) | 주문은 있는데 **결제 이벤트 유실** |
| Kafka 발행 → DB 커밋 | 발행 후 커밋 실패 (롤백) | 주문은 없는데 **결제가 진행됨** |
| 발행 재시도 | 브로커는 받았는데 응답만 못 받음 | 같은 이벤트 **중복 전달** |

## 해결 구조

```
 [주문 API] ── 하나의 DB 트랜잭션 ──┐
                                   ├─ orders 저장
                                   └─ outbox_event 저장 (PENDING)
        │ 커밋 성공 후에만 (@TransactionalEventListener AFTER_COMMIT)
        ▼
 ① 즉시 발행 (비동기) ──성공──▶ SENT
        │ 실패해도 PENDING 으로 남음
        ▼
 ② 스위퍼 (주기 실행, SELECT … FOR UPDATE SKIP LOCKED) ──성공──▶ SENT
        │ 실패 횟수 한도 초과 → DEAD 로 격리
        ▼
 [Kafka: order-events] ──▶ [결제 소비자]
                              ├─ processed_event 에 eventId INSERT … ON CONFLICT DO NOTHING
                              │    └ 0건이면 이미 처리한 이벤트 → 건너뜀 (멱등)
                              ├─ payment 저장 (위와 같은 트랜잭션)
                              ├─ DB 처리 후에만 오프셋 수동 커밋
                              └─ 처리 실패 → 재시도 2회 → order-events.DLT 로 격리
```

### 설계 판단

| 결정 | 이유 |
|---|---|
| 주문과 이벤트를 **같은 트랜잭션**에 저장 | "둘 중 하나만 성공"하는 경우를 DB 트랜잭션으로 없앤다 |
| 발행은 **커밋 이후에만** | 롤백된 주문의 이벤트가 나가지 않게 |
| 즉시 발행 + 스위퍼 **이중 경로** | 평소엔 지연 없이 바로 나가고, 장애 때 남은 건은 스위퍼가 책임진다 |
| 즉시 발행은 **결과를 기다리지 않음** | Kafka 장애가 주문 API 응답 지연으로 번지지 않게 |
| 스위퍼는 `FOR UPDATE SKIP LOCKED` | 인스턴스를 여러 대 띄워도 같은 이벤트를 두 스위퍼가 잡지 않게 |
| 스위퍼는 **일정 시간 지난 건만** | 막 커밋돼 즉시 발행 중인 이벤트와 겹치지 않게 |
| 스위퍼는 **첫 실패에서 배치 중단** | 장애 중에 같은 실패를 수십 번 반복하지 않게 |
| 소비 측 **멱등 처리** | Outbox는 "최소 한 번" 발행이라 중복은 피할 수 없다. 받는 쪽에서 거른다 |
| 처리 이력 + 결제를 **같은 트랜잭션** | 결제 중 실패하면 처리 이력도 롤백 → 재시도 시 다시 처리 |
| 실패 메시지는 **DLT 로 격리** | 독성 메시지 하나 때문에 파티션 전체가 멈추지 않게 |
| 메시지 키 = **주문 ID** | 같은 주문의 이벤트는 같은 파티션으로 → 주문 단위 순서 보장 |

### 다른 선택지와 비교

| 방식 | 판단 |
|---|---|
| 2PC (분산 트랜잭션) | Kafka가 XA를 지원하지 않고, 참여자 장애 시 전체가 묶인다 |
| CDC (Debezium) | 발행 지연·순서가 더 좋지만 Kafka Connect·Debezium 운영 부담이 생긴다. 규모가 커지면 outbox 테이블은 그대로 두고 발행기만 CDC로 바꿀 수 있다 |
| Polling Outbox (이 예제) | 추가 인프라 없이 애플리케이션만으로 정합성 확보. 대신 스위퍼 주기만큼 지연이 생길 수 있다 |

---

## 테스트로 증명한 것

실제 PostgreSQL·Kafka 컨테이너(Testcontainers)를 띄워 장애 상황을 직접 만든 통합 테스트입니다.
→ [`OutboxIntegrationTest`](src/test/java/com/hyeonju/outbox/OutboxIntegrationTest.java)

| 상황 | 재현 방법 | 검증 결과 |
|---|---|---|
| 정상 | 주문 50건 | outbox 50건 SENT, 결제 50건 (유실·중복 0) |
| **Kafka 장애** | Kafka 컨테이너를 일시 정지한 채 주문 10건 → 재개 | 장애 중에도 주문은 성공·PENDING 유지 → 복구 후 스위퍼가 전부 발행, 결제 정확히 10건 |
| **중복 전달** | 같은 이벤트를 3번 발행 | 결제 1건 |
| **동시 스위퍼** | 스위퍼 4개를 동시에 실행 (이벤트 40건) | 토픽을 처음부터 읽어 확인: 이벤트마다 정확히 1번 발행 |
| **독성 메시지** | 처리할 수 없는 메시지(금액 -1) 발행 | 재시도 후 DLT 로 격리, 뒤따른 정상 주문은 계속 처리 |

```
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
```

### 테스트 중 실제로 재현된 중복

Kafka 장애 테스트에서, 장애 중 보낸 메시지 일부가 클라이언트에서는 "실패(타임아웃)"로 처리됐지만 실제로는 브로커에 도착해 있었습니다.
스위퍼가 같은 이벤트를 다시 보내면서 복구 직후 **같은 이벤트가 두 번 도착**했고, 소비 측 멱등 처리가 이를 걸러 결제는 정확히 10건만 생겼습니다.

```
INFO  PaymentSvcImpl     : 이미 처리한 이벤트, 건너뜀: eventId=6b5146ac-...
INFO  PaymentSvcImpl     : 이미 처리한 이벤트, 건너뜀: eventId=1b2660b8-...
INFO  OutboxRelaySvcImpl : 스위퍼 재발행 10건
```

"발행 쪽에서 정확히 한 번"은 보장할 수 없고, **발행은 최소 한 번 + 소비는 멱등**으로 짝을 맞춰야 한다는 걸 보여 주는 장면입니다.

---

## 실행

```bash
# 테스트 (Docker 필요, 컨테이너는 Testcontainers가 자동 실행)
mvn test

# 로컬 실행
docker compose up -d
mvn spring-boot:run
curl -X POST localhost:8080/orders -H "Content-Type: application/json" -d '{"productName":"keyboard","amount":50000}'
```

## 구조

```
src/main/java/com/hyeonju/outbox
├── order/      # 주문 API · 주문+outbox 저장 (ctrl / svc / dao / dto)
├── outbox/     # 커밋 후 즉시 발행, 스위퍼, outbox DAO
├── payment/    # 소비자 · 멱등 결제 처리
├── config/     # 토픽, 재시도·DLT 에러 핸들러, 설정값
└── common/     # 공통 응답, UUID 타입 핸들러
src/main/resources
├── schema.sql  # orders / outbox_event / processed_event / payment
└── mapper/     # MyBatis SQL
```
