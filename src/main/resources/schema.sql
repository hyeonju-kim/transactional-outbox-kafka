-- 주문
CREATE TABLE IF NOT EXISTS orders (
    order_id     BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    amount       BIGINT       NOT NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT now()
);

-- Outbox: 주문과 같은 트랜잭션으로 기록되는 발행 대기 이벤트
CREATE TABLE IF NOT EXISTS outbox_event (
    event_id     UUID         PRIMARY KEY,
    aggregate_id BIGINT       NOT NULL,
    event_type   VARCHAR(50)  NOT NULL,
    payload      TEXT         NOT NULL,
    status       VARCHAR(10)  NOT NULL DEFAULT 'PENDING',  -- PENDING / SENT / DEAD
    retry_count  INT          NOT NULL DEFAULT 0,
    last_error   VARCHAR(500),
    created_at   TIMESTAMP    NOT NULL DEFAULT now(),
    sent_at      TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_event (status, created_at);

-- 소비 측: 이미 처리한 이벤트 기록 (멱등 처리 키)
CREATE TABLE IF NOT EXISTS processed_event (
    event_id     UUID         PRIMARY KEY,
    processed_at TIMESTAMP    NOT NULL DEFAULT now()
);

-- 소비 측 결과: 주문당 결제 1건
CREATE TABLE IF NOT EXISTS payment (
    payment_id   BIGSERIAL    PRIMARY KEY,
    order_id     BIGINT       NOT NULL UNIQUE,
    amount       BIGINT       NOT NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT now()
);
