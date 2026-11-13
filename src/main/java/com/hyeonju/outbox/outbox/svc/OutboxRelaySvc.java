package com.hyeonju.outbox.outbox.svc;

import java.util.UUID;

/**
 * description    : outbox 이벤트를 Kafka 로 발행 (즉시 발행 + 스위퍼 재시도)
 * fileName       : OutboxRelaySvc
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
public interface OutboxRelaySvc {
    void relayImmediately(UUID eventId); // 커밋 직후 비동기 즉시 발행 (실패해도 스위퍼가 재시도)
    int sweepPendingEvents(); // 발행 대기 건 재시도, 발행 성공 건수 반환
}
