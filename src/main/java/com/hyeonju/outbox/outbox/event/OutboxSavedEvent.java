package com.hyeonju.outbox.outbox.event;

import java.util.UUID;

/**
 * description    : outbox 기록 완료 알림 (트랜잭션 커밋 후 즉시 발행 트리거)
 * fileName       : OutboxSavedEvent
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
public record OutboxSavedEvent(UUID eventId) {
}
