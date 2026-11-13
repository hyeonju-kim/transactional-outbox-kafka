package com.hyeonju.outbox.outbox.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * description    : outbox_event 행
 * fileName       : OutboxEventDto
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Getter
@Setter
@NoArgsConstructor
public class OutboxEventDto {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_SENT = "SENT";
    public static final String STATUS_DEAD = "DEAD";

    private UUID eventId;
    private Long aggregateId;
    private String eventType;
    private String payload;
    private String status;
    private int retryCount;
    private String lastError;
    private LocalDateTime createdAt;
    private LocalDateTime sentAt;

    public static OutboxEventDto pending(Long aggregateId, String eventType, String payload) {
        OutboxEventDto e = new OutboxEventDto();
        e.eventId = UUID.randomUUID();
        e.aggregateId = aggregateId;
        e.eventType = eventType;
        e.payload = payload;
        e.status = STATUS_PENDING;
        return e;
    }
}
