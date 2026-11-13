package com.hyeonju.outbox.outbox.dto;

import java.util.UUID;

/**
 * description    : ORDER_CREATED 이벤트 본문 (eventId 를 소비 측 멱등 키로 사용)
 * fileName       : OrderCreatedPayload
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
public record OrderCreatedPayload(UUID eventId, Long orderId, String productName, long amount) {

    public static final String EVENT_TYPE = "ORDER_CREATED";
}
