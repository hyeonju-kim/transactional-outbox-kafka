package com.hyeonju.outbox.order.svc.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyeonju.outbox.order.dao.OrderDao;
import com.hyeonju.outbox.order.dto.OrderDto;
import com.hyeonju.outbox.order.dto.OrderRegReqDto;
import com.hyeonju.outbox.order.svc.OrderSvc;
import com.hyeonju.outbox.outbox.dao.OutboxDao;
import com.hyeonju.outbox.outbox.dto.OrderCreatedPayload;
import com.hyeonju.outbox.outbox.dto.OutboxEventDto;
import com.hyeonju.outbox.outbox.event.OutboxSavedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * description    : 주문 서비스 구현
 * fileName       : OrderSvcImpl
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Service
@RequiredArgsConstructor
public class OrderSvcImpl implements OrderSvc {

    private final OrderDao orderDao;
    private final OutboxDao outboxDao;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;

    /**
     * Author: hj kim
     * Description: 주문과 outbox 이벤트를 같은 트랜잭션으로 저장한다.
     *              Kafka 발행은 여기서 하지 않는다. 커밋이 확정된 뒤 리스너가 발행하므로
     *              "주문은 저장됐는데 이벤트가 없음" / "롤백됐는데 이벤트만 나감" 두 경우가 모두 사라진다.
     * Date: 2026-11-13
     */
    @Override
    @Transactional
    public Long regOrder(OrderRegReqDto req) {
        OrderDto order = new OrderDto(req.productName(), req.amount());
        orderDao.regOrder(order);

        UUID eventId = UUID.randomUUID();
        OrderCreatedPayload payload = new OrderCreatedPayload(eventId, order.getOrderId(), order.getProductName(), order.getAmount());
        OutboxEventDto event = OutboxEventDto.pending(order.getOrderId(), OrderCreatedPayload.EVENT_TYPE, toJson(payload));
        event.setEventId(eventId);
        outboxDao.regOutboxEvent(event);

        eventPublisher.publishEvent(new OutboxSavedEvent(eventId));
        return order.getOrderId();
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("이벤트 직렬화 실패", e);
        }
    }
}
