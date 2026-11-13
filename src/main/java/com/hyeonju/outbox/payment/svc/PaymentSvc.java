package com.hyeonju.outbox.payment.svc;

import com.hyeonju.outbox.outbox.dto.OrderCreatedPayload;

/**
 * description    : 결제 서비스 (ORDER_CREATED 소비)
 * fileName       : PaymentSvc
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
public interface PaymentSvc {
    boolean regPaymentOnce(OrderCreatedPayload payload); // 이벤트당 1번만 결제 등록, 중복이면 false
}
