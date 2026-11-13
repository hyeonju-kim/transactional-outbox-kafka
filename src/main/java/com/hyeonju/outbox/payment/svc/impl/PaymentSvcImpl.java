package com.hyeonju.outbox.payment.svc.impl;

import com.hyeonju.outbox.outbox.dto.OrderCreatedPayload;
import com.hyeonju.outbox.payment.dao.PaymentDao;
import com.hyeonju.outbox.payment.svc.PaymentSvc;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * description    : 결제 서비스 구현
 * fileName       : PaymentSvcImpl
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentSvcImpl implements PaymentSvc {

    private final PaymentDao paymentDao;

    /**
     * Author: hj kim
     * Description: Outbox 는 "최소 한 번" 발행이라 같은 이벤트가 두 번 올 수 있다.
     *              처리 이력 등록과 결제 등록을 한 트랜잭션으로 묶어,
     *              처리 이력이 이미 있으면 건너뛰고 / 결제 중 실패하면 처리 이력도 함께 롤백되게 한다.
     * Date: 2026-11-13
     */
    @Override
    @Transactional
    public boolean regPaymentOnce(OrderCreatedPayload payload) {
        if (payload.amount() <= 0) {
            throw new IllegalArgumentException("결제 금액이 올바르지 않습니다: " + payload.amount());
        }
        if (paymentDao.regProcessedEvent(payload.eventId()) == 0) {
            log.info("이미 처리한 이벤트, 건너뜀: eventId={}", payload.eventId());
            return false;
        }
        paymentDao.regPayment(payload.orderId(), payload.amount());
        return true;
    }
}
