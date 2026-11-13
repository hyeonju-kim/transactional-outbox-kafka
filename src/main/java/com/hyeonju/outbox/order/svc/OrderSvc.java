package com.hyeonju.outbox.order.svc;

import com.hyeonju.outbox.order.dto.OrderRegReqDto;

/**
 * description    : 주문 서비스
 * fileName       : OrderSvc
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
public interface OrderSvc {
    Long regOrder(OrderRegReqDto req); // 주문 등록 + outbox 기록 (한 트랜잭션), 주문 ID 반환
}
