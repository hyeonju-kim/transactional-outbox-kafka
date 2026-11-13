package com.hyeonju.outbox.payment.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.UUID;

/**
 * description    : 소비 측 DAO (처리 이력 + 결제)
 * fileName       : PaymentDao
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Mapper
public interface PaymentDao {
    int regProcessedEvent(@Param("eventId") UUID eventId); // 처리 이력 등록, 이미 있으면 0 반환 (ON CONFLICT DO NOTHING)
    void regPayment(@Param("orderId") Long orderId, @Param("amount") long amount); // 결제 등록
}
