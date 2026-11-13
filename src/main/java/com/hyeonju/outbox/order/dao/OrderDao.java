package com.hyeonju.outbox.order.dao;

import com.hyeonju.outbox.order.dto.OrderDto;
import org.apache.ibatis.annotations.Mapper;

/**
 * description    : 주문 DAO
 * fileName       : OrderDao
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Mapper
public interface OrderDao {
    void regOrder(OrderDto order); // 주문 등록 (orderId 채움)
}
