package com.hyeonju.outbox.order.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * description    : 주문 (MyBatis 매핑용, orderId 는 INSERT 후 채워짐)
 * fileName       : OrderDto
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Getter
@Setter
@NoArgsConstructor
public class OrderDto {

    private Long orderId;
    private String productName;
    private long amount;

    public OrderDto(String productName, long amount) {
        this.productName = productName;
        this.amount = amount;
    }
}
