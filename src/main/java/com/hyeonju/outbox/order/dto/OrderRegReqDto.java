package com.hyeonju.outbox.order.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

/**
 * description    : 주문 등록 요청
 * fileName       : OrderRegReqDto
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
public record OrderRegReqDto(@NotBlank String productName, @Positive long amount) {
}
