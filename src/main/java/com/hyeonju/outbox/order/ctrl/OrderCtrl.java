package com.hyeonju.outbox.order.ctrl;

import com.hyeonju.outbox.common.dto.ResponseBaseDto;
import com.hyeonju.outbox.order.dto.OrderRegReqDto;
import com.hyeonju.outbox.order.svc.OrderSvc;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * description    : 주문 API
 * fileName       : OrderCtrl
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderCtrl {

    private final OrderSvc orderSvc;

    /**
     * 주문 등록
     */
    @PostMapping
    public ResponseBaseDto<Long> regOrder(@Valid @RequestBody OrderRegReqDto req) {
        return ResponseBaseDto.ok(orderSvc.regOrder(req));
    }
}
