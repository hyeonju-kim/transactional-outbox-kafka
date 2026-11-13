package com.hyeonju.outbox.payment.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyeonju.outbox.outbox.dto.OrderCreatedPayload;
import com.hyeonju.outbox.payment.svc.PaymentSvc;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * description    : ORDER_CREATED 소비자.
 *                  DB 처리가 끝난 뒤에만 오프셋을 커밋한다 (처리 전에 커밋하면 장애 시 메시지가 사라짐).
 *                  처리 실패는 KafkaConfig 의 에러 핸들러가 재시도 후 DLT 로 보낸다.
 * fileName       : PaymentConsumer
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Component
@RequiredArgsConstructor
public class PaymentConsumer {

    private final PaymentSvc paymentSvc;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${outbox.topic}", groupId = "payment-service")
    public void onOrderCreated(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        OrderCreatedPayload payload = objectMapper.readValue(record.value(), OrderCreatedPayload.class);
        paymentSvc.regPaymentOnce(payload);
        ack.acknowledge();
    }
}
