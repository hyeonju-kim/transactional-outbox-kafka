package com.hyeonju.outbox.outbox.svc.impl;

import com.hyeonju.outbox.config.OutboxProperties;
import com.hyeonju.outbox.outbox.dao.OutboxDao;
import com.hyeonju.outbox.outbox.dto.OutboxEventDto;
import com.hyeonju.outbox.outbox.svc.OutboxRelaySvc;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * description    : outbox 발행기 구현
 * fileName       : OutboxRelaySvcImpl
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxRelaySvcImpl implements OutboxRelaySvc {

    public static final String HEADER_EVENT_ID = "eventId";

    private final OutboxDao outboxDao;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxProperties props;

    /**
     * Author: hj kim
     * Description: 커밋 직후 즉시 발행. 응답 지연을 막기 위해 결과를 기다리지 않고 콜백으로 상태만 갱신한다.
     *              Kafka 가 죽어 있으면 PENDING 으로 남고, 스위퍼가 이어서 처리한다.
     * Date: 2026-11-13
     */
    @Override
    public void relayImmediately(UUID eventId) {
        OutboxEventDto event = outboxDao.getOutboxEvent(eventId);
        if (event == null || !OutboxEventDto.STATUS_PENDING.equals(event.getStatus())) {
            return;
        }
        try {
            kafkaTemplate.send(toRecord(event)).whenComplete((result, ex) -> {
                if (ex == null) {
                    outboxDao.modOutboxSent(eventId);
                } else {
                    log.warn("즉시 발행 실패, 스위퍼가 재시도 예정: eventId={}, cause={}", eventId, ex.getMessage());
                }
            });
        } catch (Exception e) {
            // 메타데이터 조회 대기(max.block.ms) 초과 등 send() 자체가 실패한 경우도 PENDING 으로 남긴다
            log.warn("즉시 발행 요청 실패, 스위퍼가 재시도 예정: eventId={}, cause={}", eventId, e.getMessage());
        }
    }

    /**
     * Author: hj kim
     * Description: 발행 대기 건을 잠금 조회(SKIP LOCKED)해 동기 발행한다.
     *              - 여러 인스턴스가 동시에 돌아도 같은 행을 두 번 잡지 않는다.
     *              - 한 건이라도 실패하면 Kafka 장애로 보고 이번 배치를 멈춘다 (장애 중 같은 실패를 반복하지 않게).
     *              - 실패 횟수가 한도를 넘으면 DEAD 로 격리한다.
     * Date: 2026-11-13
     */
    @Override
    @Transactional
    public int sweepPendingEvents() {
        OutboxProperties.Relay relay = props.relay();
        List<OutboxEventDto> events = outboxDao.getPendingEventListForUpdate(relay.minAgeMs(), relay.batchSize());
        int sent = 0;
        for (OutboxEventDto event : events) {
            try {
                kafkaTemplate.send(toRecord(event)).get(relay.sendTimeoutMs(), TimeUnit.MILLISECONDS);
                outboxDao.modOutboxSent(event.getEventId());
                sent++;
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                outboxDao.modOutboxRetry(event.getEventId(), String.valueOf(e.getMessage()), relay.maxRetry());
                log.warn("스위퍼 발행 실패, 이번 배치 중단: eventId={}, retry={}, cause={}",
                        event.getEventId(), event.getRetryCount() + 1, e.getMessage());
                break;
            }
        }
        if (sent > 0) {
            log.info("스위퍼 재발행 {}건", sent);
        }
        return sent;
    }

    // 주문 단위 순서를 지키기 위해 aggregateId(주문 ID)를 키로, eventId 는 헤더에도 싣는다
    private ProducerRecord<String, String> toRecord(OutboxEventDto event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                props.topic(), String.valueOf(event.getAggregateId()), event.getPayload());
        record.headers().add(HEADER_EVENT_ID, event.getEventId().toString().getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
