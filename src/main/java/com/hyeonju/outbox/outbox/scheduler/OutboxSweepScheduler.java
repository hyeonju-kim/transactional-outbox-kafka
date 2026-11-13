package com.hyeonju.outbox.outbox.scheduler;

import com.hyeonju.outbox.outbox.svc.OutboxRelaySvc;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * description    : 즉시 발행에 실패해 남은 PENDING 이벤트를 주기적으로 재발행
 * fileName       : OutboxSweepScheduler
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxSweepScheduler {

    private final OutboxRelaySvc outboxRelaySvc;

    @Scheduled(fixedDelayString = "${outbox.relay.sweep-interval-ms}")
    public void sweep() {
        try {
            outboxRelaySvc.sweepPendingEvents();
        } catch (Exception e) {
            log.warn("스위퍼 실행 실패: {}", e.getMessage());
        }
    }
}
