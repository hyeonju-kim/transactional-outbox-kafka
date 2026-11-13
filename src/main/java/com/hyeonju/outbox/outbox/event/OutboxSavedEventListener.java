package com.hyeonju.outbox.outbox.event;

import com.hyeonju.outbox.outbox.svc.OutboxRelaySvc;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * description    : 주문 트랜잭션이 "커밋된 뒤에만" 즉시 발행을 시도한다.
 *                  커밋 전에 발행하면 롤백된 주문의 이벤트가 나갈 수 있기 때문.
 * fileName       : OutboxSavedEventListener
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Component
@RequiredArgsConstructor
public class OutboxSavedEventListener {

    private final OutboxRelaySvc outboxRelaySvc;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onOutboxSaved(OutboxSavedEvent event) {
        outboxRelaySvc.relayImmediately(event.eventId());
    }
}
