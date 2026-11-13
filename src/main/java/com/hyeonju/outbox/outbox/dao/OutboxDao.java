package com.hyeonju.outbox.outbox.dao;

import com.hyeonju.outbox.outbox.dto.OutboxEventDto;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.UUID;

/**
 * description    : outbox_event DAO
 * fileName       : OutboxDao
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Mapper
public interface OutboxDao {
    void regOutboxEvent(OutboxEventDto event); // outbox 기록
    OutboxEventDto getOutboxEvent(@Param("eventId") UUID eventId); // 단건 조회
    List<OutboxEventDto> getPendingEventListForUpdate(@Param("minAgeMs") long minAgeMs, @Param("limit") int limit); // 발행 대기 건 잠금 조회 (SKIP LOCKED)
    int modOutboxSent(@Param("eventId") UUID eventId); // 발행 완료 처리 (PENDING 일 때만)
    int modOutboxRetry(@Param("eventId") UUID eventId, @Param("error") String error, @Param("maxRetry") int maxRetry); // 실패 횟수 증가, 한도 초과 시 DEAD
}
