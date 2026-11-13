package com.hyeonju.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * description    : outbox.* 설정값
 * fileName       : OutboxProperties
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@ConfigurationProperties(prefix = "outbox")
public record OutboxProperties(String topic, Relay relay) {

    /**
     * @param sweepIntervalMs 스위퍼 실행 주기
     * @param minAgeMs        이 시간보다 오래된 PENDING 만 스위퍼가 가져간다 (즉시 발행 중인 건과 겹치지 않게)
     * @param batchSize       한 번에 가져갈 건수
     * @param maxRetry        이 횟수를 넘으면 DEAD 로 격리
     * @param sendTimeoutMs   스위퍼의 동기 발행 대기 시간
     */
    public record Relay(long sweepIntervalMs, long minAgeMs, int batchSize, int maxRetry, long sendTimeoutMs) {
    }
}
