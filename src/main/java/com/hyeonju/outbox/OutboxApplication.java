package com.hyeonju.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * description    : Transactional Outbox 예제 애플리케이션
 * fileName       : OutboxApplication
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@EnableScheduling
@SpringBootApplication
public class OutboxApplication {

    public static void main(String[] args) {
        SpringApplication.run(OutboxApplication.class, args);
    }
}
