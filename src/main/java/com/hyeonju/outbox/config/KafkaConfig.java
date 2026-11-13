package com.hyeonju.outbox.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * description    : 토픽 생성, 소비 실패 시 재시도 후 DLT 로 격리하는 에러 핸들러
 * fileName       : KafkaConfig
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
@Configuration
@EnableConfigurationProperties(OutboxProperties.class)
public class KafkaConfig {

    public static final String DLT_SUFFIX = ".DLT";

    @Bean
    public NewTopic orderEventsTopic(OutboxProperties props) {
        return TopicBuilder.name(props.topic()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic orderEventsDltTopic(OutboxProperties props) {
        return TopicBuilder.name(props.topic() + DLT_SUFFIX).partitions(3).replicas(1).build();
    }

    /**
     * 소비 실패 시 0.5초 간격으로 2번 더 시도하고, 그래도 실패하면 {topic}.DLT 로 보낸 뒤 오프셋을 넘긴다.
     * (독성 메시지 하나 때문에 파티션 전체가 멈추지 않게)
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 2));
        handler.setCommitRecovered(true); // 수동 커밋 모드에서도 DLT 로 넘긴 메시지의 오프셋을 커밋
        return handler;
    }
}
