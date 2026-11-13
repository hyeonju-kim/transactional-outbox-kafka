package com.hyeonju.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyeonju.outbox.config.KafkaConfig;
import com.hyeonju.outbox.order.dto.OrderRegReqDto;
import com.hyeonju.outbox.order.svc.OrderSvc;
import com.hyeonju.outbox.outbox.dao.OutboxDao;
import com.hyeonju.outbox.outbox.dto.OrderCreatedPayload;
import com.hyeonju.outbox.outbox.dto.OutboxEventDto;
import com.hyeonju.outbox.outbox.svc.OutboxRelaySvc;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * description    : Outbox 정합성 검증 (정상 / Kafka 장애 / 중복 전달 / 동시 스위퍼 / 독성 메시지)
 * fileName       : OutboxIntegrationTest
 * date           : 2026-11-13
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2026-11-13        hj kim             최초 생성
 */
class OutboxIntegrationTest extends IntegrationTestBase {

    private static final String TOPIC = "order-events";

    @Autowired OrderSvc orderSvc;
    @Autowired OutboxDao outboxDao;
    @Autowired OutboxRelaySvc outboxRelaySvc;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ObjectMapper objectMapper;

    @Test
    @DisplayName("정상: 주문 50건 → outbox 50건 모두 SENT, 결제 50건 (유실·중복 0)")
    void normalFlow() {
        for (int i = 0; i < 50; i++) {
            orderSvc.regOrder(new OrderRegReqDto("상품-" + i, 1000L + i));
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(count("SELECT count(*) FROM outbox_event WHERE status = 'SENT'")).isEqualTo(50);
            assertThat(count("SELECT count(*) FROM payment")).isEqualTo(50);
        });
        assertThat(count("SELECT count(DISTINCT order_id) FROM payment")).isEqualTo(50);
    }

    @Test
    @DisplayName("Kafka 장애: 장애 중에도 주문은 성공하고 PENDING 으로 남았다가, 복구 후 스위퍼가 전부 전달 (유실·중복 0)")
    void kafkaDownThenRecovered() {
        pauseKafka();
        try {
            for (int i = 0; i < 10; i++) {
                orderSvc.regOrder(new OrderRegReqDto("장애중-" + i, 2000L + i));
            }
            // 주문 트랜잭션은 Kafka 와 무관하게 커밋된다
            assertThat(count("SELECT count(*) FROM orders")).isEqualTo(10);
            assertThat(count("SELECT count(*) FROM outbox_event WHERE status = 'PENDING'")).isEqualTo(10);

            // 장애가 이어지는 동안 스위퍼가 돌아도 발행되지 않고 PENDING 유지, 실패 횟수만 쌓인다
            await().pollDelay(Duration.ofSeconds(8)).atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                assertThat(count("SELECT count(*) FROM outbox_event WHERE status = 'PENDING'")).isEqualTo(10);
                assertThat(count("SELECT count(*) FROM outbox_event WHERE retry_count > 0")).isGreaterThan(0);
            });
            assertThat(count("SELECT count(*) FROM payment")).isZero();
        } finally {
            unpauseKafka();
        }

        await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
            assertThat(count("SELECT count(*) FROM outbox_event WHERE status = 'SENT'")).isEqualTo(10);
            assertThat(count("SELECT count(*) FROM payment")).isEqualTo(10);
        });
        assertThat(count("SELECT count(DISTINCT order_id) FROM payment")).isEqualTo(10);
    }

    @Test
    @DisplayName("중복 전달: 같은 이벤트가 3번 와도 결제는 1건 (멱등 소비)")
    void duplicateDelivery() throws Exception {
        UUID eventId = UUID.randomUUID();
        String payload = objectMapper.writeValueAsString(new OrderCreatedPayload(eventId, 999L, "중복", 5000L));

        for (int i = 0; i < 3; i++) {
            kafkaTemplate.send(TOPIC, "999", payload).get(10, TimeUnit.SECONDS);
        }

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM processed_event WHERE event_id = ?", eventId)).isEqualTo(1));
        // 나머지 2번이 처리될 시간을 준 뒤에도 결제는 1건
        await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM payment WHERE order_id = 999")).isEqualTo(1));
    }

    @Test
    @DisplayName("동시 스위퍼: 4개 스위퍼가 동시에 돌아도 각 이벤트는 Kafka 로 정확히 1번만 발행 (SKIP LOCKED)")
    void concurrentSweepersPublishOnce() throws Exception {
        // 즉시 발행 경로를 거치지 않게 PENDING 이벤트를 직접 넣고, 스위퍼 대상이 되도록 생성 시각을 과거로
        Set<String> eventIds = new HashSet<>();
        for (int i = 0; i < 40; i++) {
            UUID eventId = UUID.randomUUID();
            long orderId = 10_000L + i;
            String payload = objectMapper.writeValueAsString(new OrderCreatedPayload(eventId, orderId, "스윕-" + i, 100L));
            OutboxEventDto e = OutboxEventDto.pending(orderId, OrderCreatedPayload.EVENT_TYPE, payload);
            e.setEventId(eventId);
            outboxDao.regOutboxEvent(e);
            eventIds.add(eventId.toString());
        }
        jdbcTemplate.update("UPDATE outbox_event SET created_at = now() - INTERVAL '1 minute'");

        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Integer>> results = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            results.add(pool.submit(() -> {
                start.await();
                int total = 0;
                for (int round = 0; round < 3; round++) {
                    total += outboxRelaySvc.sweepPendingEvents();
                }
                return total;
            }));
        }
        start.countDown();
        int sentByTestSweepers = 0;
        for (Future<Integer> f : results) {
            sentByTestSweepers += f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM outbox_event WHERE status = 'SENT'")).isEqualTo(40));

        // 토픽을 처음부터 읽어 이번 테스트의 이벤트가 몇 번 발행됐는지 센다
        Map<String, Integer> publishCount = countPublishedEvents(eventIds);
        assertThat(publishCount).hasSize(40);
        assertThat(publishCount.values()).allMatch(c -> c == 1);
        assertThat(sentByTestSweepers).isLessThanOrEqualTo(40);
    }

    @Test
    @DisplayName("독성 메시지: 처리할 수 없는 메시지는 재시도 후 DLT 로 격리되고, 뒤따르는 정상 메시지는 계속 처리")
    void poisonMessageGoesToDlt() throws Exception {
        UUID badEventId = UUID.randomUUID();
        String bad = objectMapper.writeValueAsString(new OrderCreatedPayload(badEventId, 777L, "금액오류", -1L));
        kafkaTemplate.send(TOPIC, "777", bad).get(10, TimeUnit.SECONDS);

        orderSvc.regOrder(new OrderRegReqDto("정상", 3000L));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM payment")).isEqualTo(1));
        assertThat(count("SELECT count(*) FROM payment WHERE order_id = 777")).isZero();

        Map<String, Integer> dlt = readTopic(TOPIC + KafkaConfig.DLT_SUFFIX, Set.of(badEventId.toString()), Duration.ofSeconds(20));
        assertThat(dlt).containsEntry(badEventId.toString(), 1);
    }

    private Map<String, Integer> countPublishedEvents(Set<String> eventIds) {
        return readTopic(TOPIC, eventIds, Duration.ofSeconds(15));
    }

    // 지정한 토픽을 처음부터 읽어, 관심 있는 eventId 별 등장 횟수를 센다
    private Map<String, Integer> readTopic(String topic, Set<String> eventIds, Duration window) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Map<String, Integer> counts = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + window.toMillis();
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    String id = extractEventId(r.value());
                    if (id != null && eventIds.contains(id)) {
                        counts.merge(id, 1, Integer::sum);
                    }
                }
            }
        }
        return counts;
    }

    private String extractEventId(String json) {
        try {
            return objectMapper.readTree(json).path("eventId").asText(null);
        } catch (Exception e) {
            return null;
        }
    }
}
