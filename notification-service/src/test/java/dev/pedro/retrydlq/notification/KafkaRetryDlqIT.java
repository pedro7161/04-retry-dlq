package dev.pedro.retrydlq.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.pedro.retrydlq.notification.dlq.FailedNotification;
import dev.pedro.retrydlq.notification.email.EmailProviderBehavior;
import dev.pedro.retrydlq.notification.event.OrderConfirmed;
import dev.pedro.retrydlq.notification.service.EmailDeliveryService;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
class KafkaRetryDlqIT {

    private static final String NOTIFICATION_TOPIC = "notification";
    private static final String DLQ_TOPIC = "notification-dlq";

    @InjectKafkaCompanion
    KafkaCompanion companion;

    @Inject
    ObjectMapper objectMapper;

    @AfterEach
    void resetProvider() {
        configureProvider(EmailProviderBehavior.ALWAYS_SUCCEED, 0);
    }

    @Test
    void eventSucceedsAfterConfiguredRetryAttempts() throws Exception {
        OrderConfirmed event = event("eventually-succeeds@example.com");
        configureProvider(EmailProviderBehavior.FAIL_FIRST_N_ATTEMPTS, EmailDeliveryService.MAX_RETRIES);

        ConsumerTask<String, String> dlqRecords = companion.consumeStrings()
                .withGroupId("success-dlq-" + UUID.randomUUID())
                .withProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .fromTopics(DLQ_TOPIC, Duration.ofSeconds(5));

        publish(event);

        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertSuccessfulNotification(event));

        dlqRecords.awaitCompletion();
        assertFalse(containsFailureFor(dlqRecords, event.eventId()),
                "successful event must not be written to notification-dlq");
    }

    @Test
    void failedNotificationIsPublishedToDlqAfterRetriesAreExhausted() throws Exception {
        OrderConfirmed event = event("always-fails@example.com");
        configureProvider(EmailProviderBehavior.ALWAYS_FAIL, 0);

        ConsumerTask<String, String> dlqRecords = companion.consumeStrings()
                .withGroupId("failure-dlq-" + UUID.randomUUID())
                .withProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .fromTopics(DLQ_TOPIC, 1);

        publish(event);

        ConsumerRecord<String, String> dlqRecord = dlqRecords.awaitCompletion().getLastRecord();
        FailedNotification failure = objectMapper.readValue(dlqRecord.value(), FailedNotification.class);

        assertEquals(event, failure.originalEvent());
        assertEquals(event.eventId(), failure.originalEvent().eventId());
        assertEquals(event.orderId(), failure.originalEvent().orderId());
        assertEquals(event.customerEmail(), failure.originalEvent().customerEmail());
        assertNotNull(failure.error());
        assertFalse(failure.error().isBlank());
        assertTrue(failure.error().contains(event.eventId().toString()));
        assertEquals(EmailDeliveryService.MAX_ATTEMPTS, failure.numberOfAttempts());
        assertNotNull(failure.failedAt());
        assertFalse(failure.failedAt().isBefore(event.occurredAt()));

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertDlqProjectionContains(event.eventId()));
    }

    private void publish(OrderConfirmed event) throws Exception {
        String json = objectMapper.writeValueAsString(event);
        companion.produceStrings()
                .fromRecords(new ProducerRecord<>(NOTIFICATION_TOPIC, event.eventId().toString(), json))
                .awaitCompletion();
    }

    private void configureProvider(EmailProviderBehavior behavior, int failFirstNAttempts) {
        given()
                .contentType("application/json")
                .body(Map.of(
                        "behavior", behavior.name(),
                        "failFirstNAttempts", failFirstNAttempts))
                .when()
                .post("/admin/email-provider/config")
                .then()
                .statusCode(200);
    }

    private void assertSuccessfulNotification(OrderConfirmed event) {
        List<Map<String, Object>> notifications = given()
                .when()
                .get("/notifications")
                .then()
                .statusCode(200)
                .extract()
                .jsonPath()
                .getList("$");

        Map<String, Object> notification = notifications.stream()
                .filter(record -> event.eventId().toString().equals(record.get("eventId")))
                .findFirst()
                .orElse(null);

        assertNotNull(notification, "successful notification record was not created");
        assertEquals(EmailDeliveryService.MAX_ATTEMPTS,
                ((Number) notification.get("attempts")).intValue());
    }

    private void assertDlqProjectionContains(UUID eventId) {
        List<Map<String, Object>> failures = given()
                .when()
                .get("/admin/dlq")
                .then()
                .statusCode(200)
                .extract()
                .jsonPath()
                .getList("$");

        boolean found = failures.stream().anyMatch(failure -> {
            Object originalEvent = failure.get("originalEvent");
            if (!(originalEvent instanceof Map<?, ?> eventMap)) {
                return false;
            }
            return eventId.toString().equals(eventMap.get("eventId"));
        });

        assertTrue(found, "DLQ monitor did not observe the Kafka DLQ record");
    }

    private boolean containsFailureFor(ConsumerTask<String, String> records, UUID eventId) throws Exception {
        for (ConsumerRecord<String, String> record : records) {
            FailedNotification failure = objectMapper.readValue(record.value(), FailedNotification.class);
            if (eventId.equals(failure.originalEvent().eventId())) {
                return true;
            }
        }
        return false;
    }

    private OrderConfirmed event(String customerEmail) {
        return new OrderConfirmed(
                UUID.randomUUID(),
                UUID.randomUUID(),
                customerEmail,
                Instant.now().truncatedTo(ChronoUnit.MILLIS));
    }
}
