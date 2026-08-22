package dev.pedro.retrydlq.producer.event;

import java.time.Instant;
import java.util.UUID;

public record OrderConfirmed(
        UUID eventId,
        UUID orderId,
        String customerEmail,
        Instant occurredAt) {
}
