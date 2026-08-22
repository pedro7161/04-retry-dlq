package dev.pedro.retrydlq.notification.event;

import java.time.Instant;
import java.util.UUID;

public record OrderConfirmed(
        UUID eventId,
        UUID orderId,
        String customerEmail,
        Instant occurredAt) {
}
