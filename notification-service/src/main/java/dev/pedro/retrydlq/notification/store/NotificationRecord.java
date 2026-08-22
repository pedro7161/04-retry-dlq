package dev.pedro.retrydlq.notification.store;

import java.time.Instant;
import java.util.UUID;

public record NotificationRecord(
        UUID eventId,
        UUID orderId,
        String customerEmail,
        int attempts,
        Instant sentAt) {
}
