package dev.pedro.retrydlq.notification.dlq;

import dev.pedro.retrydlq.notification.event.OrderConfirmed;

import java.time.Instant;

public record FailedNotification(
        OrderConfirmed originalEvent,
        String error,
        int numberOfAttempts,
        Instant failedAt) {
}
