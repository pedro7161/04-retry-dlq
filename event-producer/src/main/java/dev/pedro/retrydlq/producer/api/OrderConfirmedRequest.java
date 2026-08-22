package dev.pedro.retrydlq.producer.api;

import java.util.UUID;

public record OrderConfirmedRequest(UUID orderId, String customerEmail) {
}
