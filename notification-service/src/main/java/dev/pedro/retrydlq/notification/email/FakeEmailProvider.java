package dev.pedro.retrydlq.notification.email;

import dev.pedro.retrydlq.notification.event.OrderConfirmed;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class FakeEmailProvider {

    private final AtomicReference<EmailProviderConfig> config =
            new AtomicReference<>(EmailProviderConfig.defaultConfig());
    private final ConcurrentHashMap<UUID, AtomicInteger> attempts = new ConcurrentHashMap<>();

    public int nextAttempt(UUID eventId) {
        return attempts.computeIfAbsent(eventId, ignored -> new AtomicInteger()).incrementAndGet();
    }

    public void send(OrderConfirmed event, int attempt) {
        EmailProviderConfig current = config.get();

        boolean shouldFail = switch (current.behavior()) {
            case ALWAYS_SUCCEED -> false;
            case ALWAYS_FAIL -> true;
            case FAIL_FIRST_N_ATTEMPTS -> attempt <= current.failFirstNAttempts();
        };

        if (shouldFail) {
            throw new EmailProviderException(
                    "Fake email provider failure for event %s on attempt %d"
                            .formatted(event.eventId(), attempt));
        }
    }

    public EmailProviderConfig configure(EmailProviderConfig newConfig) {
        if (newConfig == null || newConfig.behavior() == null) {
            throw new IllegalArgumentException("behavior is required");
        }
        if (newConfig.failFirstNAttempts() < 0) {
            throw new IllegalArgumentException("failFirstNAttempts must be >= 0");
        }

        EmailProviderConfig normalized = newConfig.behavior() == EmailProviderBehavior.FAIL_FIRST_N_ATTEMPTS
                ? newConfig
                : new EmailProviderConfig(newConfig.behavior(), 0);

        config.set(normalized);
        attempts.clear();
        return normalized;
    }

    public EmailProviderConfig config() {
        return config.get();
    }

    public int attemptsFor(UUID eventId) {
        AtomicInteger counter = attempts.get(eventId);
        return counter == null ? 0 : counter.get();
    }

    public void clearAttempts(UUID eventId) {
        attempts.remove(eventId);
    }
}
