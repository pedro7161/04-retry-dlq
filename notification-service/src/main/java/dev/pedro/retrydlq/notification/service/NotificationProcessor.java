package dev.pedro.retrydlq.notification.service;

import dev.pedro.retrydlq.notification.dlq.FailedNotification;
import dev.pedro.retrydlq.notification.email.FakeEmailProvider;
import dev.pedro.retrydlq.notification.event.OrderConfirmed;
import dev.pedro.retrydlq.notification.store.NotificationRecord;
import dev.pedro.retrydlq.notification.store.NotificationStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.Optional;

@ApplicationScoped
public class NotificationProcessor {

    private static final Logger LOG = Logger.getLogger(NotificationProcessor.class);

    @Inject
    EmailDeliveryService emailDeliveryService;

    @Inject
    FakeEmailProvider emailProvider;

    @Inject
    NotificationStore notificationStore;

    public Optional<FailedNotification> process(OrderConfirmed event) {
        try {
            int attempts = emailDeliveryService.deliver(event);
            notificationStore.add(new NotificationRecord(
                    event.eventId(),
                    event.orderId(),
                    event.customerEmail(),
                    attempts,
                    Instant.now()));
            return Optional.empty();
        } catch (RuntimeException exception) {
            int attempts = emailProvider.attemptsFor(event.eventId());
            LOG.errorf("Retries exhausted for event %s", event.eventId());
            return Optional.of(new FailedNotification(
                    event,
                    rootMessage(exception),
                    attempts,
                    Instant.now()));
        } finally {
            emailProvider.clearAttempts(event.eventId());
        }
    }

    private String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }
}
