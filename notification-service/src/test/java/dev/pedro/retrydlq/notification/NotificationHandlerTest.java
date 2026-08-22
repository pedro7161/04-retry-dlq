package dev.pedro.retrydlq.notification;

import dev.pedro.retrydlq.notification.email.EmailProviderBehavior;
import dev.pedro.retrydlq.notification.email.EmailProviderConfig;
import dev.pedro.retrydlq.notification.email.FakeEmailProvider;
import dev.pedro.retrydlq.notification.event.OrderConfirmed;
import dev.pedro.retrydlq.notification.service.NotificationHandler;
import dev.pedro.retrydlq.notification.store.DlqStore;
import dev.pedro.retrydlq.notification.store.NotificationStore;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(UnitNotificationTestProfile.class)
class NotificationHandlerTest {

    @Inject
    NotificationHandler handler;

    @Inject
    FakeEmailProvider emailProvider;

    @Inject
    NotificationStore notificationStore;

    @Inject
    DlqStore dlqStore;

    @BeforeEach
    void reset() {
        notificationStore.clear();
        dlqStore.clear();
        emailProvider.configure(EmailProviderConfig.defaultConfig());
    }

    @Test
    void providerWorksImmediately() {
        emailProvider.configure(new EmailProviderConfig(EmailProviderBehavior.ALWAYS_SUCCEED, 0));

        handler.handle(event());

        assertEquals(1, notificationStore.all().size());
        assertEquals(1, notificationStore.all().getFirst().attempts());
        assertTrue(dlqStore.all().isEmpty());
    }

    @Test
    void providerFailsTwiceAndThenSucceeds() {
        emailProvider.configure(new EmailProviderConfig(EmailProviderBehavior.FAIL_FIRST_N_ATTEMPTS, 2));

        handler.handle(event());

        assertEquals(1, notificationStore.all().size());
        assertEquals(3, notificationStore.all().getFirst().attempts());
        assertTrue(dlqStore.all().isEmpty());
    }

    @Test
    void providerAlwaysFailsAndEventReachesDlq() {
        emailProvider.configure(new EmailProviderConfig(EmailProviderBehavior.ALWAYS_FAIL, 0));
        OrderConfirmed event = event();

        handler.handle(event);

        assertTrue(notificationStore.all().isEmpty());
        assertEquals(1, dlqStore.all().size());
        assertEquals(event.eventId(), dlqStore.all().getFirst().originalEvent().eventId());
        assertEquals(4, dlqStore.all().getFirst().numberOfAttempts());
    }

    private OrderConfirmed event() {
        return new OrderConfirmed(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "customer@example.com",
                Instant.now());
    }
}
