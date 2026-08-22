package dev.pedro.retrydlq.notification.messaging;

import dev.pedro.retrydlq.notification.dlq.FailedNotification;
import dev.pedro.retrydlq.notification.store.DlqStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class DlqMonitor {

    @Inject
    DlqStore dlqStore;

    @Incoming("notification-dlq-in")
    public void consume(FailedNotification failure) {
        dlqStore.add(failure);
    }
}
