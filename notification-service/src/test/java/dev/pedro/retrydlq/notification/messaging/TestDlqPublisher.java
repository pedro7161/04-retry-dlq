package dev.pedro.retrydlq.notification.messaging;

import dev.pedro.retrydlq.notification.dlq.FailedNotification;
import dev.pedro.retrydlq.notification.store.DlqStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.quarkus.test.Mock;

@Mock
@ApplicationScoped
public class TestDlqPublisher implements DlqPublisher {

    @Inject
    DlqStore dlqStore;

    @Override
    public void publish(FailedNotification failure) {
        dlqStore.add(failure);
    }
}
