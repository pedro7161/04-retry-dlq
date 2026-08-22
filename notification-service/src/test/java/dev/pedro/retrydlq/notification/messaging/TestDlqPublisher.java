package dev.pedro.retrydlq.notification.messaging;

import dev.pedro.retrydlq.notification.dlq.FailedNotification;
import dev.pedro.retrydlq.notification.store.DlqStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

@Alternative
@ApplicationScoped
public class TestDlqPublisher implements DlqPublisher {

    @Inject
    DlqStore dlqStore;

    @Override
    public void publish(FailedNotification failure) {
        dlqStore.add(failure);
    }
}
