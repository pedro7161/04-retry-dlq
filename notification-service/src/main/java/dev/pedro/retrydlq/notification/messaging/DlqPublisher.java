package dev.pedro.retrydlq.notification.messaging;

import dev.pedro.retrydlq.notification.dlq.FailedNotification;

public interface DlqPublisher {
    void publish(FailedNotification failure);
}
