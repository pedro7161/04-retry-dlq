package dev.pedro.retrydlq.notification.service;

import dev.pedro.retrydlq.notification.event.OrderConfirmed;
import dev.pedro.retrydlq.notification.messaging.DlqPublisher;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class NotificationHandler {

    @Inject
    NotificationProcessor processor;

    @Inject
    DlqPublisher dlqPublisher;

    public void handle(OrderConfirmed event) {
        processor.process(event).ifPresent(dlqPublisher::publish);
    }
}
