package dev.pedro.retrydlq.notification.messaging;

import dev.pedro.retrydlq.notification.event.OrderConfirmed;
import dev.pedro.retrydlq.notification.service.NotificationHandler;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class NotificationKafkaConsumer {

    @Inject
    NotificationHandler handler;

    @Incoming("notification-in")
    @Blocking
    public void consume(OrderConfirmed event) {
        handler.handle(event);
    }
}
