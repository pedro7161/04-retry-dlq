package dev.pedro.retrydlq.notification.messaging;

import dev.pedro.retrydlq.notification.dlq.FailedNotification;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

@ApplicationScoped
public class KafkaDlqPublisher implements DlqPublisher {

    private static final Logger LOG = Logger.getLogger(KafkaDlqPublisher.class);

    @Inject
    @Channel("notification-dlq-out")
    Emitter<FailedNotification> emitter;

    @Override
    public void publish(FailedNotification failure) {
        LOG.errorf("Sending %s to notification-dlq", failure.originalEvent().eventId());
        emitter.send(failure).toCompletableFuture().join();
    }
}
