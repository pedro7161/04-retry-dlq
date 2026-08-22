package dev.pedro.retrydlq.notification.dlq;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class FailedNotificationDeserializer extends ObjectMapperDeserializer<FailedNotification> {

    public FailedNotificationDeserializer() {
        super(FailedNotification.class);
    }
}
