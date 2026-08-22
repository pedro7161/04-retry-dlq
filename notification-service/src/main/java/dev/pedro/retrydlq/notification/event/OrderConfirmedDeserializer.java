package dev.pedro.retrydlq.notification.event;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class OrderConfirmedDeserializer extends ObjectMapperDeserializer<OrderConfirmed> {

    public OrderConfirmedDeserializer() {
        super(OrderConfirmed.class);
    }
}
