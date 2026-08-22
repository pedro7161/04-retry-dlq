package dev.pedro.retrydlq.producer.api;

import dev.pedro.retrydlq.producer.event.OrderConfirmed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

@Path("/events/order-confirmed")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class OrderConfirmedResource {

    @Inject
    @Channel("notification-out")
    Emitter<OrderConfirmed> emitter;

    @POST
    public CompletionStage<Response> publish(OrderConfirmedRequest request) {
        if (request == null || request.orderId() == null || request.customerEmail() == null || request.customerEmail().isBlank()) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                    Response.status(Response.Status.BAD_REQUEST)
                            .entity("orderId and customerEmail are required")
                            .build());
        }

        OrderConfirmed event = new OrderConfirmed(
                UUID.randomUUID(),
                request.orderId(),
                request.customerEmail(),
                Instant.now());

        return emitter.send(event)
                .thenApply(ignored -> Response.accepted(event).build());
    }
}
