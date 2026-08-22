package dev.pedro.retrydlq.notification.api;

import dev.pedro.retrydlq.notification.store.NotificationRecord;
import dev.pedro.retrydlq.notification.store.NotificationStore;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;

@Path("/notifications")
@Produces(MediaType.APPLICATION_JSON)
public class NotificationResource {

    @Inject
    NotificationStore notificationStore;

    @GET
    public List<NotificationRecord> all() {
        return notificationStore.all();
    }
}
