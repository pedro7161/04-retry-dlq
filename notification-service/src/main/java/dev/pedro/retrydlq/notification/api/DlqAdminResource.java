package dev.pedro.retrydlq.notification.api;

import dev.pedro.retrydlq.notification.dlq.FailedNotification;
import dev.pedro.retrydlq.notification.store.DlqStore;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;

@Path("/admin/dlq")
@Produces(MediaType.APPLICATION_JSON)
public class DlqAdminResource {

    @Inject
    DlqStore dlqStore;

    @GET
    public List<FailedNotification> all() {
        return dlqStore.all();
    }
}
