package dev.pedro.retrydlq.notification.api;

import dev.pedro.retrydlq.notification.email.EmailProviderConfig;
import dev.pedro.retrydlq.notification.email.FakeEmailProvider;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/admin/email-provider/config")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class EmailProviderAdminResource {

    @Inject
    FakeEmailProvider emailProvider;

    @POST
    public EmailProviderConfig configure(EmailProviderConfig config) {
        try {
            return emailProvider.configure(config);
        } catch (IllegalArgumentException exception) {
            throw new BadRequestException(exception.getMessage());
        }
    }
}
