package dev.pedro.retrydlq.notification.service;

import dev.pedro.retrydlq.notification.email.EmailProviderException;
import dev.pedro.retrydlq.notification.email.FakeEmailProvider;
import dev.pedro.retrydlq.notification.event.OrderConfirmed;
import io.smallrye.faulttolerance.api.ExponentialBackoff;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.jboss.logging.Logger;

import java.time.temporal.ChronoUnit;

@ApplicationScoped
public class EmailDeliveryService {

    public static final int MAX_RETRIES = 3;
    public static final int MAX_ATTEMPTS = MAX_RETRIES + 1;

    private static final Logger LOG = Logger.getLogger(EmailDeliveryService.class);

    @Inject
    FakeEmailProvider emailProvider;

    @Retry(
            maxRetries = MAX_RETRIES,
            delay = 200,
            delayUnit = ChronoUnit.MILLIS,
            jitter = 50,
            jitterDelayUnit = ChronoUnit.MILLIS,
            retryOn = EmailProviderException.class)
    @ExponentialBackoff(
            factor = 2,
            maxDelay = 2,
            maxDelayUnit = ChronoUnit.SECONDS)
    public int deliver(OrderConfirmed event) {
        int attempt = emailProvider.nextAttempt(event.eventId());
        LOG.infof("Processing event %s - attempt %d", event.eventId(), attempt);

        try {
            emailProvider.send(event, attempt);
            return attempt;
        } catch (EmailProviderException exception) {
            LOG.warn("Email provider failed");
            if (attempt < MAX_ATTEMPTS) {
                LOG.infof("Retrying event %s", event.eventId());
            }
            throw exception;
        }
    }
}
