package dev.pedro.retrydlq.notification.email;

public class EmailProviderException extends RuntimeException {

    public EmailProviderException(String message) {
        super(message);
    }
}
