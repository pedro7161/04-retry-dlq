package dev.pedro.retrydlq.notification.email;

public record EmailProviderConfig(
        EmailProviderBehavior behavior,
        int failFirstNAttempts) {

    public static EmailProviderConfig defaultConfig() {
        return new EmailProviderConfig(EmailProviderBehavior.ALWAYS_SUCCEED, 0);
    }
}
