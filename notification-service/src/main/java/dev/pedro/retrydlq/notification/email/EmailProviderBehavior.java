package dev.pedro.retrydlq.notification.email;

public enum EmailProviderBehavior {
    ALWAYS_SUCCEED,
    ALWAYS_FAIL,
    FAIL_FIRST_N_ATTEMPTS
}
