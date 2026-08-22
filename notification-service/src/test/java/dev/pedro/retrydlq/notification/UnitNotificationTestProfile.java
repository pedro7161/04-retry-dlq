package dev.pedro.retrydlq.notification;

import dev.pedro.retrydlq.notification.messaging.TestDlqPublisher;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Set;

public class UnitNotificationTestProfile implements QuarkusTestProfile {

    @Override
    public Set<Class<?>> getEnabledAlternatives() {
        return Set.of(TestDlqPublisher.class);
    }
}
