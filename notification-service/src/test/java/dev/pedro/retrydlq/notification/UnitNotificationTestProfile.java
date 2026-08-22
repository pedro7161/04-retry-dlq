package dev.pedro.retrydlq.notification;

import dev.pedro.retrydlq.notification.messaging.TestDlqPublisher;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;
import java.util.Set;

public class UnitNotificationTestProfile implements QuarkusTestProfile {

    @Override
    public Set<Class<?>> getEnabledAlternatives() {
        return Set.of(TestDlqPublisher.class);
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("quarkus.arc.exclude-types",
                "dev.pedro.retrydlq.notification.messaging.NotificationKafkaConsumer,"
                        + "dev.pedro.retrydlq.notification.messaging.KafkaDlqPublisher,"
                        + "dev.pedro.retrydlq.notification.messaging.DlqMonitor");
    }
}
