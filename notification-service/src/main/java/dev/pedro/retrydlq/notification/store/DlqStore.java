package dev.pedro.retrydlq.notification.store;

import dev.pedro.retrydlq.notification.dlq.FailedNotification;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
public class DlqStore {

    private final CopyOnWriteArrayList<FailedNotification> failures = new CopyOnWriteArrayList<>();

    public void add(FailedNotification failure) {
        failures.add(failure);
    }

    public List<FailedNotification> all() {
        return new ArrayList<>(failures);
    }

    public void clear() {
        failures.clear();
    }
}
