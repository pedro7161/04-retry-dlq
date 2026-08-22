package dev.pedro.retrydlq.notification.store;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
public class NotificationStore {

    private final CopyOnWriteArrayList<NotificationRecord> notifications = new CopyOnWriteArrayList<>();

    public void add(NotificationRecord notification) {
        notifications.add(notification);
    }

    public List<NotificationRecord> all() {
        return new ArrayList<>(notifications);
    }

    public void clear() {
        notifications.clear();
    }
}
