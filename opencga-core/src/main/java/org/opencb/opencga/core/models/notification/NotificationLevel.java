package org.opencb.opencga.core.models.notification;

public enum NotificationLevel {

    INFO(1),
    WARNING(2),
    ERROR(3),
    URGENT(4);

    private final int value;

    NotificationLevel(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static NotificationLevel fromValue(int value) {
        for (NotificationLevel level : NotificationLevel.values()) {
            if (level.value == value) {
                return level;
            }
        }
        throw new IllegalArgumentException("Unknown Notification level value: " + value);
    }
}
