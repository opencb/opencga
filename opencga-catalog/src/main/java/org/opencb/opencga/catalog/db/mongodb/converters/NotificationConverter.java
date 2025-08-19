package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.core.models.notification.Notification;

public class NotificationConverter extends OpenCgaMongoConverter<Notification> {

    public NotificationConverter() {
        super(Notification.class);
    }

    public NotificationConverter(ObjectMapper objectMapper) {
        super(Notification.class, objectMapper);
    }
}
