package org.opencb.opencga.core.models.notification;

import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.PrivateStudyUid;

@DataClass(id = "Notification", since = "4.0.0",
        description = "Notification data model hosts information about the notifications the system or other users make.")
public class Notification extends PrivateStudyUid {

    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "operationId", managed = true, unique = true, immutable = true,
            description = FieldConstants.NOTIFICATION_OPERATION_ID_DESCRIPTION)
    private String operationId;

    @DataField(id = "subject", immutable = true, description = FieldConstants.NOTIFICATION_SUBJECT_DESCRIPTION)
    private String subject;

    @DataField(id = "body", immutable = true, description = FieldConstants.NOTIFICATION_BODY_DESCRIPTION)
    private String body;

    @DataField(id = "type", immutable = true, description = FieldConstants.NOTIFICATION_TYPE_DESCRIPTION)
    private String type;

    @DataField(id = "internal", description = FieldConstants.NOTIFICATION_INTERNAL_DESCRIPTION)
    private NotificationInternal internal;

    @DataField(id = "sender", immutable = true, description = FieldConstants.NOTIFICATION_SENDER_DESCRIPTION)
    private String sender;

    @DataField(id = "target", immutable = true, description = FieldConstants.NOTIFICATION_TARGET_DESCRIPTION)
    private String target;

    public Notification() {
    }

    public Notification(String subject, String body, String type, String target) {
        this.subject = subject;
        this.body = body;
        this.type = type;
        this.target = target;
    }

    public Notification(String uuid, String operationId, String subject, String body, String type, NotificationInternal internal,
                        String sender, String target) {
        this.uuid = uuid;
        this.operationId = operationId;
        this.subject = subject;
        this.body = body;
        this.type = type;
        this.internal = internal;
        this.sender = sender;
        this.target = target;
    }

    public Notification(Notification notification) {
        this(notification.getUuid(), notification.getOperationId(), notification.getSubject(), notification.getBody(),
                notification.getType(), notification.getInternal(), notification.getSender(), notification.getTarget());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Notification{");
        sb.append("uuid='").append(uuid).append('\'');
        sb.append(", operationId='").append(operationId).append('\'');
        sb.append(", subject='").append(subject).append('\'');
        sb.append(", body='").append(body).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", internal=").append(internal);
        sb.append(", sender='").append(sender).append('\'');
        sb.append(", target='").append(target).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public Notification setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public Notification setStudyUid(long studyUid) {
        super.setStudyUid(studyUid);
        return this;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public Notification setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getOperationId() {
        return operationId;
    }

    public Notification setOperationId(String operationId) {
        this.operationId = operationId;
        return this;
    }

    public String getSubject() {
        return subject;
    }

    public Notification setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public String getBody() {
        return body;
    }

    public Notification setBody(String body) {
        this.body = body;
        return this;
    }

    public String getType() {
        return type;
    }

    public Notification setType(String type) {
        this.type = type;
        return this;
    }

    public NotificationInternal getInternal() {
        return internal;
    }

    public Notification setInternal(NotificationInternal internal) {
        this.internal = internal;
        return this;
    }

    public String getSender() {
        return sender;
    }

    public Notification setSender(String sender) {
        this.sender = sender;
        return this;
    }

    public String getTarget() {
        return target;
    }

    public Notification setTarget(String target) {
        this.target = target;
        return this;
    }
}
