package org.opencb.opencga.core.models.notification;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class NotificationCreateParams {

    @DataField(id = "subject", immutable = true, description = FieldConstants.NOTIFICATION_SUBJECT_DESCRIPTION)
    private String subject;

    @DataField(id = "body", immutable = true, description = FieldConstants.NOTIFICATION_BODY_DESCRIPTION)
    private String body;

    @DataField(id = "type", immutable = true, description = FieldConstants.NOTIFICATION_TYPE_DESCRIPTION)
    private NotificationType type;

    @DataField(id = "scope", immutable = true, description = FieldConstants.NOTIFICATION_SCOPE_DESCRIPTION)
    private NotificationScope scope;

    @DataField(id = "fqn", immutable = true, description = FieldConstants.NOTIFICATION_FQN_DESCRIPTION)
    private String fqn;

    @DataField(id = "target", immutable = true, description = FieldConstants.NOTIFICATION_TARGET_DESCRIPTION)
    private String target;

    public NotificationCreateParams() {
    }

    public NotificationCreateParams(String subject, String body, NotificationType type, NotificationScope scope, String fqn, String target) {
        this.subject = subject;
        this.body = body;
        this.type = type;
        this.scope = scope;
        this.fqn = fqn;
        this.target = target;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationCreateParams{");
        sb.append("subject='").append(subject).append('\'');
        sb.append(", body='").append(body).append('\'');
        sb.append(", type=").append(type);
        sb.append(", scope=").append(scope);
        sb.append(", fqn=").append(fqn);
        sb.append(", target='").append(target).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSubject() {
        return subject;
    }

    public NotificationCreateParams setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public String getBody() {
        return body;
    }

    public NotificationCreateParams setBody(String body) {
        this.body = body;
        return this;
    }

    public NotificationType getType() {
        return type;
    }

    public NotificationCreateParams setType(NotificationType type) {
        this.type = type;
        return this;
    }

    public NotificationScope getScope() {
        return scope;
    }

    public NotificationCreateParams setScope(NotificationScope scope) {
        this.scope = scope;
        return this;
    }

    public String getFqn() {
        return fqn;
    }

    public NotificationCreateParams setFqn(String fqn) {
        this.fqn = fqn;
        return this;
    }

    public String getTarget() {
        return target;
    }

    public NotificationCreateParams setTarget(String target) {
        this.target = target;
        return this;
    }
}
