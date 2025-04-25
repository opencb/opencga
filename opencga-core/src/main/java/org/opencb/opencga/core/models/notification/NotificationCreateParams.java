package org.opencb.opencga.core.models.notification;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.List;

public class NotificationCreateParams {

    @DataField(id = "subject", immutable = true, description = FieldConstants.NOTIFICATION_SUBJECT_DESCRIPTION)
    private String subject;

    @DataField(id = "content", immutable = true, description = FieldConstants.NOTIFICATION_CONTENT_DESCRIPTION)
    private String content;

    @DataField(id = "level", immutable = true, description = FieldConstants.NOTIFICATION_LEVEL_DESCRIPTION)
    private NotificationLevel level;

    @DataField(id = "scope", immutable = true, description = FieldConstants.NOTIFICATION_SCOPE_DESCRIPTION)
    private NotificationScope scope;

    @DataField(id = "fqn", immutable = true, description = FieldConstants.NOTIFICATION_FQN_DESCRIPTION)
    private String fqn;

    @DataField(id = "targets", immutable = true, description = FieldConstants.NOTIFICATION_TARGET_DESCRIPTION)
    private List<String> targets;

    public NotificationCreateParams() {
    }

    public NotificationCreateParams(String subject, String content, NotificationLevel level, NotificationScope scope, String fqn,
                                    List<String> targets) {
        this.subject = subject;
        this.content = content;
        this.level = level;
        this.scope = scope;
        this.fqn = fqn;
        this.targets = targets;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotificationCreateParams{");
        sb.append("subject='").append(subject).append('\'');
        sb.append(", content='").append(content).append('\'');
        sb.append(", level=").append(level);
        sb.append(", scope=").append(scope);
        sb.append(", fqn=").append(fqn);
        sb.append(", targets='").append(targets).append('\'');
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

    public String getContent() {
        return content;
    }

    public NotificationCreateParams setContent(String content) {
        this.content = content;
        return this;
    }

    public NotificationLevel getLevel() {
        return level;
    }

    public NotificationCreateParams setLevel(NotificationLevel level) {
        this.level = level;
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

    public List<String> getTargets() {
        return targets;
    }

    public NotificationCreateParams setTargets(List<String> targets) {
        this.targets = targets;
        return this;
    }
}
