package org.opencb.opencga.core.models.study;

import java.util.Map;

public class StudyUpdateParams {

    private String name;
    private String alias;
    private String description;
    private StudyNotification notification;

    private Map<String, Object> attributes;

    public StudyUpdateParams() {
    }

    public StudyUpdateParams(String name, String alias, String description, StudyNotification notification,
                             Map<String, Object> attributes) {
        this.name = name;
        this.alias = alias;
        this.description = description;
        this.notification = notification;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", notifications=").append(notification);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public StudyUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public StudyUpdateParams setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public StudyUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public StudyNotification getNotification() {
        return notification;
    }

    public StudyUpdateParams setNotification(StudyNotification notification) {
        this.notification = notification;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public StudyUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
