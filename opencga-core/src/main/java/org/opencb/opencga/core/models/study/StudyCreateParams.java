package org.opencb.opencga.core.models.study;

import org.opencb.opencga.core.models.common.CustomStatusParams;

import java.util.Map;

public class StudyCreateParams {

    private String id;
    private String name;
    private String alias;
    private String description;
    private StudyNotification notification;
    private Map<String, Object> attributes;
    private CustomStatusParams status;

    public StudyCreateParams() {
    }

    public StudyCreateParams(String id, String name, String alias, String description, StudyNotification notification,
                             Map<String, Object> attributes, CustomStatusParams status) {
        this.id = id;
        this.name = name;
        this.alias = alias;
        this.description = description;
        this.notification = notification;
        this.attributes = attributes;
        this.status = status;
    }

    public static StudyCreateParams of(Study study) {
        return new StudyCreateParams(study.getId(), study.getName(), study.getAlias(), study.getDescription(),
                study.getNotification(), study.getAttributes(), CustomStatusParams.of(study.getStatus()));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", notification=").append(notification);
        sb.append(", attributes=").append(attributes);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public StudyCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public StudyCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public StudyCreateParams setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public StudyCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public StudyNotification getNotification() {
        return notification;
    }

    public StudyCreateParams setNotification(StudyNotification notification) {
        this.notification = notification;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public StudyCreateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public StudyCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
