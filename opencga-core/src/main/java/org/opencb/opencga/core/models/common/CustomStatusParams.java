package org.opencb.opencga.core.models.common;

import org.opencb.opencga.core.common.TimeUtils;

public class CustomStatusParams {

    private String name;
    private String description;

    public CustomStatusParams() {
    }

    public CustomStatusParams(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public static CustomStatusParams of(CustomStatus status) {
        if (status != null) {
            return new CustomStatusParams(status.getName(), status.getDescription());
        } else {
            return null;
        }
    }

    public CustomStatus toCustomStatus() {
        return new CustomStatus(name, description, TimeUtils.getTime());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomStatusParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public CustomStatusParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CustomStatusParams setDescription(String description) {
        this.description = description;
        return this;
    }
}
