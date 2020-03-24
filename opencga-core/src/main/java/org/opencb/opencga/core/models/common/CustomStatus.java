package org.opencb.opencga.core.models.common;

public class CustomStatus {

    private String name;
    private String description;
    private String date;

    public CustomStatus() {
        this("", "", "");
    }

    public CustomStatus(String name, String description, String date) {
        this.name = name;
        this.description = description;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomStatus{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public CustomStatus setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CustomStatus setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getDate() {
        return date;
    }

    public CustomStatus setDate(String date) {
        this.date = date;
        return this;
    }
}
