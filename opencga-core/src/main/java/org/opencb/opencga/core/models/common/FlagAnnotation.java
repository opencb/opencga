package org.opencb.opencga.core.models.common;

public class FlagAnnotation {

    private String id;
    private String description;
    private String date;

    public FlagAnnotation() {
    }

    public FlagAnnotation(String id, String description, String date) {
        this.id = id;
        this.description = description;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FlagAnnotation{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", date=").append(date);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FlagAnnotation setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FlagAnnotation setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getDate() {
        return date;
    }

    public FlagAnnotation setDate(String date) {
        this.date = date;
        return this;
    }
}
