package org.opencb.opencga.core.models.clinical;

public class ClinicalStatusValue {

    protected String id;
    protected String description;
    protected ClinicalStatusType type;

    public enum ClinicalStatusType {
        NOT_STARTED,
        ACTIVE,
        DONE,
        CLOSED
    }

    public ClinicalStatusValue() {
    }

    public ClinicalStatusValue(String id, String description, ClinicalStatusType type) {
        this.id = id;
        this.description = description;
        this.type = type;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalStatusValue{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalStatusValue setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalStatusValue setDescription(String description) {
        this.description = description;
        return this;
    }

    public ClinicalStatusType getType() {
        return type;
    }

    public ClinicalStatusValue setType(ClinicalStatusType type) {
        this.type = type;
        return this;
    }
}
