package org.opencb.opencga.core.models.clinical;

public class ClinicalIdentifier {

    private String id;
    private String description;
    private String value;

    public ClinicalIdentifier() {
    }

    public ClinicalIdentifier(String id, String description, String value) {
        this.id = id;
        this.description = description;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalIdentifier{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", value='").append(value).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalIdentifier setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalIdentifier setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getValue() {
        return value;
    }

    public ClinicalIdentifier setValue(String value) {
        this.value = value;
        return this;
    }
}
