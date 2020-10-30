package org.opencb.opencga.core.models.study.configuration;

public class ClinicalConsent {

    private String id;
    private String name;
    private String description;
    private Value value;

    public enum Value {
        YES,
        NO,
        UNKNOWN
    }

    public ClinicalConsent() {
    }

    public ClinicalConsent(String id, String name, String description, Value value) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalConsent{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalConsent setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ClinicalConsent setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalConsent setDescription(String description) {
        this.description = description;
        return this;
    }

    public Value getValue() {
        return value;
    }

    public ClinicalConsent setValue(Value value) {
        this.value = value;
        return this;
    }
}
