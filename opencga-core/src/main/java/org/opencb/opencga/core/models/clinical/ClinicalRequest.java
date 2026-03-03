package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.annotations.DataField;

import java.util.Map;

public class ClinicalRequest {

    @DataField(id = "id", description = "Unique identifier for the clinical request")
    private String id;

    @DataField(id = "justification", description = "Justification for the clinical request")
    private String justification;

    @DataField(id = "date", description = "Date of the clinical request")
    private String date;

    @DataField(id = "responsible", description = "Responsible person for the clinical request")
    private ClinicalResponsible responsible;

    @DataField(id = "attributes", description = "Additional attributes for the clinical request")
    private Map<String, Object> attributes;

    public ClinicalRequest() {
    }

    public ClinicalRequest(String id, String justification, String date, ClinicalResponsible responsible, Map<String, Object> attributes) {
        this.id = id;
        this.justification = justification;
        this.date = date;
        this.responsible = responsible;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalRequest{");
        sb.append("id='").append(id).append('\'');
        sb.append(", justification='").append(justification).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", responsible=").append(responsible);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalRequest setId(String id) {
        this.id = id;
        return this;
    }

    public String getJustification() {
        return justification;
    }

    public ClinicalRequest setJustification(String justification) {
        this.justification = justification;
        return this;
    }

    public String getDate() {
        return date;
    }

    public ClinicalRequest setDate(String date) {
        this.date = date;
        return this;
    }

    public ClinicalResponsible getResponsible() {
        return responsible;
    }

    public ClinicalRequest setResponsible(ClinicalResponsible responsible) {
        this.responsible = responsible;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalRequest setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
