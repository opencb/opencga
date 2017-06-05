package org.opencb.opencga.catalog.models;

import org.opencb.opencga.catalog.models.acls.AbstractAcl;
import org.opencb.opencga.catalog.models.acls.permissions.ClinicalAnalysisAclEntry;

import java.util.Map;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysis extends AbstractAcl<ClinicalAnalysisAclEntry> {

    private long id;
    private String name;
    private String description;
    private Type type;

    private Family family;
    private Individual proband;
    private Sample sample;

    private String creationDate;
    private Status status;
    private Map<String, Object> attributes;

    public enum Type {
        DUO, TRIO
    }

    public ClinicalAnalysis() {
    }

    public ClinicalAnalysis(long id, String name, String description, Type type, Family family, Individual proband, Sample sample, String
            creationDate, Status status, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.type = type;
        this.family = family;
        this.proband = proband;
        this.sample = sample;
        this.creationDate = creationDate;
        this.status = status;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysis{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", family=").append(family);
        sb.append(", proband=").append(proband);
        sb.append(", sample=").append(sample);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", attributes=").append(attributes);
        sb.append(", acl=").append(acl);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public ClinicalAnalysis setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ClinicalAnalysis setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalAnalysis setDescription(String description) {
        this.description = description;
        return this;
    }

    public Type getType() {
        return type;
    }

    public ClinicalAnalysis setType(Type type) {
        this.type = type;
        return this;
    }

    public Family getFamily() {
        return family;
    }

    public ClinicalAnalysis setFamily(Family family) {
        this.family = family;
        return this;
    }

    public Individual getProband() {
        return proband;
    }

    public ClinicalAnalysis setProband(Individual proband) {
        this.proband = proband;
        return this;
    }

    public Sample getSample() {
        return sample;
    }

    public ClinicalAnalysis setSample(Sample sample) {
        this.sample = sample;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ClinicalAnalysis setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public ClinicalAnalysis setStatus(Status status) {
        this.status = status;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalAnalysis setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
