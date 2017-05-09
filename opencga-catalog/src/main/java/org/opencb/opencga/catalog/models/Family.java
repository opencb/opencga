package org.opencb.opencga.catalog.models;

import org.opencb.opencga.catalog.models.acls.permissions.FamilyAclEntry;

import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 02/05/17.
 */
public class Family extends Annotable<FamilyAclEntry> {

    private long id;
    private String name;
    private List<Long> individualIds;
    private String creationDate;
    private Status status;
    private String description;

    private List<OntologyTerm> ontologyTerms;

    private Map<String, Object> attributes;

    public Family() {
    }

    public Family(long id, String name, List<Long> individualIds, String creationDate, Status status, String description,
                  List<OntologyTerm> ontologyTerms, List<FamilyAclEntry> aclEntries, List<AnnotationSet> annotationSets,
                  Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.individualIds = individualIds;
        this.creationDate = creationDate;
        this.status = status;
        this.description = description;
        this.ontologyTerms = ontologyTerms;
        this.attributes = attributes;
        this.acl = aclEntries;
        this.annotationSets = annotationSets;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Family{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", individualIds=").append(individualIds);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", description='").append(description).append('\'');
        sb.append(", ontologyTerms=").append(ontologyTerms);
        sb.append(", attributes=").append(attributes);
        sb.append(", acl=").append(acl);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public Family setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Family setName(String name) {
        this.name = name;
        return this;
    }

    public List<Long> getIndividualIds() {
        return individualIds;
    }

    public Family setIndividualIds(List<Long> individualIds) {
        this.individualIds = individualIds;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Family setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Family setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Family setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<OntologyTerm> getOntologyTerms() {
        return ontologyTerms;
    }

    public Family setOntologyTerms(List<OntologyTerm> ontologyTerms) {
        this.ontologyTerms = ontologyTerms;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Family setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public Family setAcl(List<FamilyAclEntry> acl) {
        this.acl = acl;
        return this;
    }

}
