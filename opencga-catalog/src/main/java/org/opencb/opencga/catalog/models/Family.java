package org.opencb.opencga.catalog.models;

import org.opencb.opencga.catalog.models.acls.permissions.FamilyAclEntry;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 02/05/17.
 */
public class Family extends Annotable<FamilyAclEntry> {

    private long id;
    private String name;

    private Individual father;
    private Individual mother;
    private List<Individual> children;

    private boolean parentalConsanguinity;

    private String creationDate;
    private Status status;
    private String description;

    private List<OntologyTerm> ontologyTerms;

    private Map<String, Object> attributes;

    public Family() {
    }

    public Family(String name, Individual father, Individual mother, List<Individual> children, boolean parentalConsanguinity,
                  String description) {
        this(name, father, mother, children, parentalConsanguinity, description, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap());
    }

    public Family(String name, Individual father, Individual mother, List<Individual> children, boolean parentalConsanguinity,
                  String description, List<OntologyTerm> ontologyTerms, List<AnnotationSet> annotationSets,
                  Map<String, Object> attributes) {
        this(-1, name, father, mother, children, parentalConsanguinity, TimeUtils.getTime(), new Status(), description, ontologyTerms,
                annotationSets, Collections.emptyList(), attributes);
    }

    public Family(long id, String name, Individual father, Individual mother, List<Individual> children, boolean parentalConsanguinity,
                  String creationDate, Status status, String description, List<OntologyTerm> ontologyTerms,
                  List<AnnotationSet> annotationSets, List<FamilyAclEntry> acl, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.father = father;
        this.mother = mother;
        this.children = children;
        this.parentalConsanguinity = parentalConsanguinity;
        this.creationDate = creationDate;
        this.status = status;
        this.description = description;
        this.ontologyTerms = ontologyTerms;
        this.attributes = attributes;
        this.acl = acl;
        this.annotationSets = annotationSets;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Family{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", father=").append(father);
        sb.append(", mother=").append(mother);
        sb.append(", children=").append(children);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
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

    public Individual getFather() {
        return father;
    }

    public Family setFather(Individual father) {
        this.father = father;
        return this;
    }

    public Individual getMother() {
        return mother;
    }

    public Family setMother(Individual mother) {
        this.mother = mother;
        return this;
    }

    public List<Individual> getChildren() {
        return children;
    }

    public Family setChildren(List<Individual> children) {
        this.children = children;
        return this;
    }

    public boolean isParentalConsanguinity() {
        return parentalConsanguinity;
    }

    public Family setParentalConsanguinity(boolean parentalConsanguinity) {
        this.parentalConsanguinity = parentalConsanguinity;
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

}
