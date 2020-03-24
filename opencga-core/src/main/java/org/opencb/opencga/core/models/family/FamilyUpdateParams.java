package org.opencb.opencga.core.models.family;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatusParams;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class FamilyUpdateParams {
    private String id;
    private String name;
    private String description;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;
    private List<String> members;
    private Integer expectedSize;
    private CustomStatusParams status;
    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;

    public FamilyUpdateParams() {
    }

    public FamilyUpdateParams(String id, String name, String description, List<Phenotype> phenotypes, List<Disorder> disorders,
                              List<String> members, Integer expectedSize, CustomStatusParams status, List<AnnotationSet> annotationSets,
                              Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.phenotypes = phenotypes;
        this.disorders = disorders;
        this.members = members;
        this.expectedSize = expectedSize;
        this.status = status;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        List<AnnotationSet> annotationSetList = this.annotationSets;
        this.annotationSets = null;

        ObjectMap params = new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));

        this.annotationSets = annotationSetList;
        if (this.annotationSets != null) {
            // We leave annotation sets as is so we don't need to make any more castings
            params.put("annotationSets", this.annotationSets);
        }

        return params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", members=").append(members);
        sb.append(", expectedSize=").append(expectedSize);
        sb.append(", status=").append(status);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FamilyUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public FamilyUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FamilyUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public FamilyUpdateParams setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<Disorder> getDisorders() {
        return disorders;
    }

    public FamilyUpdateParams setDisorders(List<Disorder> disorders) {
        this.disorders = disorders;
        return this;
    }

    public List<String> getMembers() {
        return members;
    }

    public FamilyUpdateParams setMembers(List<String> members) {
        this.members = members;
        return this;
    }

    public Integer getExpectedSize() {
        return expectedSize;
    }

    public FamilyUpdateParams setExpectedSize(Integer expectedSize) {
        this.expectedSize = expectedSize;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public FamilyUpdateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public FamilyUpdateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public FamilyUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
