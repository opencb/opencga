package org.opencb.opencga.core.models.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.common.AnnotationSet;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class SampleUpdateParams {

    private String id;
    private String description;
    private String individualId;
    private SampleProcessing processing;
    private SampleCollection collection;
    private Boolean somatic;
    private List<Phenotype> phenotypes;
    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;

    public SampleUpdateParams() {
    }

    public SampleUpdateParams(String id, String description, String individualId, SampleProcessing processing, SampleCollection collection,
                              Boolean somatic, List<Phenotype> phenotypes, List<AnnotationSet> annotationSets,
                              Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.individualId = individualId;
        this.processing = processing;
        this.collection = collection;
        this.somatic = somatic;
        this.phenotypes = phenotypes;
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
        final StringBuilder sb = new StringBuilder("SampleUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", processing=").append(processing);
        sb.append(", collection=").append(collection);
        sb.append(", somatic=").append(somatic);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }


    public String getId() {
        return id;
    }

    public SampleUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public SampleUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public SampleUpdateParams setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public SampleProcessing getProcessing() {
        return processing;
    }

    public SampleUpdateParams setProcessing(SampleProcessing processing) {
        this.processing = processing;
        return this;
    }

    public SampleCollection getCollection() {
        return collection;
    }

    public SampleUpdateParams setCollection(SampleCollection collection) {
        this.collection = collection;
        return this;
    }

    public Boolean getSomatic() {
        return somatic;
    }

    public SampleUpdateParams setSomatic(Boolean somatic) {
        this.somatic = somatic;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public SampleUpdateParams setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public SampleUpdateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public SampleUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
