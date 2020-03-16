package org.opencb.opencga.core.models.sample;

import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatusParams;

import java.util.List;
import java.util.Map;

public class SampleCreateParams {

    private String id;
    private String description;
    private String individualId;
    private SampleProcessing processing;
    private SampleCollection collection;
    private Boolean somatic;
    private List<Phenotype> phenotypes;

    private CustomStatusParams status;

    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;

    public SampleCreateParams() {
    }

    public SampleCreateParams(String id, String description, String individualId, SampleProcessing processing, SampleCollection collection,
                              Boolean somatic, List<Phenotype> phenotypes, CustomStatusParams status, List<AnnotationSet> annotationSets,
                              Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.individualId = individualId;
        this.processing = processing;
        this.collection = collection;
        this.somatic = somatic;
        this.phenotypes = phenotypes;
        this.status = status;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
    }

    public static SampleCreateParams of(Sample sample) {
        return new SampleCreateParams(sample.getId(), sample.getDescription(), sample.getIndividualId(), sample.getProcessing(),
                sample.getCollection(), sample.isSomatic(), sample.getPhenotypes(), CustomStatusParams.of(sample.getStatus()),
                sample.getAnnotationSets(), sample.getAttributes());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleCreateParams{");
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

    public Sample toSample() {
        return new Sample(getId(), getIndividualId(), getProcessing(), getCollection(), 1, 1, getDescription(), getSomatic(),
                getPhenotypes(), getAnnotationSets(), getStatus() != null ? getStatus().toCustomStatus() : null, null, getAttributes());
    }

    public String getId() {
        return id;
    }

    public SampleCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public SampleCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public SampleCreateParams setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public SampleProcessing getProcessing() {
        return processing;
    }

    public SampleCreateParams setProcessing(SampleProcessing processing) {
        this.processing = processing;
        return this;
    }

    public SampleCollection getCollection() {
        return collection;
    }

    public SampleCreateParams setCollection(SampleCollection collection) {
        this.collection = collection;
        return this;
    }

    public Boolean getSomatic() {
        return somatic;
    }

    public SampleCreateParams setSomatic(Boolean somatic) {
        this.somatic = somatic;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public SampleCreateParams setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public SampleCreateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public SampleCreateParams setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public SampleCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
