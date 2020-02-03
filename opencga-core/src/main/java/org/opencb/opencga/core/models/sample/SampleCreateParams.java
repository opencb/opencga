package org.opencb.opencga.core.models.sample;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.opencga.core.models.common.AnnotationSet;

import java.util.List;
import java.util.Map;

public class SampleCreateParams {

    private String id;
    private String description;
    private String type;
    private String individualId;
    private SampleProcessing processing;
    private SampleCollection collection;
    private String source;
    private Boolean somatic;
    private List<Phenotype> phenotypes;
    private List<AnnotationSet> annotationSets;
    private Map<String, Object> attributes;

    @Deprecated
    public String name;
    @Deprecated
    public Map<String, Object> stats;

    public SampleCreateParams() {
    }

    public SampleCreateParams(String id, String description, String type, String individualId, SampleProcessing processing,
                              SampleCollection collection, String source, Boolean somatic, List<Phenotype> phenotypes,
                              List<AnnotationSet> annotationSets, Map<String, Object> attributes, String name, Map<String, Object> stats) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.individualId = individualId;
        this.processing = processing;
        this.collection = collection;
        this.source = source;
        this.somatic = somatic;
        this.phenotypes = phenotypes;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
        this.name = name;
        this.stats = stats;
    }

    public static SampleCreateParams of(Sample sample) {
        return new SampleCreateParams(sample.getId(), sample.getDescription(), sample.getType(), sample.getIndividualId(),
                sample.getProcessing(), sample.getCollection(), sample.getSource(), sample.isSomatic(), sample.getPhenotypes(),
                sample.getAnnotationSets(), sample.getAttributes(), sample.getName(), sample.getStats());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleCreateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", stats=").append(stats);
        sb.append(", id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", processing=").append(processing);
        sb.append(", collection=").append(collection);
        sb.append(", source='").append(source).append('\'');
        sb.append(", somatic=").append(somatic);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Sample toSample() {

        String sampleId = StringUtils.isEmpty(this.getId()) ? name : this.getId();
        String sampleName = StringUtils.isEmpty(name) ? sampleId : name;
        return new Sample(sampleId, getSource(), getIndividualId(), getProcessing(), getCollection(), 1, 1, getDescription(), getType(),
                getSomatic(), getPhenotypes(), getAnnotationSets(), getAttributes())
                .setName(sampleName).setStats(stats);
    }

    public String getName() {
        return name;
    }

    public SampleCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public SampleCreateParams setStats(Map<String, Object> stats) {
        this.stats = stats;
        return this;
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

    public String getType() {
        return type;
    }

    public SampleCreateParams setType(String type) {
        this.type = type;
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

    public String getSource() {
        return source;
    }

    public SampleCreateParams setSource(String source) {
        this.source = source;
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
