package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class SampleSolrModel extends CatalogSolrModel {

    @Field
    private int version;

    @Field
    private boolean somatic;

    @Field
    private String product;

    @Field
    private String preparationMethod;

    @Field
    private String extractionMethod;

    @Field
    private String labSampleId;

    @Field
    private String tissue;

    @Field
    private String organ;

    @Field
    private String method;

    @Field
    private List<String> phenotypes;

    @Field
    private List<String> annotationSets;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public SampleSolrModel() {
        this.annotationSets = new ArrayList<>();
        this.phenotypes = new ArrayList<>();
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleSolrModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationYear=").append(creationYear);
        sb.append(", creationMonth='").append(creationMonth).append('\'');
        sb.append(", creationDay=").append(creationDay);
        sb.append(", creationDayOfWeek='").append(creationDayOfWeek).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", somatic=").append(somatic);
        sb.append(", product='").append(product).append('\'');
        sb.append(", preparationMethod='").append(preparationMethod).append('\'');
        sb.append(", extractionMethod='").append(extractionMethod).append('\'');
        sb.append(", labSampleId='").append(labSampleId).append('\'');
        sb.append(", tissue='").append(tissue).append('\'');
        sb.append(", organ='").append(organ).append('\'');
        sb.append(", method='").append(method).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", acl=").append(acl);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public int getVersion() {
        return version;
    }

    public SampleSolrModel setVersion(int version) {
        this.version = version;
        return this;
    }

    public boolean isSomatic() {
        return somatic;
    }

    public SampleSolrModel setSomatic(boolean somatic) {
        this.somatic = somatic;
        return this;
    }

    public String getProduct() {
        return product;
    }

    public SampleSolrModel setProduct(String product) {
        this.product = product;
        return this;
    }

    public String getPreparationMethod() {
        return preparationMethod;
    }

    public SampleSolrModel setPreparationMethod(String preparationMethod) {
        this.preparationMethod = preparationMethod;
        return this;
    }

    public String getExtractionMethod() {
        return extractionMethod;
    }

    public SampleSolrModel setExtractionMethod(String extractionMethod) {
        this.extractionMethod = extractionMethod;
        return this;
    }

    public String getLabSampleId() {
        return labSampleId;
    }

    public SampleSolrModel setLabSampleId(String labSampleId) {
        this.labSampleId = labSampleId;
        return this;
    }

    public String getTissue() {
        return tissue;
    }

    public SampleSolrModel setTissue(String tissue) {
        this.tissue = tissue;
        return this;
    }

    public String getOrgan() {
        return organ;
    }

    public SampleSolrModel setOrgan(String organ) {
        this.organ = organ;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public SampleSolrModel setMethod(String method) {
        this.method = method;
        return this;
    }

    public List<String> getPhenotypes() {
        return phenotypes;
    }

    public SampleSolrModel setPhenotypes(List<String> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<String> getAnnotationSets() {
        return annotationSets;
    }

    public SampleSolrModel setAnnotationSets(List<String> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public SampleSolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }
}

