package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class SampleSolrModel {

    @Field
    private String id;

    @Field
    private String name;

    @Field
    private String source;

    @Field
    private String individual;

    @Field
    private int release;

    @Field
    private int version;

    @Field
    private String creationDate;

    @Field
    private String status;

    @Field
    private String description;

    @Field
    private String type;

    @Field
    private boolean somatic;

    @Field
    private List<String> phenotypes;

    @Field("annotations_*")
    private Map<String, Object> annotations;

    public SampleSolrModel() {
        this.phenotypes = new ArrayList<>();
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleSolrModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", source='").append(source).append('\'');
        sb.append(", individual='").append(individual).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", somatic=").append(somatic);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public SampleSolrModel setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public SampleSolrModel setName(String name) {
        this.name = name;
        return this;
    }

    public String getSource() {
        return source;
    }

    public SampleSolrModel setSource(String source) {
        this.source = source;
        return this;
    }

    public String getIndividual() {
        return individual;
    }

    public SampleSolrModel setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public SampleSolrModel setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public SampleSolrModel setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public SampleSolrModel setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public SampleSolrModel setStatus(String status) {
        this.status = status;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public SampleSolrModel setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getType() {
        return type;
    }

    public SampleSolrModel setType(String type) {
        this.type = type;
        return this;
    }

    public boolean isSomatic() {
        return somatic;
    }

    public SampleSolrModel setSomatic(boolean somatic) {
        this.somatic = somatic;
        return this;
    }

    public List<String> getPhenotypes() {
        return phenotypes;
    }

    public SampleSolrModel setPhenotypes(List<String> phenotypes) {
        this.phenotypes = phenotypes;
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
