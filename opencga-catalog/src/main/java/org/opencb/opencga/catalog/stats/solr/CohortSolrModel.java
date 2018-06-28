package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class CohortSolrModel {

    @Field
    private String id;

    @Field
    private String name;

    @Field
    private String type;

    @Field
    private String creationDate;

    @Field
    private String status;

    @Field
    private String description;

    @Field
    private String family;

    @Field
    private String release;

    @Field
    private List<String> samples;

    @Field("annotations_*")
    private Map<String, Object> annotations;

    public CohortSolrModel() {
        this.samples = new ArrayList<>();
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortSolrModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", family='").append(family).append('\'');
        sb.append(", release='").append(release).append('\'');
        sb.append(", samples=").append(samples);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public CohortSolrModel setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public CohortSolrModel setName(String name) {
        this.name = name;
        return this;
    }

    public String getType() {
        return type;
    }

    public CohortSolrModel setType(String type) {
        this.type = type;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public CohortSolrModel setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public CohortSolrModel setStatus(String status) {
        this.status = status;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CohortSolrModel setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public CohortSolrModel setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getRelease() {
        return release;
    }

    public CohortSolrModel setRelease(String release) {
        this.release = release;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public CohortSolrModel setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public CohortSolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }
}
