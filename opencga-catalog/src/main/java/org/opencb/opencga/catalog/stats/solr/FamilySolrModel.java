package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class FamilySolrModel {

    @Field
    private String id;

    @Field
    private String name;

    @Field
    private String creationDate;

    @Field
    private String status;

    @Field
    private int expectedSize;

    @Field
    private String description;

    @Field
    private int release;

    @Field
    private int version;

    @Field("annotations_*")
    private Map<String, Object> annotations;

    public FamilySolrModel() {
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilySolrModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", expectedSize=").append(expectedSize);
        sb.append(", description='").append(description).append('\'');
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FamilySolrModel setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public FamilySolrModel setName(String name) {
        this.name = name;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public FamilySolrModel setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public FamilySolrModel setStatus(String status) {
        this.status = status;
        return this;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public FamilySolrModel setExpectedSize(int expectedSize) {
        this.expectedSize = expectedSize;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FamilySolrModel setDescription(String description) {
        this.description = description;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public FamilySolrModel setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public FamilySolrModel setVersion(int version) {
        this.version = version;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public FamilySolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }
}
