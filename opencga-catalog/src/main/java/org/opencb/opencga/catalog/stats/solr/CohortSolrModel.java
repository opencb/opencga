package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class CohortSolrModel extends CatalogSolrModel {

    @Field
    private String type;

    @Field
    private int numSamples;

    @Field
    private List<String> annotationSets;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public CohortSolrModel() {
        this.annotationSets = new ArrayList<>();
        this.annotations = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortSolrModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", creationYear=").append(creationYear);
        sb.append(", creationMonth='").append(creationMonth).append('\'');
        sb.append(", creationDay=").append(creationDay);
        sb.append(", creationDayOfWeek='").append(creationDayOfWeek).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", release=").append(release);
        sb.append(", numSamples=").append(numSamples);
        sb.append(", acl=").append(acl);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public String getType() {
        return type;
    }

    public CohortSolrModel setType(String type) {
        this.type = type;
        return this;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public CohortSolrModel setNumSamples(int numSamples) {
        this.numSamples = numSamples;
        return this;
    }

    public List<String> getAnnotationSets() {
        return annotationSets;
    }

    public CohortSolrModel setAnnotationSets(List<String> annotationSets) {
        this.annotationSets = annotationSets;
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
