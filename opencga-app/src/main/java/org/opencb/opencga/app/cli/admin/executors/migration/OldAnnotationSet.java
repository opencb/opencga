package org.opencb.opencga.app.cli.admin.executors.migration;

import org.opencb.opencga.core.models.Annotation;

import java.util.List;
import java.util.Map;

public class OldAnnotationSet {

    private String name;
    private long variableSetId;
    private List<Annotation> annotations;
    private String creationDate;
    private int release;
    private Map<String, Object> attributes;

    public OldAnnotationSet() {
    }

    public OldAnnotationSet(String name, long variableSetId, List<Annotation> annotations, String creationDate, int release,
                         Map<String, Object> attributes) {
        this.name = name;
        this.variableSetId = variableSetId;
        this.annotations = annotations;
        this.creationDate = creationDate;
        this.release = release;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OldAnnotationSet{");
        sb.append("name='").append(name).append('\'');
        sb.append(", variableSetId=").append(variableSetId);
        sb.append(", annotations=").append(annotations);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", release=").append(release);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public OldAnnotationSet setName(String name) {
        this.name = name;
        return this;
    }

    public long getVariableSetId() {
        return variableSetId;
    }

    public OldAnnotationSet setVariableSetId(long variableSetId) {
        this.variableSetId = variableSetId;
        return this;
    }

    public List<Annotation> getAnnotations() {
        return annotations;
    }

    public OldAnnotationSet setAnnotations(List<Annotation> annotations) {
        this.annotations = annotations;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public OldAnnotationSet setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public OldAnnotationSet setRelease(int release) {
        this.release = release;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public OldAnnotationSet setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
    
}
