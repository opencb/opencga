package org.opencb.opencga.storage.core.metadata.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.LinkedHashSet;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileMetadata extends StudyResourceMetadata<FileMetadata> {

    public static final String VIRTUAL_PARENT = "virtualParent";
    public static final String VIRTUAL_FILES = "virtualFiles";

    private String path;
    private LinkedHashSet<Integer> samples;
    private Type type = Type.NORMAL;

    public enum Type {
        NORMAL,
        PARTIAL,
        VIRTUAL;
    }

//    private VariantFileMetadata variantFileMetadata;

//    private TaskMetadata.Status indexStatus;
//    private TaskMetadata.Status annotationStatus;

    public FileMetadata() {
    }

    public FileMetadata(int studyId, int id, String name) {
        super(studyId, id, name);
        this.path = name;
    }

    public String getPath() {
        return path;
    }

    public FileMetadata setPath(String path) {
        this.path = path;
        return this;
    }

    public LinkedHashSet<Integer> getSamples() {
        return samples;
    }

    public FileMetadata setSamples(LinkedHashSet<Integer> samples) {
        this.samples = samples;
        return this;
    }

    public Type getType() {
        return type;
    }

    public FileMetadata setType(Type type) {
        this.type = type;
        return this;
    }

//    public VariantFileMetadata getVariantFileMetadata() {
//        return variantFileMetadata;
//    }
//
//    public FileMetadata setVariantFileMetadata(VariantFileMetadata variantFileMetadata) {
//        this.variantFileMetadata = variantFileMetadata;
//        return this;
//    }

    @JsonIgnore
    public boolean isIndexed() {
        return TaskMetadata.Status.READY.equals(getIndexStatus());
    }

    @JsonIgnore
    public TaskMetadata.Status getIndexStatus() {
        return getStatus("index");
    }

    @JsonIgnore
    public FileMetadata setIndexStatus(TaskMetadata.Status indexStatus) {
        return setStatus("index", indexStatus);
    }

    @JsonIgnore
    public boolean isAnnotated() {
        return TaskMetadata.Status.READY.equals(getAnnotationStatus());
    }

    @JsonIgnore
    public TaskMetadata.Status getAnnotationStatus() {
        return getStatus("annotation");
    }

    @JsonIgnore
    public FileMetadata setAnnotationStatus(TaskMetadata.Status annotationStatus) {
        return setStatus("annotation", annotationStatus);
    }

    @JsonIgnore
    public TaskMetadata.Status getSecondaryIndexStatus() {
        return getStatus("secondaryIndex");
    }
    @JsonIgnore
    public FileMetadata setSecondaryIndexStatus(TaskMetadata.Status annotationStatus) {
        return setStatus("secondaryIndex", annotationStatus);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("studyId", getStudyId())
                .append("id", getId())
                .append("name", getName())
                .append("status", getStatus())
                .append("path", path)
                .append("samples", samples)
//                .append("variantFileMetadata", variantFileMetadata)
                .toString();
    }
}
