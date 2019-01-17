package org.opencb.opencga.storage.core.metadata.models;

import org.opencb.biodata.models.variant.VariantFileMetadata;

import java.util.LinkedHashSet;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileMetadata {

    private int studyId;
    private int id;
    private String name;
    private String path;
    private LinkedHashSet<Integer> samples;

    private VariantFileMetadata variantFileMetadata;

    private BatchFileTask.Status indexStatus;
    private BatchFileTask.Status annotationStatus;

    public FileMetadata() {
    }

    public FileMetadata(int studyId, int id, String name) {
        this.studyId = studyId;
        this.id = id;
        this.name = name;
        this.path = name;
    }

    public int getStudyId() {
        return studyId;
    }

    public FileMetadata setStudyId(int studyId) {
        this.studyId = studyId;
        return this;
    }

    public int getId() {
        return id;
    }

    public FileMetadata setId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public FileMetadata setName(String name) {
        this.name = name;
        return this;
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

    public VariantFileMetadata getVariantFileMetadata() {
        return variantFileMetadata;
    }

    public FileMetadata setVariantFileMetadata(VariantFileMetadata variantFileMetadata) {
        this.variantFileMetadata = variantFileMetadata;
        return this;
    }

    public boolean isIndexed() {
        return BatchFileTask.Status.READY.equals(getIndexStatus());
    }

    public BatchFileTask.Status getIndexStatus() {
        return indexStatus;
    }

    public FileMetadata setIndexStatus(BatchFileTask.Status indexStatus) {
        this.indexStatus = indexStatus;
        return this;
    }

    public BatchFileTask.Status getAnnotationStatus() {
        return annotationStatus;
    }

    public FileMetadata setAnnotationStatus(BatchFileTask.Status annotationStatus) {
        this.annotationStatus = annotationStatus;
        return this;
    }
}
