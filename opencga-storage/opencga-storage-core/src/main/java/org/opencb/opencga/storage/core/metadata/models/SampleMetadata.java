package org.opencb.opencga.storage.core.metadata.models;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleMetadata extends StudyResourceMetadata<SampleMetadata> {

    private Set<Integer> files;
    private Set<Integer> cohorts;
    // Prepared to have more than one secondary index per sample.
    // Currently only one is allowed.
    private Set<Integer> secondaryIndexCohorts;

    public SampleMetadata() {
        files = new HashSet<>();
        cohorts = new HashSet<>();
        secondaryIndexCohorts = new HashSet<>();
    }

    public SampleMetadata(int studyId, int id, String name) {
        super(studyId, id, name);
        files = new HashSet<>();
        cohorts = new HashSet<>();
        secondaryIndexCohorts = new HashSet<>();
    }

    public Set<Integer> getFiles() {
        return files;
    }

    public SampleMetadata setFiles(Set<Integer> files) {
        this.files = files;
        return this;
    }

    public Set<Integer> getCohorts() {
        return cohorts;
    }

    public SampleMetadata setCohorts(Set<Integer> cohorts) {
        this.cohorts = cohorts;
        return this;
    }

    public SampleMetadata addCohort(int cohortId) {
        this.cohorts.add(cohortId);
        return this;
    }

    public Set<Integer> getSecondaryIndexCohorts() {
        return secondaryIndexCohorts;
    }

    public Integer getSecondaryIndexCohort() {
        return secondaryIndexCohorts.isEmpty() ? null : secondaryIndexCohorts.iterator().next();
    }

    public SampleMetadata setSecondaryIndexCohorts(Set<Integer> secondaryIndexCohorts) {
        this.secondaryIndexCohorts = secondaryIndexCohorts;
        return this;
    }

    public SampleMetadata addSecondaryIndexCohort(int cohortId) {
        this.secondaryIndexCohorts.add(cohortId);
        return this;
    }

    public TaskMetadata.Status getIndexStatus() {
        return getStatus("index");
    }

    public SampleMetadata setIndexStatus(TaskMetadata.Status indexStatus) {
        return setStatus("index", indexStatus);
    }

    public boolean isIndexed() {
        return isReady("index");
    }

    public boolean isAnnotated() {
        return TaskMetadata.Status.READY.equals(getAnnotationStatus());
    }

    public TaskMetadata.Status getAnnotationStatus() {
        return getStatus("annotation");
    }

    public SampleMetadata setAnnotationStatus(TaskMetadata.Status annotationStatus) {
        return setStatus("annotation", annotationStatus);
    }

    public SampleMetadata setMendelianErrorStatus(TaskMetadata.Status mendelianErrorStatus) {
        return setStatus("mendelian_error", mendelianErrorStatus);
    }

    public TaskMetadata.Status getMendelianErrorStatus() {
        return getStatus("mendelian_error");
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("studyId", getStudyId())
                .append("id", getId())
                .append("name", getName())
                .append("status", getStatus())
                .append("files", files)
//                .append("cohorts", cohorts)
                .toString();
    }
}
