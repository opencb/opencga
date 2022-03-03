package org.opencb.opencga.storage.core.metadata.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.*;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleMetadata extends StudyResourceMetadata<SampleMetadata> {

    private List<Integer> files;
    private Set<Integer> cohorts;
    // Prepared to have more than one secondary index per sample.
    // Currently only one is allowed.
    private Set<Integer> secondaryIndexCohorts;

    private VariantStorageEngine.SplitData splitData;

    private Integer father;
    private Integer mother;

    private SampleVariantStats stats;


    private static final String SAMPLE_INDEX_STATUS_PREFIX = "sampleIndexGenotypes_";
    private static final String SAMPLE_INDEX_VERSIONS = "sampleIndexGenotypesReadyVersions";
    private static final String SAMPLE_INDEX_ANNOTATION_STATUS_PREFIX = "sampleIndexAnnotation_";
    private static final String SAMPLE_INDEX_ANNOTATION_VERSIONS = "sampleIndexAnnotationReadyVersions";


    public SampleMetadata() {
        files = new ArrayList<>(1);
        cohorts = new HashSet<>();
        secondaryIndexCohorts = new HashSet<>();
    }

    public SampleMetadata(int studyId, int id, String name) {
        super(studyId, id, name);
        files = new ArrayList<>(1);
        cohorts = new HashSet<>();
        secondaryIndexCohorts = new HashSet<>();
    }

    public List<Integer> getFiles() {
        return files;
    }

    public SampleMetadata setFiles(List<Integer> files) {
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

    public boolean isMultiFileSample() {
        return VariantStorageEngine.SplitData.MULTI.equals(splitData);
    }

    public VariantStorageEngine.SplitData getSplitData() {
        return splitData;
    }

    public SampleMetadata setSplitData(VariantStorageEngine.SplitData splitData) {
        this.splitData = splitData;
        return this;
    }

    public Integer getFather() {
        return father;
    }

    public SampleMetadata setFather(Integer father) {
        this.father = father;
        return this;
    }

    public Integer getMother() {
        return mother;
    }

    public SampleMetadata setMother(Integer mother) {
        this.mother = mother;
        return this;
    }

    public SampleVariantStats getStats() {
        return stats;
    }

    public SampleMetadata setStats(SampleVariantStats stats) {
        this.stats = stats;
        return this;
    }

    @JsonIgnore
    public TaskMetadata.Status getIndexStatus() {
        return getStatus("index");
    }

    @JsonIgnore
    public SampleMetadata setIndexStatus(TaskMetadata.Status indexStatus) {
        return setStatus("index", indexStatus);
    }

    @JsonIgnore
    public boolean isIndexed() {
        return isReady("index");
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
    public SampleMetadata setAnnotationStatus(TaskMetadata.Status annotationStatus) {
        return setStatus("annotation", annotationStatus);
    }

    @JsonIgnore
    public TaskMetadata.Status getSampleIndexAnnotationStatus(int sampleIndexVersion) {
        return this.getStatus(SAMPLE_INDEX_ANNOTATION_STATUS_PREFIX + sampleIndexVersion);
    }

    @JsonIgnore
    public List<Integer> getSampleIndexAnnotationVersions() {
        return this.getAttributes().getAsIntegerList(SAMPLE_INDEX_ANNOTATION_VERSIONS);
    }

    @JsonIgnore
    public Integer getSampleIndexAnnotationVersion() {
        List<Integer> versions = getSampleIndexAnnotationVersions();
        if (versions.isEmpty()) {
            return null;
        } else {
            return versions.get(versions.size() - 1);
        }
    }

    @JsonIgnore
    public SampleMetadata setSampleIndexAnnotationStatus(TaskMetadata.Status status, int version) {
        ObjectMap attributes = this.getAttributes();
        if (status == TaskMetadata.Status.READY) {
            List<Integer> versions = new ArrayList<>(getSampleIndexAnnotationVersions());
            if (!versions.contains(version)) {
                versions.add(version);
                versions.sort(Integer::compareTo);
                attributes.put(SAMPLE_INDEX_ANNOTATION_VERSIONS, versions);
            }
        }
        this.setStatus(SAMPLE_INDEX_ANNOTATION_STATUS_PREFIX + version, status);
        return this;
    }

    @JsonIgnore
    public TaskMetadata.Status getSampleIndexStatus(int sampleIndexVersion) {
        return this.getStatus(SAMPLE_INDEX_STATUS_PREFIX + sampleIndexVersion);
    }

    @JsonIgnore
    public List<Integer> getSampleIndexVersions() {
        return this.getAttributes().getAsIntegerList(SAMPLE_INDEX_VERSIONS);
    }

    @JsonIgnore
    public Integer getSampleIndexVersion() {
        List<Integer> versions = getSampleIndexVersions();
        if (versions.isEmpty()) {
            return null;
        } else {
            return versions.get(versions.size() - 1);
        }
    }

    @JsonIgnore
    public SampleMetadata setSampleIndexStatus(TaskMetadata.Status status, int version) {
        ObjectMap attributes = this.getAttributes();
        if (status == TaskMetadata.Status.READY) {
            List<Integer> versions = new ArrayList<>(getSampleIndexVersions());
            if (!versions.contains(version)) {
                versions.add(version);
                versions.sort(Integer::compareTo);
                attributes.put(SAMPLE_INDEX_VERSIONS, versions);
            }
        }
        this.setStatus(SAMPLE_INDEX_STATUS_PREFIX + version, status);
        return this;
    }

    @JsonIgnore
    public SampleMetadata setMendelianErrorStatus(TaskMetadata.Status mendelianErrorStatus) {
        return setStatus("mendelian_error", mendelianErrorStatus);
    }

    @JsonIgnore
    public TaskMetadata.Status getMendelianErrorStatus() {
        return getStatus("mendelian_error");
    }

    @JsonIgnore
    public SampleMetadata setFamilyIndexStatus(TaskMetadata.Status familyIndexStatus) {
        return setStatus("family_index", familyIndexStatus);
    }

    @JsonIgnore
    public TaskMetadata.Status getFamilyIndexStatus() {
        return getStatus("family_index");
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("studyId", getStudyId())
                .append("id", getId())
                .append("name", getName())
                .append("status", getStatus())
                .append("files", files)
                .append("stats", stats)
                .append("cohorts", cohorts)
                .append("attributes", getAttributes().toJson())
                .toString();
    }
}
