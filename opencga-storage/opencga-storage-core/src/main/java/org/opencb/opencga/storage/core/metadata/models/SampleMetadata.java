package org.opencb.opencga.storage.core.metadata.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleMetadata extends StudyResourceMetadata<SampleMetadata> {

    /**
     * List of fileIds that contains the sample.
     * This list is sorted by the order files were loaded.
     * Only files of type {@link FileMetadata.Type#NORMAL} and {@link FileMetadata.Type#VIRTUAL} will be present in this list.
     * If the sample was part of a set of {@link FileMetadata.Type#PARTIAL} files, only the
     * {@link FileMetadata.Type#VIRTUAL} file will appear in this list.
     */
    private List<Integer> files;
    private Set<Integer> cohorts;
    // Other cohorts. Should contain all other cohorts.
    private Set<Integer> internalCohorts;
    // Prepared to have more than one secondary index per sample.
    // Currently only one is allowed.
    // This is a deprecated field. New cohort types should be stored at {@link #internalCohorts}.
    @Deprecated
    private Set<Integer> secondaryIndexCohorts;

    private VariantStorageEngine.SplitData splitData;

    private Integer father;
    private Integer mother;

    private SampleVariantStats stats;


    private static final String SAMPLE_INDEX_STATUS_PREFIX = "sampleIndexGenotypes_";
    private static final String SAMPLE_INDEX_VERSIONS = "sampleIndexGenotypesReadyVersions";
    private static final String SAMPLE_INDEX_ANNOTATION_STATUS_PREFIX = "sampleIndexAnnotation_";
    private static final String SAMPLE_INDEX_ANNOTATION_VERSIONS = "sampleIndexAnnotationReadyVersions";
    private static final String FAMILY_INDEX_STATUS_PREFIX = "familyIndex_";
    private static final String FAMILY_INDEX_VERSIONS = "familyIndexReadyVersions";
    private static final String FAMILY_INDEX_DEFINED = "familyIndexDefined";

    public SampleMetadata() {
        files = new ArrayList<>(1);
        cohorts = new HashSet<>();
        secondaryIndexCohorts = new HashSet<>();
        internalCohorts = new HashSet<>();
    }

    public SampleMetadata(int studyId, int id, String name) {
        super(studyId, id, name);
        files = new ArrayList<>(1);
        cohorts = new HashSet<>();
        secondaryIndexCohorts = new HashSet<>();
        internalCohorts = new HashSet<>();
    }

    public List<Integer> getFiles() {
        return files;
    }

    public SampleMetadata setFiles(List<Integer> files) {
        this.files = files;
        return this;
    }

    private Set<Integer> getCohorts(CohortMetadata.Type type) {
        switch (type) {
            case USER_DEFINED:
                return cohorts;
            case SECONDARY_INDEX:
                return secondaryIndexCohorts;
            case AGGREGATE_FAMILY:
                return internalCohorts;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    public boolean containsCohort(CohortMetadata.Type type, int cohortId) {
        return getCohorts(type).contains(cohortId);
    }

    public SampleMetadata addCohort(CohortMetadata.Type type, int cohortId) {
        getCohorts(type).add(cohortId);
        return this;
    }

    public SampleMetadata removeCohort(CohortMetadata.Type type, int cohortId) {
        getCohorts(type).remove(cohortId);
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

    @Deprecated
    public Set<Integer> getSecondaryIndexCohorts() {
        return secondaryIndexCohorts;
    }

    @Deprecated
    public Integer getSecondaryIndexCohort() {
        return secondaryIndexCohorts.isEmpty() ? null : secondaryIndexCohorts.iterator().next();
    }

    @Deprecated
    public SampleMetadata setSecondaryIndexCohorts(Set<Integer> secondaryIndexCohorts) {
        this.secondaryIndexCohorts = secondaryIndexCohorts;
        return this;
    }

    @Deprecated
    public SampleMetadata addSecondaryIndexCohort(int cohortId) {
        this.secondaryIndexCohorts.add(cohortId);
        return this;
    }

    public Set<Integer> getInternalCohorts() {
        return internalCohorts;
    }

    public SampleMetadata setInternalCohorts(Set<Integer> internalCohorts) {
        this.internalCohorts = internalCohorts;
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
    public TaskMetadata.Status getSecondaryAnnotationIndexStatus() {
        return getStatus("secondaryAnnotationIndex");
    }

    @JsonIgnore
    public SampleMetadata setSecondaryAnnotationIndexStatus(TaskMetadata.Status annotationStatus) {
        return setStatus("secondaryAnnotationIndex", annotationStatus);
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
        return getLatestVersion(SAMPLE_INDEX_VERSIONS);
    }

    @JsonIgnore
    public SampleMetadata setSampleIndexStatus(TaskMetadata.Status status, int version) {
        registerVersion(SAMPLE_INDEX_VERSIONS, status, version);
        this.setStatus(SAMPLE_INDEX_STATUS_PREFIX + version, status);
        return this;
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
        return getLatestVersion(SAMPLE_INDEX_ANNOTATION_VERSIONS);
    }

    @JsonIgnore
    public SampleMetadata setSampleIndexAnnotationStatus(TaskMetadata.Status status, int version) {
        registerVersion(SAMPLE_INDEX_ANNOTATION_VERSIONS, status, version);
        this.setStatus(SAMPLE_INDEX_ANNOTATION_STATUS_PREFIX + version, status);
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

    /**
     * @return if the family index was enabled on this sample. It might not be READY.
     */
    @JsonIgnore
    public boolean isFamilyIndexDefined() {
        return getAttributes().getBoolean(FAMILY_INDEX_DEFINED, false);
    }

    @JsonIgnore
    public void setFamilyIndexDefined(boolean defined) {
        getAttributes().put(FAMILY_INDEX_DEFINED, defined);
    }

    @JsonIgnore
    public SampleMetadata setFamilyIndexStatus(TaskMetadata.Status status, int version) {
        if (status == TaskMetadata.Status.READY) {
            // If any family index is ready, set "family index defined"
            setFamilyIndexDefined(true);
        }
        registerVersion(FAMILY_INDEX_VERSIONS, status, version);
        return setStatus(FAMILY_INDEX_STATUS_PREFIX + version, status);
    }

    @JsonIgnore
    public List<Integer> getFamilyIndexVersions() {
        return getAttributes().getAsIntegerList(FAMILY_INDEX_VERSIONS);
    }

    @JsonIgnore
    public Integer getFamilyIndexVersion() {
        return getLatestVersion(FAMILY_INDEX_VERSIONS);
    }

    @JsonIgnore
    public TaskMetadata.Status getFamilyIndexStatus(int version) {
        return getStatus(FAMILY_INDEX_STATUS_PREFIX + version);
    }

    private void registerVersion(String versionsKey, TaskMetadata.Status status, int version) {
        ObjectMap attributes = this.getAttributes();
        List<Integer> versions = attributes.getAsIntegerList(versionsKey);
        if (status == TaskMetadata.Status.READY) {
            if (!versions.contains(version)) {
                versions = new ArrayList<>(versions);
                versions.add(version);
                versions.sort(Integer::compareTo);
                attributes.put(versionsKey, versions);
            }
        } else {
            int idx = versions.indexOf(version);
            if (idx >= 0) {
                versions = new ArrayList<>(versions);
                versions.remove(idx);
                versions.sort(Integer::compareTo);
                attributes.put(versionsKey, versions);
            }
        }
    }

    private Integer getLatestVersion(String key) {
        List<Integer> versions = this.getAttributes().getAsIntegerList(key);
        if (versions.isEmpty()) {
            return null;
        } else {
            return versions.get(versions.size() - 1);
        }
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
