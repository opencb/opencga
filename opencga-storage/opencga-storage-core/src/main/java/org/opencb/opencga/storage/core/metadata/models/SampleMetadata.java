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


    private static final String SAMPLE_INDEX_STATUS = "sampleIndexGenotypes";
    private static final String SAMPLE_INDEX_VERSION = "sampleIndexGenotypesVersion";
    private static final String SAMPLE_INDEX_VERSION_ARCHIVE = "sampleIndexGenotypesVersionArchive";
    private static final String SAMPLE_INDEX_ANNOTATION_STATUS = "sampleIndexAnnotation";
    private static final String SAMPLE_INDEX_ANNOTATION_VERSION = "sampleIndexAnnotationVersion";
    private static final String SAMPLE_INDEX_ANNOTATION_VERSION_ARCHIVE = "sampleIndexAnnotationVersionArchive";

    @Deprecated // Deprecated to avoid confusion with actual "SAMPLE_INDEX_STATUS"
    private static final String SAMPLE_INDEX_ANNOTATION_STATUS_OLD = "sampleIndex";


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
    private TaskMetadata.Status getSampleIndexAnnotationStatus() {
        TaskMetadata.Status status = this.getStatus(SAMPLE_INDEX_ANNOTATION_STATUS, null);
        if (status == null) {
            // The status name was renamed. In case of missing value (null), check for the deprecated value.
            status = this.getStatus(SAMPLE_INDEX_ANNOTATION_STATUS_OLD);
        }
        return status;
    }

    @JsonIgnore
    public TaskMetadata.Status getSampleIndexAnnotationStatus(int latestSampleIndexVersion) {
        TaskMetadata.Status status = getSampleIndexAnnotationStatus();
        if (status == TaskMetadata.Status.READY) {
            int actualSampleIndexVersion = getSampleIndexAnnotationVersion();
            if (actualSampleIndexVersion != latestSampleIndexVersion) {
//                logger.debug("Sample index annotation version outdated. Actual : " + actualSampleIndexVersion
//                        + " , expected : " + latestSampleIndexVersion);
                status = TaskMetadata.Status.NONE;
            }
        }
        return status;
    }

    @JsonIgnore
    public List<Integer> getSampleIndexAnnotationVersions() {
        ObjectMap attributes = this.getAttributes();

        List<Integer> archivedVersions = new LinkedList<>(attributes.getAsIntegerList(SAMPLE_INDEX_ANNOTATION_VERSION_ARCHIVE));
        Integer current = getSampleIndexAnnotationVersion();
        if (current != null) {
            archivedVersions.add(getSampleIndexAnnotationVersion());
        }
        return archivedVersions;
    }

    @JsonIgnore
    public Integer getSampleIndexAnnotationVersion() {
        int current = this.getAttributes().getInt(SAMPLE_INDEX_ANNOTATION_VERSION, -1);
        if (current == -1) {
            TaskMetadata.Status status = getSampleIndexAnnotationStatus();
            if (status == TaskMetadata.Status.READY) {
                // If status READY, and missing version, assume default version
                return StudyMetadata.DEFAULT_SAMPLE_INDEX_VERSION;
            } else {
                return null;
            }
        } else {
            return current;
        }
    }

    @JsonIgnore
    public SampleMetadata setSampleIndexAnnotationStatus(TaskMetadata.Status status, int version) {
        ObjectMap attributes = this.getAttributes();
        if (status == TaskMetadata.Status.READY) {
            Integer oldVersion = getSampleIndexAnnotationVersion();
            if (oldVersion != null && oldVersion != version) {
                Set<Integer> archivedVersions = new LinkedHashSet<>(attributes.getAsIntegerList(SAMPLE_INDEX_ANNOTATION_VERSION_ARCHIVE));
                archivedVersions.add(oldVersion);
                attributes.put(SAMPLE_INDEX_ANNOTATION_VERSION_ARCHIVE, archivedVersions);
            }
        }

        // Remove deprecated value.
        this.getStatus().remove(SAMPLE_INDEX_ANNOTATION_STATUS_OLD);
        this.setStatus(SAMPLE_INDEX_ANNOTATION_STATUS, status);
        attributes.put(SAMPLE_INDEX_ANNOTATION_VERSION, version);
        return this;
    }

    @JsonIgnore
    private TaskMetadata.Status getSampleIndexStatus() {
        TaskMetadata.Status status = this.getStatus(SAMPLE_INDEX_STATUS, null);
        if (status == null) {
            // This is a new status. In case of missing value (null), assume it's READY
            status = TaskMetadata.Status.READY;
        }
        return status;
    }

    @JsonIgnore
    public boolean hasSampleIndexStatus() {
        return getStatus(SAMPLE_INDEX_STATUS, null) != null && getAttributes().containsKey(SAMPLE_INDEX_VERSION);
    }

    @JsonIgnore
    public TaskMetadata.Status getSampleIndexStatus(int latestSampleIndexVersion) {
        TaskMetadata.Status status = getSampleIndexStatus();
        if (status == TaskMetadata.Status.READY) {
            int actualSampleIndexVersion = getSampleIndexVersion();
            if (actualSampleIndexVersion != latestSampleIndexVersion) {
//                logger.debug("Sample index version outdated. Actual : " + actualSampleIndexVersion
//                        + " , expected : " + latestSampleIndexVersion);
                status = TaskMetadata.Status.NONE;
            }
        }
        return status;
    }

    @JsonIgnore
    public List<Integer> getSampleIndexVersions() {
        ObjectMap attributes = this.getAttributes();

        List<Integer> archivedVersions = new LinkedList<>(attributes.getAsIntegerList(SAMPLE_INDEX_VERSION_ARCHIVE));
        Integer current = getSampleIndexVersion();
        if (current != null) {
            archivedVersions.add(current);
        }
        return archivedVersions;
    }

    @JsonIgnore
    public Integer getSampleIndexVersion() {
        int version = this.getAttributes().getInt(SAMPLE_INDEX_VERSION, -1);
        if (version == -1) {
            TaskMetadata.Status status = getSampleIndexStatus();
            if (status == TaskMetadata.Status.READY) {
                // If status READY, and missing version, assume default version
                return StudyMetadata.DEFAULT_SAMPLE_INDEX_VERSION;
            } else {
                return null;
            }
        } else {
            return version;
        }
    }

    @JsonIgnore
    public SampleMetadata setSampleIndexStatus(TaskMetadata.Status status, int version) {
        if (status == TaskMetadata.Status.READY) {
            Integer oldVersion = getSampleIndexVersion();
            if (oldVersion != null && oldVersion != version) {
                List<Integer> archivedVersions = new ArrayList<>(this.getAttributes().getAsIntegerList(SAMPLE_INDEX_VERSION_ARCHIVE));
                archivedVersions.add(oldVersion);
                this.getAttributes().put(SAMPLE_INDEX_VERSION_ARCHIVE, archivedVersions);
            }
        }
        this.getAttributes().put(SAMPLE_INDEX_VERSION, version);
        return this.setStatus(SAMPLE_INDEX_STATUS, status);
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
//                .append("cohorts", cohorts)
                .toString();
    }
}
