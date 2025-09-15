package org.opencb.opencga.storage.core.metadata.models;

import java.util.List;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CohortMetadata extends StudyResourceMetadata<CohortMetadata> {

    public static final String INVALID_STATS_NUM_SAMPLES = "invalidStatsNumSamples";
    public static final String SECONDARY_INDEX_STATUS = "secondaryIndex";
    public static final String AGGREGATE_FAMILY_STATUS = "aggregateFamily";
    public static final String SECONDARY_INDEX_PREFIX = "__SECONDARY_INDEX_COHORT_";
    public static final String AGGREGATE_FAMILY_PREFIX = "__AGGREGATE_FAMILY_COHORT_";

//    private int studyId;
//    private int id;
//    private String name;

    private List<Integer> samples;
    private List<Integer> files;

//    private TaskMetadata.Status status = TaskMetadata.Status.NONE;

    public CohortMetadata() {
    }

    public CohortMetadata(int studyId, int id, String name, List<Integer> samples, List<Integer> files) {
        super(studyId, id, name);
        this.samples = samples;
        this.files = files;
    }

    public List<Integer> getSamples() {
        return samples;
    }

    public CohortMetadata setSamples(List<Integer> samples) {
        this.samples = samples;
        return this;
    }

    public List<Integer> getFiles() {
        return files;
    }

    public CohortMetadata setFiles(List<Integer> files) {
        this.files = files;
        return this;
    }

    public TaskMetadata.Status getStatsStatus() {
        return getStatus("stats");
    }

    public CohortMetadata setStatsStatus(TaskMetadata.Status status) {
        return setStatus("stats", status);
    }

    public boolean isStatsReady() {
        return isReady("stats");
    }

    public boolean isInvalid() {
        return isError("stats");
    }

    public CohortMetadata setInvalidStats() {
        setStatsStatus(TaskMetadata.Status.ERROR);
        return this;
    }

    public TaskMetadata.Status getStatusByType() {
        switch (getType()) {
            case SECONDARY_INDEX:
                return getSecondaryIndexStatus();
            case AGGREGATE_FAMILY:
                return getAggregateFamilyStatus();
            case USER_DEFINED:
                return getStatsStatus();
            default:
                throw new IllegalArgumentException("Unknown cohort type: " + getType());
        }
    }

    public CohortMetadata setStatusByType(TaskMetadata.Status status) {
        switch (getType()) {
            case SECONDARY_INDEX:
                return setSecondaryIndexStatus(status);
            case AGGREGATE_FAMILY:
                return setAggregateFamilyStatus(status);
            case USER_DEFINED:
                return setStatsStatus(status);
            default:
                throw new IllegalArgumentException("Unknown cohort type: " + getType());
        }
    }

    public TaskMetadata.Status getSecondaryIndexStatus() {
        return getStatus(SECONDARY_INDEX_STATUS);
    }

    public CohortMetadata setSecondaryIndexStatus(TaskMetadata.Status status) {
        return setStatus(SECONDARY_INDEX_STATUS, status);
    }

    public TaskMetadata.Status getAggregateFamilyStatus() {
        return getStatus(AGGREGATE_FAMILY_STATUS);
    }

    public CohortMetadata setAggregateFamilyStatus(TaskMetadata.Status status) {
        return setStatus(AGGREGATE_FAMILY_STATUS, status);
    }

    public Type getType() {
        return getType(getName());
    }

    public static Type getType(String name) {
        if (name.startsWith(SECONDARY_INDEX_PREFIX)) {
            return Type.SECONDARY_INDEX;
        } else if (name.startsWith(AGGREGATE_FAMILY_PREFIX)) {
            return Type.AGGREGATE_FAMILY;
        } else {
            return Type.USER_DEFINED;
        }
    }

    public enum Type {
        // Standard user defined cohort for variant stats
        USER_DEFINED,
        // Cohort created by the secondary index.
        SECONDARY_INDEX,
        // Cohort created by the aggregate family.
        AGGREGATE_FAMILY
    }

}
