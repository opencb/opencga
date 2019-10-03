package org.opencb.opencga.storage.core.metadata.models;

public class VariantScoreMetadata extends StudyResourceMetadata<VariantScoreMetadata> {

    private String description;
    private int cohortId1;
    private Integer cohortId2; //optional

    public VariantScoreMetadata() {
    }

    public VariantScoreMetadata(int studyId, int id, String name, String description, int cohortId1, Integer cohortId2) {
        super(studyId, id, name);
        this.description = description;
        this.cohortId1 = cohortId1;
        this.cohortId2 = cohortId2;
    }

    public String getDescription() {
        return description;
    }

    public VariantScoreMetadata setDescription(String description) {
        this.description = description;
        return this;
    }

    public int getCohortId1() {
        return cohortId1;
    }

    public VariantScoreMetadata setCohortId1(int cohortId1) {
        this.cohortId1 = cohortId1;
        return this;
    }

    public Integer getCohortId2() {
        return cohortId2;
    }

    public VariantScoreMetadata setCohortId2(Integer cohortId2) {
        this.cohortId2 = cohortId2;
        return this;
    }

    public TaskMetadata.Status getIndexStatus() {
        return getStatus("index");
    }

    public VariantScoreMetadata setIndexStatus(TaskMetadata.Status indexStatus) {
        return setStatus("index", indexStatus);
    }

}
