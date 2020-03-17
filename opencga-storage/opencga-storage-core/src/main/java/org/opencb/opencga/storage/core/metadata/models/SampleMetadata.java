package org.opencb.opencga.storage.core.metadata.models;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
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

    private List<Integer> files;
    private Set<Integer> cohorts;
    // Prepared to have more than one secondary index per sample.
    // Currently only one is allowed.
    private Set<Integer> secondaryIndexCohorts;

    private VariantStorageEngine.LoadSplitData splitData;

    private Integer father;
    private Integer mother;

    private SampleVariantStats stats;

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
        return VariantStorageEngine.LoadSplitData.MULTI.equals(splitData);
    }

    public VariantStorageEngine.LoadSplitData getSplitData() {
        return splitData;
    }

    public SampleMetadata setSplitData(VariantStorageEngine.LoadSplitData splitData) {
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

    public SampleMetadata setFamilyIndexStatus(TaskMetadata.Status familyIndexStatus) {
        return setStatus("family_index", familyIndexStatus);
    }

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
