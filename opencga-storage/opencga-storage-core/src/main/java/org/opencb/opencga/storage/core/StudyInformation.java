package org.opencb.opencga.storage.core;

import com.google.common.collect.BiMap;

import java.util.List;
import java.util.Map;

/**
 * Created by hpccoll1 on 12/03/15.
 */
public class StudyInformation {

    private int studyId;
    private String studyName;

    private BiMap<Integer, String> fileIds;
    private BiMap<Integer, String> sampleIds;
    private BiMap<Integer, String> cohortIds;
    private Map<Integer, List<Integer>> cohorts;

    public StudyInformation() {
    }

    public StudyInformation(int studyId, String studyName, BiMap<Integer, String> fileIds,
                            BiMap<Integer, String> sampleIds, BiMap<Integer, String> cohortIds,
                            Map<Integer, List<Integer>> cohorts) {
        this.studyId = studyId;
        this.studyName = studyName;
        this.fileIds = fileIds;
        this.sampleIds = sampleIds;
        this.cohortIds = cohortIds;
        this.cohorts = cohorts;
    }

    @Override
    public String toString() {
        return "StudyInformation{" +
                "studyId=" + studyId +
                ", studyName='" + studyName + '\'' +
                ", fileIds=" + fileIds +
                ", sampleIds=" + sampleIds +
                ", cohortIds=" + cohortIds +
                ", cohorts=" + cohorts +
                '}';
    }

    public int getStudyId() {
        return studyId;
    }

    public void setStudyId(int studyId) {
        this.studyId = studyId;
    }

    public String getStudyName() {
        return studyName;
    }

    public void setStudyName(String studyName) {
        this.studyName = studyName;
    }

    public BiMap<Integer, String> getFileIds() {
        return fileIds;
    }

    public void setFileIds(BiMap<Integer, String> fileIds) {
        this.fileIds = fileIds;
    }

    public BiMap<Integer, String> getSampleIds() {
        return sampleIds;
    }

    public void setSampleIds(BiMap<Integer, String> sampleIds) {
        this.sampleIds = sampleIds;
    }

    public BiMap<Integer, String> getCohortIds() {
        return cohortIds;
    }

    public void setCohortIds(BiMap<Integer, String> cohortIds) {
        this.cohortIds = cohortIds;
    }

    public Map<Integer, List<Integer>> getCohorts() {
        return cohorts;
    }

    public void setCohorts(Map<Integer, List<Integer>> cohorts) {
        this.cohorts = cohorts;
    }
}
