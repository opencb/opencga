package org.opencb.opencga.storage.core.metadata.local;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class LocalMetadataStorage {

    private final ProjectMetadata projectMetadata;
    private final Map<Integer, StudyMetadataStore> studyMetadataMap;
    private final Map<String, Integer> studyNameIdMap;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public LocalMetadataStorage(@JsonProperty("projectMetadata") ProjectMetadata projectMetadata) {
        this.projectMetadata = projectMetadata;
        this.studyMetadataMap = new HashMap<>();
        this.studyNameIdMap = new HashMap<>();
    }

    protected LocalMetadataStorage(ProjectMetadata projectMetadata, Map<Integer, StudyMetadataStore> studyMetadataMap) {
        this.projectMetadata = projectMetadata;
        this.studyMetadataMap = studyMetadataMap;
        this.studyNameIdMap = new HashMap<>(studyMetadataMap.size());
    }

    // Class that holds all the metadata for a study. Sampeles, files, cohorts, etc.
    public static class StudyMetadataStore {
        private final StudyMetadata studyMetadata;
        private final Map<Integer, SampleMetadata> sampleMetadataMap;
        private final Map<String, Integer> sampleNameIdMap;
        private final Map<Integer, FileMetadata> fileMetadataMap;
        private final Map<String, Integer> fileNameIdMap;
        private final Map<Integer, CohortMetadata> cohortMetadataMap;
        private final Map<String, Integer> cohortNameIdMap;
        private final Map<Integer, TaskMetadata> taskMetadataMap;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public StudyMetadataStore(@JsonProperty("studyMetadata") StudyMetadata studyMetadata) {
            this.studyMetadata = studyMetadata;
            sampleMetadataMap = new LinkedHashMap<>();
            sampleNameIdMap = new HashMap<>();
            fileMetadataMap = new LinkedHashMap<>();
            fileNameIdMap = new HashMap<>();
            cohortMetadataMap = new LinkedHashMap<>();
            cohortNameIdMap = new HashMap<>();
            taskMetadataMap = new LinkedHashMap<>();
        }

        public StudyMetadataStore(StudyMetadata studyMetadata,
                                  Map<Integer, SampleMetadata> sampleMetadataMap,
                                  Map<String, Integer> sampleNameIdMap,
                                  Map<Integer, FileMetadata> fileMetadataMap,
                                  Map<String, Integer> fileNameIdMap,
                                  Map<Integer, CohortMetadata> cohortMetadataMap,
                                  Map<String, Integer> cohortNameIdMap,
                                  Map<Integer, TaskMetadata> taskMetadataMap) {
            this.studyMetadata = studyMetadata;
            this.sampleMetadataMap = sampleMetadataMap;
            this.sampleNameIdMap = sampleNameIdMap;
            this.fileMetadataMap = fileMetadataMap;
            this.fileNameIdMap = fileNameIdMap;
            this.cohortMetadataMap = cohortMetadataMap;
            this.cohortNameIdMap = cohortNameIdMap;
            this.taskMetadataMap = taskMetadataMap;
        }

        public StudyMetadata getStudyMetadata() {
            return studyMetadata;
        }

        public void addSample(SampleMetadata sampleMetadata) {
            sampleMetadataMap.put(sampleMetadata.getId(), sampleMetadata);
            sampleNameIdMap.put(sampleMetadata.getName(), sampleMetadata.getId());
        }

        public Map<Integer, SampleMetadata> getSamples() {
            return sampleMetadataMap;
        }

        public Map<String, Integer> getSampleNameIdsMap() {
            return sampleNameIdMap;
        }

        public void addFile(FileMetadata fileMetadata) {
            fileMetadataMap.put(fileMetadata.getId(), fileMetadata);
            fileNameIdMap.put(fileMetadata.getName(), fileMetadata.getId());
        }

        public Map<Integer, FileMetadata> getFiles() {
            return fileMetadataMap;
        }

        public Map<String, Integer> getFileNameIdsMap() {
            return fileNameIdMap;
        }

        public void addCohort(CohortMetadata cohortMetadata) {
            cohortMetadataMap.put(cohortMetadata.getId(), cohortMetadata);
            cohortNameIdMap.put(cohortMetadata.getName(), cohortMetadata.getId());
        }

        public Map<Integer, CohortMetadata> getCohorts() {
            return cohortMetadataMap;
        }

        public Map<String, Integer> getCohortNameIdsMap() {
            return cohortNameIdMap;
        }

        public Map<Integer, TaskMetadata> getTasks() {
            return taskMetadataMap;
        }
    }

    public ProjectMetadata getProjectMetadata() {
        return projectMetadata;
    }

    public void addStudy(StudyMetadataStore studyMetadataStore) {
        studyMetadataMap.put(studyMetadataStore.getStudyMetadata().getId(), studyMetadataStore);
        studyNameIdMap.put(studyMetadataStore.getStudyMetadata().getName(), studyMetadataStore.getStudyMetadata().getId());
    }

    public Map<Integer, StudyMetadataStore> getStudies() {
        return studyMetadataMap;
    }

    public Map<String, Integer> getStudyNameIdsMap() {
        return studyNameIdMap;
    }

    public StudyMetadataStore getStudy(int studyId) {
        StudyMetadataStore studyMetadataStore = studyMetadataMap.get(studyId);
        if (studyMetadataStore == null) {
            throw VariantQueryException.studyNotFound(studyId);
        }
        return studyMetadataStore;
    }

    public StudyMetadataStore getStudy(String studyName) {
        Integer i = getStudyNameIdsMap().get(studyName);
        if (i == null) {
            throw VariantQueryException.studyNotFound(studyName);
        }
        return getStudy(i);
    }
}
