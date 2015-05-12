/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class StudyConfiguration {

    private int studyId;
    private String studyName;

    private Map<String, Integer> fileIds;
    private Map<String, Integer> sampleIds;
    private Map<String, Integer> cohortIds;
//    private Map<Integer, String> inverseFileIds;
//    private Map<Integer, String> inverseSampleIds;
//    private Map<Integer, String> inverseCohortIds;
    private Map<Integer, Set<Integer>> cohorts;

    public StudyConfiguration() {
    }

    public StudyConfiguration(int studyId, String studyName) {
        this.studyId = studyId;
        this.studyName = studyName;
        setFileIds(new HashMap<String, Integer>(1));
        setSampleIds(new HashMap<String, Integer>());
        setCohortIds(new HashMap<String, Integer>());
        this.cohorts = new HashMap<>();
    }

    public StudyConfiguration(int studyId, String studyName, int fileId, String fileName) {
        this.studyId = studyId;
        this.studyName = studyName;
        setFileIds(new HashMap<String, Integer>(1));
        fileIds.put(fileName, fileId);
        setSampleIds(new HashMap<String, Integer>());
        setCohortIds(new HashMap<String, Integer>());
        this.cohorts = new HashMap<>();
    }

    public StudyConfiguration(int studyId, String studyName, Map<String, Integer> fileIds,
                              Map<String, Integer> sampleIds, Map<String, Integer> cohortIds,
                              Map<Integer, Set<Integer>> cohorts) {
        this.studyId = studyId;
        this.studyName = studyName;
        this.fileIds = fileIds;
        this.sampleIds = sampleIds;
        this.cohortIds = cohortIds;
        this.cohorts = cohorts;
    }


    @Deprecated
    static public StudyConfiguration read(Path path) throws IOException {
        return new ObjectMapper(new JsonFactory()).readValue(path.toFile(), StudyConfiguration.class);
    }

    @Deprecated
    public void write(Path path) throws IOException {
        new ObjectMapper(new JsonFactory()).writerWithDefaultPrettyPrinter().withoutAttribute("inverseFileIds").writeValue(path.toFile(), this);
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

    public Map<String, Integer> getFileIds() {
        return fileIds;
    }

    public void setFileIds(Map<String, Integer> fileIds) {
        this.fileIds = fileIds;
//        inverseFileIds = createInverseMap(fileIds);
    }

    public Map<String, Integer> getSampleIds() {
        return sampleIds;
    }

    public void setSampleIds(Map<String, Integer> sampleIds) {
        this.sampleIds = sampleIds;
//        inverseSampleIds = createInverseMap(sampleIds);
    }

    public Map<String, Integer> getCohortIds() {
        return cohortIds;
    }

    public void setCohortIds(Map<String, Integer> cohortIds) {
        this.cohortIds = cohortIds;
//        inverseCohortIds = createInverseMap(cohortIds);
    }

    public Map<Integer, Set<Integer>> getCohorts() {
        return cohorts;
    }

    public void setCohorts(Map<Integer, Set<Integer>> cohorts) {
        this.cohorts = cohorts;
    }



    public static <T,R> Map<R,T> inverseMap(Map<T, R> map) {
        Map<R,T> inverseMap = new HashMap<>(map.size());
        for (Map.Entry<T, R> entry : map.entrySet()) {
            inverseMap.put(entry.getValue(), entry.getKey());
        }
        if (inverseMap.size() != map.size()) {
            return null;
        }
        return inverseMap;
    }
}
