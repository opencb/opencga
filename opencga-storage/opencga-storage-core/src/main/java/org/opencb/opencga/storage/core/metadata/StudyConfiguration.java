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

package org.opencb.opencga.storage.core.metadata;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class StudyConfiguration {

    private int studyId;
    private String studyName;

    private BiMap<String, Integer> fileIds;
    private BiMap<String, Integer> sampleIds;
    private BiMap<String, Integer> cohortIds;
    private Map<Integer, Set<Integer>> cohorts;

    private LinkedHashSet<Integer> indexedFiles;    //Use LinkedHashSet instead of Set to ensure indexing order
    @Deprecated
    /**
     * @deprecated Read from variant source. Use VariantSourceDBAdaptor or similar.
     */
    private Map<Integer, String> headers;
    private Map<Integer, LinkedHashSet<Integer>> samplesInFiles; //Use LinkedHashSet instead of Set to ensure sample order
    private Set<Integer> calculatedStats;
    private Set<Integer> invalidStats;

    private List<BatchFileOperation> batches;

    private VariantSource.Aggregation aggregation;

    private Long timeStamp;

    private ObjectMap attributes;

    protected StudyConfiguration() {
    }

    public StudyConfiguration(StudyConfiguration other) {
        copy(other);
    }

    public void copy(StudyConfiguration other) {
        this.studyId = other.studyId;
        this.studyName = other.studyName;
        this.fileIds = HashBiMap.create(other.fileIds == null ? Collections.emptyMap() : other.fileIds);
        this.sampleIds = HashBiMap.create(other.sampleIds == null ? Collections.emptyMap() : other.sampleIds);
        this.cohortIds = HashBiMap.create(other.cohortIds == null ? Collections.emptyMap() : other.cohortIds);
        this.cohorts = new HashMap<>(other.cohorts);
        this.indexedFiles = new LinkedHashSet<>(other.indexedFiles);
        this.headers = new HashMap<>(other.headers);
        this.samplesInFiles = new HashMap<>(other.samplesInFiles);
        this.calculatedStats = new LinkedHashSet<>(other.calculatedStats);
        this.invalidStats = new LinkedHashSet<>(other.invalidStats);
        this.batches = other.batches;
        this.aggregation = other.aggregation;
        this.attributes = new ObjectMap(other.attributes);
    }

    public StudyConfiguration newInstance() {
        return new StudyConfiguration(this);
    }

    public StudyConfiguration(int studyId, String studyName) {
        this.studyId = studyId;
        this.studyName = studyName;
        this.fileIds = HashBiMap.create(1);
        this.sampleIds = HashBiMap.create();
        this.cohortIds = HashBiMap.create();
        this.cohorts = new HashMap<>();
        this.indexedFiles = new LinkedHashSet<>();
        this.headers = new HashMap<>();
        this.samplesInFiles = new LinkedHashMap<>();
        this.calculatedStats = new LinkedHashSet<>();
        this.invalidStats = new LinkedHashSet<>();
        this.batches = new LinkedList<>();
        this.aggregation = VariantSource.Aggregation.NONE;
        this.attributes = new ObjectMap();
    }

    public StudyConfiguration(int studyId, String studyName, int fileId, String fileName) {
        this(studyId, studyName);
        fileIds.put(fileName, fileId);
    }

    public StudyConfiguration(int studyId, String studyName, Map<String, Integer> fileIds,
                              Map<String, Integer> sampleIds, Map<String, Integer> cohortIds,
                              Map<Integer, Set<Integer>> cohorts) {
        this.studyId = studyId;
        this.studyName = studyName;
        this.fileIds = HashBiMap.create(fileIds == null ? Collections.emptyMap() : fileIds);
        this.sampleIds = HashBiMap.create(sampleIds == null ? Collections.emptyMap() : sampleIds);
        this.cohortIds = HashBiMap.create(cohortIds == null ? Collections.emptyMap() : cohortIds);
        this.cohorts = cohorts;
        this.indexedFiles = new LinkedHashSet<>();
        this.headers = HashBiMap.create();
        this.samplesInFiles = HashBiMap.create();
        this.calculatedStats = new LinkedHashSet<>();
        this.invalidStats = new LinkedHashSet<>();
        this.aggregation = VariantSource.Aggregation.NONE;
        this.attributes = new ObjectMap();
    }


    @Deprecated
    public static StudyConfiguration read(Path path) throws IOException {
        return new ObjectMapper(new JsonFactory()).readValue(path.toFile(), StudyConfiguration.class);
    }

    @Deprecated
    public void write(Path path) throws IOException {
        new ObjectMapper(new JsonFactory()).writerWithDefaultPrettyPrinter().withoutAttribute("inverseFileIds").writeValue(path.toFile(),
                this);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                .append("studyId", studyId)
                .append("studyName", studyName)
                .append("fileIds", fileIds)
                .append("sampleIds", sampleIds)
                .append("cohortIds", cohortIds)
                .append("cohorts", cohorts)
                .append("indexedFiles", indexedFiles)
                .append("headers", headers)
                .append("samplesInFiles", samplesInFiles)
                .append("calculatedStats", calculatedStats)
                .append("invalidStats", invalidStats)
                .append("batches", batches)
                .append("aggregation", aggregation)
                .append("timeStamp", timeStamp)
                .append("attributes", attributes)
                .toString();
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

    public BiMap<String, Integer> getFileIds() {
        return fileIds;
    }

    public void setFileIds(Map<String, Integer> fileIds) {
        this.fileIds = fileIds == null ? null : HashBiMap.create(fileIds);
    }

    public BiMap<String, Integer> getSampleIds() {
        return sampleIds;
    }

    public void setSampleIds(Map<String, Integer> sampleIds) {
        this.sampleIds = sampleIds == null ? null : HashBiMap.create(sampleIds);
    }

    public BiMap<String, Integer> getCohortIds() {
        return cohortIds;
    }

    public void setCohortIds(Map<String, Integer> cohortIds) {
        this.cohortIds = cohortIds == null ? null : HashBiMap.create(cohortIds);
    }

    public Map<Integer, Set<Integer>> getCohorts() {
        return cohorts;
    }

    public void setCohorts(Map<Integer, Set<Integer>> cohorts) {
        this.cohorts = cohorts;
    }

    public LinkedHashSet<Integer> getIndexedFiles() {
        return indexedFiles;
    }

    public void setIndexedFiles(LinkedHashSet<Integer> indexedFiles) {
        this.indexedFiles = indexedFiles;
    }

    public Map<Integer, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<Integer, String> headers) {
        this.headers = headers;
    }

    public Map<Integer, LinkedHashSet<Integer>> getSamplesInFiles() {
        return samplesInFiles;
    }

    public void setSamplesInFiles(Map<Integer, LinkedHashSet<Integer>> samplesInFiles) {
        this.samplesInFiles = samplesInFiles;
    }

    public Set<Integer> getCalculatedStats() {
        return calculatedStats;
    }

    public void setCalculatedStats(Set<Integer> calculatedStats) {
        this.calculatedStats = calculatedStats;
    }

    public Set<Integer> getInvalidStats() {
        return invalidStats;
    }

    public void setInvalidStats(Set<Integer> invalidStats) {
        this.invalidStats = invalidStats;
    }

    public List<BatchFileOperation> getBatches() {
        return batches;
    }

    public StudyConfiguration setBatches(List<BatchFileOperation> batches) {
        this.batches = batches;
        return this;
    }

    public BatchFileOperation lastBatch() {
        return getBatches().get(getBatches().size() - 1);
    }

    public VariantSource.Aggregation getAggregation() {
        return aggregation;
    }

    public void setAggregation(VariantSource.Aggregation aggregation) {
        this.aggregation = aggregation;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public void setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StudyConfiguration)) {
            return false;
        }

        StudyConfiguration that = (StudyConfiguration) o;

        if (studyId != that.studyId) {
            return false;
        }
        if (studyName != null ? !studyName.equals(that.studyName) : that.studyName != null) {
            return false;
        }
        if (fileIds != null ? !fileIds.equals(that.fileIds) : that.fileIds != null) {
            return false;
        }
        if (sampleIds != null ? !sampleIds.equals(that.sampleIds) : that.sampleIds != null) {
            return false;
        }
        if (cohortIds != null ? !cohortIds.equals(that.cohortIds) : that.cohortIds != null) {
            return false;
        }
        if (cohorts != null ? !cohorts.equals(that.cohorts) : that.cohorts != null) {
            return false;
        }
        if (indexedFiles != null ? !indexedFiles.equals(that.indexedFiles) : that.indexedFiles != null) {
            return false;
        }
        if (samplesInFiles != null ? !samplesInFiles.equals(that.samplesInFiles) : that.samplesInFiles != null) {
            return false;
        }
        if (calculatedStats != null ? !calculatedStats.equals(that.calculatedStats) : that.calculatedStats != null) {
            return false;
        }
        if (invalidStats != null ? !invalidStats.equals(that.invalidStats) : that.invalidStats != null) {
            return false;
        }
        if (aggregation != null ? !aggregation.equals(that.aggregation) : that.aggregation != null) {
            return false;
        }
        if (timeStamp != null ? !timeStamp.equals(that.timeStamp) : that.timeStamp != null) {
            return false;
        }
        return !(attributes != null ? !attributes.equals(that.attributes) : that.attributes != null);

    }

    @Override
    public int hashCode() {
        int result = studyId;
        result = 31 * result + (studyName != null ? studyName.hashCode() : 0);
        result = 31 * result + (fileIds != null ? fileIds.hashCode() : 0);
        result = 31 * result + (sampleIds != null ? sampleIds.hashCode() : 0);
        result = 31 * result + (cohortIds != null ? cohortIds.hashCode() : 0);
        result = 31 * result + (cohorts != null ? cohorts.hashCode() : 0);
        result = 31 * result + (indexedFiles != null ? indexedFiles.hashCode() : 0);
        result = 31 * result + (samplesInFiles != null ? samplesInFiles.hashCode() : 0);
        result = 31 * result + (calculatedStats != null ? calculatedStats.hashCode() : 0);
        result = 31 * result + (invalidStats != null ? invalidStats.hashCode() : 0);
        result = 31 * result + (aggregation != null ? aggregation.hashCode() : 0);
        result = 31 * result + (timeStamp != null ? timeStamp.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    public static <T, R> BiMap<R, T> inverseMap(BiMap<T, R> map) {
        return map.inverse();
    }

    @Deprecated
    public static <T, R> Map<R, T> inverseMap(Map<T, R> map) {
        Map<R, T> inverseMap = new HashMap<>();
        for (Map.Entry<T, R> entry : map.entrySet()) {
            inverseMap.put(entry.getValue(), entry.getKey());
        }
        return inverseMap;
    }

    /**
     * Return a set of indexed samples in a study.
     *
     * @param studyConfiguration Selected study
     * @return  Map between the indexed sample name and its identifier
     */
    public static BiMap<String, Integer> getIndexedSamples(StudyConfiguration studyConfiguration) {
        BiMap<Integer, String> idSample = StudyConfiguration.inverseMap(studyConfiguration.getSampleIds());
        BiMap<String, Integer> sampleIds = HashBiMap.create();
        for (BiMap.Entry<Integer, LinkedHashSet<Integer>> entry : studyConfiguration.getSamplesInFiles().entrySet()) {
            if (studyConfiguration.getIndexedFiles().contains(entry.getKey())) {
                for (Integer sampleId : entry.getValue()) {
                    sampleIds.put(idSample.get(sampleId), sampleId);
                }
            }
        }
        return sampleIds;
    }

    /**
     * Return all the indexed samples of an study plus the samples from a set of files.
     * Return a map between the sampleName and its position.
     *
     * @param studyConfiguration    Selected study
     * @param fileIds               Additional files to include
     * @return      Map between sampleName and position
     */
    public static BiMap<String, Integer> getIndexedSamplesPosition(StudyConfiguration studyConfiguration, int ... fileIds) {
        Objects.requireNonNull(studyConfiguration, "StudyConfiguration is required");
        BiMap<String, Integer> samplesPosition = HashBiMap.create(studyConfiguration.getSampleIds().size());
        int position = 0;
        BiMap<Integer, String> idSamples = studyConfiguration.sampleIds.inverse();
        for (Integer indexedFileId : studyConfiguration.getIndexedFiles()) {
            for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(indexedFileId)) {
                samplesPosition.putIfAbsent(idSamples.get(sampleId), position++);
            }
        }
        for (int fileId : fileIds) {
            for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fileId)) {
                samplesPosition.putIfAbsent(idSamples.get(sampleId), position++);
            }
        }
        return samplesPosition;
    }

    /**
     * Get a list of the samples to be returned, given a study and a list of samples to be returned.
     *
     * @param studyConfiguration    Study configuration
     * @param returnedSamples       List of samples to be returned
     * @return The samples IDs
     */
    public static LinkedHashMap<String, Integer> getReturnedSamplesPosition(
            StudyConfiguration studyConfiguration,
            LinkedHashSet<String> returnedSamples) {
        return getReturnedSamplesPosition(studyConfiguration, returnedSamples, StudyConfiguration::getIndexedSamplesPosition);
    }

    public static LinkedHashMap<String, Integer> getReturnedSamplesPosition(
            StudyConfiguration studyConfiguration,
            LinkedHashSet<String> returnedSamples,
            Function<StudyConfiguration, BiMap<String, Integer>> getIndexedSamplesPosition) {
        LinkedHashMap<String, Integer> samplesPosition;
        if (returnedSamples == null || returnedSamples.isEmpty()) {
            BiMap<Integer, String> unorderedSamplesPosition = getIndexedSamplesPosition(studyConfiguration).inverse();
            samplesPosition = new LinkedHashMap<>(unorderedSamplesPosition.size());
            for (int i = 0; i < unorderedSamplesPosition.size(); i++) {
                samplesPosition.put(unorderedSamplesPosition.get(i), i);
            }
        } else {
            samplesPosition = new LinkedHashMap<>(returnedSamples.size());
            int index = 0;
            BiMap<String, Integer> indexedSamplesId = getIndexedSamplesPosition.apply(studyConfiguration);
            for (String returnedSample : returnedSamples) {
                if (!returnedSample.isEmpty() && StringUtils.isNumeric(returnedSample)) {
                    returnedSample = studyConfiguration.getSampleIds().inverse().get(Integer.parseInt(returnedSample));
                }
                if (!samplesPosition.containsKey(returnedSample)) {
                    if (indexedSamplesId.containsKey(returnedSample)) {
                        samplesPosition.put(returnedSample, index++);
                    }
                }
            }
//                for (String sample : indexedSamplesId.keySet()) {
//                    samplesPosition.put(sample, index++);
//                }
        }
        return samplesPosition;
    }

}
