/*
 * Copyright 2015-2017 OpenCB
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderSimpleLine;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.models.BatchFileTask;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
@Deprecated
public class StudyConfiguration extends StudyMetadata {

    public static final String UNKNOWN_HEADER_ATTRIBUTE = ".";
    private int studyId;
    private String studyName;

    private BiMap<String, Integer> fileIds;
    private BiMap<String, Integer> filePaths;
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

    private Map<Integer, Integer> searchIndexedSampleSets;
    private Map<Integer, BatchFileTask.Status> searchIndexedSampleSetsStatus;


    private List<BatchFileTask> batches;

    private Aggregation aggregation;

    private Long timeStamp;

    private VariantFileHeader variantHeader;

    private ObjectMap attributes;

    private Logger logger = LoggerFactory.getLogger(StudyConfiguration.class);

    protected StudyConfiguration() {
    }

    public StudyConfiguration(StudyConfiguration other) {
        copy(other);
    }

    public void copy(StudyConfiguration other) {
        this.studyId = other.studyId;
        this.studyName = other.studyName;
        this.fileIds = HashBiMap.create(other.fileIds == null ? Collections.emptyMap() : other.fileIds);
        this.filePaths = HashBiMap.create(other.filePaths == null ? Collections.emptyMap() : other.filePaths);
        this.sampleIds = HashBiMap.create(other.sampleIds == null ? Collections.emptyMap() : other.sampleIds);
        this.cohortIds = HashBiMap.create(other.cohortIds == null ? Collections.emptyMap() : other.cohortIds);
        this.cohorts = new HashMap<>(other.cohorts);
        this.indexedFiles = new LinkedHashSet<>(other.indexedFiles);
        this.headers = new HashMap<>(other.headers);
        this.samplesInFiles = new HashMap<>(other.samplesInFiles);
        this.calculatedStats = new LinkedHashSet<>(other.calculatedStats);
        this.invalidStats = new LinkedHashSet<>(other.invalidStats);
        this.searchIndexedSampleSets = other.searchIndexedSampleSets == null
                ? new HashMap<>() : new HashMap<>(other.searchIndexedSampleSets);
        this.searchIndexedSampleSetsStatus = other.searchIndexedSampleSetsStatus == null
                ? new HashMap<>() : new HashMap<>(other.searchIndexedSampleSetsStatus);
        this.batches = new ArrayList<>(other.batches.size());
        for (BatchFileTask batch : other.batches) {
            this.batches.add(new BatchFileTask(batch));
        }
        this.aggregation = other.aggregation;
        this.timeStamp = other.timeStamp;
        if (other.variantHeader == null) {
            this.variantHeader = VariantFileHeader.newBuilder().setVersion("").build();
        } else {
            this.variantHeader = VariantFileHeader.newBuilder(other.variantHeader).setVersion("").build();
        }

        this.attributes = new ObjectMap(other.attributes);
    }

    public StudyConfiguration newInstance() {
        return new StudyConfiguration(this);
    }

    public StudyConfiguration(int studyId, String studyName) {
        this(studyId, studyName, null, null, null, null);
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
        this.filePaths = HashBiMap.create();
        this.sampleIds = HashBiMap.create(sampleIds == null ? Collections.emptyMap() : sampleIds);
        this.cohortIds = HashBiMap.create(cohortIds == null ? Collections.emptyMap() : cohortIds);
        this.cohorts = cohorts == null ? new HashMap<>() : cohorts;
        this.indexedFiles = new LinkedHashSet<>();
        this.headers = new HashMap<>();
        this.samplesInFiles = new HashMap<>();
        this.calculatedStats = new LinkedHashSet<>();
        this.invalidStats = new LinkedHashSet<>();
        this.searchIndexedSampleSets = new HashMap<>();
        this.searchIndexedSampleSetsStatus = new HashMap<>();
        this.batches = new ArrayList<>();
        this.aggregation = Aggregation.NONE;
        this.timeStamp = 0L;
        this.variantHeader = VariantFileHeader.newBuilder().setVersion("").build();
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
        return toString(ToStringStyle.SIMPLE_STYLE);
    }

    public String toJson() {
        return toString(ToStringStyle.JSON_STYLE);
    }

    public String toString(ToStringStyle style) {
        return new ToStringBuilder(this, style)
                .append("studyId", studyId)
                .append("studyName", studyName)
                .append("fileIds", fileIds)
                .append("filePaths", filePaths)
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

    public BiMap<String, Integer> getFilePaths() {
        return filePaths;
    }

    public void setFilePaths(Map<String, Integer> filePaths) {
        this.filePaths = filePaths == null ? null : HashBiMap.create(filePaths);
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

    @Deprecated
    public Map<Integer, String> getHeaders() {
        return headers;
    }

    @Deprecated
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

    public Map<Integer, Integer> getSearchIndexedSampleSets() {
        return searchIndexedSampleSets;
    }

    public StudyConfiguration setSearchIndexedSampleSets(Map<Integer, Integer> searchIndexedSampleSets) {
        this.searchIndexedSampleSets = searchIndexedSampleSets;
        return this;
    }

    public Map<Integer, BatchFileTask.Status> getSearchIndexedSampleSetsStatus() {
        return searchIndexedSampleSetsStatus;
    }

    public StudyConfiguration setSearchIndexedSampleSetsStatus(Map<Integer, BatchFileTask.Status> searchIndexedSampleSetsStatus) {
        this.searchIndexedSampleSetsStatus = searchIndexedSampleSetsStatus;
        return this;
    }

    public List<BatchFileTask> getBatches() {
        return batches;
    }

    public StudyConfiguration setBatches(List<BatchFileTask> batches) {
        this.batches = batches;
        return this;
    }

    public BatchFileTask lastBatch() {
        return getBatches().get(getBatches().size() - 1);
    }

    public Aggregation getAggregation() {
        return aggregation;
    }

    public void setAggregation(Aggregation aggregation) {
        this.aggregation = aggregation;
    }

    public void setAggregationStr(String aggregation) {
        this.aggregation = AggregationUtils.valueOf(aggregation);
    }

    @JsonIgnore
    public boolean isAggregated() {
        return AggregationUtils.isAggregated(getAggregation());
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public StudyConfiguration setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
        return this;
    }

    public VariantFileHeader getVariantHeader() {
        return variantHeader;
    }

    public Map<String, VariantFileHeaderComplexLine> getVariantHeaderLines(String key) {
        return variantHeader.getComplexLines()
                .stream()
                .filter(l -> l.getKey().equalsIgnoreCase(key))
                .collect(Collectors.toMap(VariantFileHeaderComplexLine::getId, l -> l));
    }

    public VariantFileHeaderComplexLine getVariantHeaderLine(String key, String id) {
        return variantHeader.getComplexLines()
                .stream()
                .filter(l -> l.getKey().equalsIgnoreCase(key) && l.getId().equalsIgnoreCase(id))
                .findFirst().orElse(null);
    }

    public StudyConfiguration setVariantHeader(VariantFileHeader header) {
        this.variantHeader = header;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public StudyConfiguration setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
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
        return studyId == that.studyId
                && Objects.equals(studyName, that.studyName)
                && Objects.equals(fileIds, that.fileIds)
                && Objects.equals(filePaths, that.filePaths)
                && Objects.equals(sampleIds, that.sampleIds)
                && Objects.equals(cohortIds, that.cohortIds)
                && Objects.equals(cohorts, that.cohorts)
                && Objects.equals(indexedFiles, that.indexedFiles)
                && Objects.equals(headers, that.headers)
                && Objects.equals(samplesInFiles, that.samplesInFiles)
                && Objects.equals(calculatedStats, that.calculatedStats)
                && Objects.equals(invalidStats, that.invalidStats)
                && Objects.equals(batches, that.batches)
                && aggregation == that.aggregation
                && Objects.equals(timeStamp, that.timeStamp)
                && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(studyId, studyName, fileIds, filePaths, sampleIds, cohortIds, cohorts, indexedFiles, headers, samplesInFiles,
                calculatedStats, invalidStats, batches, aggregation, timeStamp, attributes);
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
    public static BiMap<String, Integer> getIndexedSamplesPosition(StudyConfiguration studyConfiguration, int... fileIds) {
        Objects.requireNonNull(studyConfiguration, "StudyConfiguration is required");
        BiMap<String, Integer> samplesPosition = HashBiMap.create(studyConfiguration.getSampleIds().size());
        BiMap<Integer, String> idSamples = studyConfiguration.sampleIds.inverse();
        for (Integer indexedFileId : studyConfiguration.getIndexedFiles()) {
            for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(indexedFileId)) {
                samplesPosition.putIfAbsent(idSamples.get(sampleId), samplesPosition.size());
            }
        }
        for (int fileId : fileIds) {
            for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fileId)) {
                samplesPosition.putIfAbsent(idSamples.get(sampleId), samplesPosition.size());
            }
        }
        return samplesPosition;
    }

    /**
     * Get a sorted list of the samples to be returned.
     * The result can be used as SamplesPosition in {@link org.opencb.biodata.models.variant.StudyEntry#setSamplesPosition}
     *
     * @param studyConfiguration    Study configuration
     * @return The samples IDs
     */
    public static LinkedHashMap<String, Integer> getSortedIndexedSamplesPosition(StudyConfiguration studyConfiguration) {
        return getSamplesPosition(studyConfiguration, null, StudyConfiguration::getIndexedSamplesPosition);
    }

    /**
     * Get a list of the samples to be returned, given a study and a list of samples to be returned.
     * The result can be used as SamplesPosition in {@link org.opencb.biodata.models.variant.StudyEntry#setSamplesPosition}
     *
     * @param studyConfiguration    Study configuration
     * @param includeSamples        List of samples to be included in the result
     * @return The samples IDs
     */
    @Deprecated
    public static LinkedHashMap<String, Integer> getSamplesPosition(
            StudyConfiguration studyConfiguration,
            LinkedHashSet<?> includeSamples) {
        return getSamplesPosition(studyConfiguration, includeSamples, StudyConfiguration::getIndexedSamplesPosition);
    }

    @Deprecated
    public static LinkedHashMap<String, Integer> getSamplesPosition(
            StudyConfiguration studyConfiguration,
            LinkedHashSet<?> includeSamples,
            Function<StudyConfiguration, BiMap<String, Integer>> getIndexedSamplesPosition) {
        LinkedHashMap<String, Integer> samplesPosition;
        // If null, return ALL samples
        if (includeSamples == null) {
            BiMap<Integer, String> unorderedSamplesPosition = getIndexedSamplesPosition.apply(studyConfiguration).inverse();
            samplesPosition = new LinkedHashMap<>(unorderedSamplesPosition.size());
            for (int i = 0; i < unorderedSamplesPosition.size(); i++) {
                samplesPosition.put(unorderedSamplesPosition.get(i), i);
            }
        } else {
            samplesPosition = new LinkedHashMap<>(includeSamples.size());
            int index = 0;
            BiMap<String, Integer> indexedSamplesId = getIndexedSamplesPosition.apply(studyConfiguration);
            for (Object includeSampleObj : includeSamples) {
                String includeSample;
                if (includeSampleObj instanceof Number) {
                    includeSample = studyConfiguration.getSampleIds().inverse().get(((Number) includeSampleObj).intValue());
                } else if (includeSampleObj instanceof String
                        && !((String) includeSampleObj).isEmpty() && StringUtils.isNumeric((String) includeSampleObj)) {
                    includeSample = studyConfiguration.getSampleIds().inverse().get(Integer.parseInt(((String) includeSampleObj)));
                } else {
                    includeSample = includeSampleObj.toString();
                }
                if (!samplesPosition.containsKey(includeSample)) {
                    if (indexedSamplesId.containsKey(includeSample)) {
                        samplesPosition.put(includeSample, index++);
                    }
                }
            }
//                for (String sample : indexedSamplesId.keySet()) {
//                    samplesPosition.put(sample, index++);
//                }
        }
        return samplesPosition;
    }

    public void addVariantFileHeader(VariantFileHeader header, List<String> formats) {
        Map<String, Map<String, VariantFileHeaderComplexLine>> map = new HashMap<>();
        for (VariantFileHeaderComplexLine line : this.variantHeader.getComplexLines()) {
            Map<String, VariantFileHeaderComplexLine> keyMap = map.computeIfAbsent(line.getKey(), key -> new HashMap<>());
            keyMap.put(line.getId(), line);
        }
        for (VariantFileHeaderComplexLine line : header.getComplexLines()) {
            if (formats == null || !line.getKey().equalsIgnoreCase("format") || formats.contains(line.getId())) {
                Map<String, VariantFileHeaderComplexLine> keyMap = map.computeIfAbsent(line.getKey(), key -> new HashMap<>());
                if (keyMap.containsKey(line.getId())) {
                    VariantFileHeaderComplexLine prevLine = keyMap.get(line.getId());
                    if (!prevLine.equals(line)) {
                        logger.warn("Previous header line does not match with new header. previous: " + prevLine + " , new: " + line);
//                        throw new IllegalArgumentException();
                    }
                } else {
                    keyMap.put(line.getId(), line);
                    variantHeader.getComplexLines().add(line);
                }
            }
        }
        Map<String, String> simpleLines = this.variantHeader.getSimpleLines()
                .stream()
                .collect(Collectors.toMap(VariantFileHeaderSimpleLine::getKey, VariantFileHeaderSimpleLine::getValue));
        header.getSimpleLines().forEach((line) -> {
            String oldValue = simpleLines.put(line.getKey(), line.getValue());
            if (oldValue != null && !oldValue.equals(line.getValue())) {
                // If the value changes among files, replace it with a dot, as it is an unknown value.
                simpleLines.put(line.getKey(), UNKNOWN_HEADER_ATTRIBUTE);
//                throw new IllegalArgumentException();
            }
        });
        this.variantHeader.setSimpleLines(simpleLines.entrySet()
                .stream()
                .map(e -> new VariantFileHeaderSimpleLine(e.getKey(), e.getValue()))
                .collect(Collectors.toList()));
    }
}
