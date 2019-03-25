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

package org.opencb.opencga.storage.hadoop.variant.converters.study;

import com.google.common.base.Throwables;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryFields;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.variant.VariantBuilder.REF_ONLY_ALT;
import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.UNKNOWN_GENOTYPE;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED;


/**
 * Created on 26/05/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToStudyEntryConverter extends AbstractPhoenixConverter {

    private static final String UNKNOWN_SAMPLE_DATA = VCFConstants.MISSING_VALUE_v4;
    public static final int FILE_CALL_IDX = 0;
    public static final int FILE_SEC_ALTS_IDX = 1;
    public static final int FILE_VARIANT_OVERLAPPING_STATUS_IDX = 2;
    public static final int FILE_QUAL_IDX = 3;
    public static final int FILE_FILTER_IDX = 4;
    public static final int FILE_INFO_START_IDX = 5;
    public static final String ALTERNATE_COORDINATE_SEPARATOR = ":";

    private final VariantStorageMetadataManager metadataManager;
    private final HBaseToVariantStatsConverter statsConverter;

    // Cached ids
    private final Map<Integer, LinkedHashMap<String, Integer>> returnedSamplesPositionMap = new HashMap<>();
    private final Map<Pair<Integer, Integer>, List<Boolean>> missingUpdatedSamplesMap = new HashMap<>();
    private final Map<Integer, Set<Integer>> returnedSampleIds = new HashMap<>();
    private final Map<String, List<String>> samplesFromFileMap = new HashMap<>();
    private final Map<String, String> sampleIdToNameMap = new HashMap<>();
    private final Map<String, String> fileIdToNameMap = new HashMap<>();
    private final Map<String, Integer> fileNameToIdMap = new HashMap<>();
    private final Map<Integer, LinkedHashSet<Integer>> indexedFiles = new HashMap<>();
    private final Map<String, FileMetadata> fileIdToFileMetadata = new HashMap<>();
    private final Map<String, String> cohortIdToNameMap = new HashMap<>();

    private boolean studyNameAsStudyId = false;
    private boolean simpleGenotypes = false;
    private boolean failOnWrongVariants = HBaseToVariantConverter.isFailOnWrongVariants();
    private String unknownGenotype = UNKNOWN_GENOTYPE;
    private boolean mutableSamplesPosition = true;
    private List<String> expectedFormat;

    protected final Logger logger = LoggerFactory.getLogger(HBaseToStudyEntryConverter.class);
    private VariantQueryFields selectVariantElements;

    public HBaseToStudyEntryConverter(byte[] columnFamily, VariantStorageMetadataManager metadataManager,
                                      HBaseToVariantStatsConverter statsConverter) {
        super(columnFamily);
        this.metadataManager = metadataManager;
        this.statsConverter = statsConverter;
    }

    public boolean isStudyNameAsStudyId() {
        return studyNameAsStudyId;
    }

    public HBaseToStudyEntryConverter setStudyNameAsStudyId(boolean studyNameAsStudyId) {
        this.studyNameAsStudyId = studyNameAsStudyId;
        return this;
    }

    public boolean isSimpleGenotypes() {
        return simpleGenotypes;
    }

    public HBaseToStudyEntryConverter setSimpleGenotypes(boolean simpleGenotypes) {
        this.simpleGenotypes = simpleGenotypes;
        return this;
    }

    public boolean isFailOnWrongVariants() {
        return failOnWrongVariants;
    }

    public HBaseToStudyEntryConverter setFailOnWrongVariants(boolean failOnWrongVariants) {
        this.failOnWrongVariants = failOnWrongVariants;
        return this;
    }

    public String getUnknownGenotype() {
        return unknownGenotype;
    }

    public HBaseToStudyEntryConverter setUnknownGenotype(String unknownGenotype) {
        if (StringUtils.isEmpty(unknownGenotype)) {
            this.unknownGenotype = UNKNOWN_GENOTYPE;
        } else {
            this.unknownGenotype = unknownGenotype;
        }
        return this;
    }

    public boolean isMutableSamplesPosition() {
        return mutableSamplesPosition;
    }

    public HBaseToStudyEntryConverter setMutableSamplesPosition(boolean mutableSamplesPosition) {
        this.mutableSamplesPosition = mutableSamplesPosition;
        return this;
    }

    /**
     * Format of the converted variants. Discard other values.
     *
     * @param formats Formats for converted variants
     * @return this
     * @see org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils#getIncludeFormats
     */
    public HBaseToStudyEntryConverter setFormats(List<String> formats) {
        this.expectedFormat = formats;
        return this;
    }

    public void setSelectVariantElements(VariantQueryFields selectVariantElements) {
        this.selectVariantElements = selectVariantElements;
    }

    protected StudyMetadata getStudyMetadata(Integer studyId) {
        StudyMetadata studyMetadata = selectVariantElements == null ? null
                : selectVariantElements.getStudyMetadatas().get(studyId);
        if (studyMetadata != null) {
            return studyMetadata;
        } else {
            studyMetadata = metadataManager.getStudyMetadata(studyId);
            if (studyMetadata == null) {
                throw new IllegalStateException("No study found for study ID: " + studyId);
            }
            return studyMetadata;
        }
    }

    public Map<Integer, StudyEntry> convert(ResultSet resultSet) {
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();

            Set<Integer> studies = new HashSet<>();
            Map<Integer, List<Pair<Integer, List<String>>>> sampleDataMap = new HashMap<>();
            Map<Integer, List<Pair<String, PhoenixArray>>> filesMap = new HashMap<>();

            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                if (columnName.endsWith(VariantPhoenixHelper.SAMPLE_DATA_SUFIX)) {
                    String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                    Integer studyId = getStudyId(split);
                    Integer sampleId = getSampleId(split);
                    PhoenixArray value = (PhoenixArray) resultSet.getArray(i);
                    if (value != null) {
                        List<String> sampleData = toModifiableList(value);
                        studies.add(studyId);
                        sampleDataMap.computeIfAbsent(studyId, s -> new ArrayList<>()).add(Pair.of(sampleId, sampleData));
                    }
                } else if (columnName.endsWith(VariantPhoenixHelper.FILE_SUFIX)) {
                    String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                    Integer studyId = getStudyId(split);
                    String fileId = getFileId(split);
                    PhoenixArray array = (PhoenixArray) resultSet.getArray(i);
                    if (array != null) {
                        studies.add(studyId);
                        filesMap.computeIfAbsent(studyId, s -> new ArrayList<>()).add(Pair.of(fileId, array));
                    }
                } else if (columnName.endsWith(VariantPhoenixHelper.STUDY_SUFIX)) {
                    Integer studyId = VariantPhoenixHelper.extractStudyId(columnName, true);
                    // Method GetInt will always return 0, even is null was stored.
                    // Check if value was actually a null.
                    resultSet.getInt(i);
                    if (!resultSet.wasNull()) {
                        studies.add(studyId);
                    }
                }
            }

            Variant variant = VariantPhoenixKeyFactory.extractVariantFromResultSet(resultSet);

            Map<Integer, Map<Integer, VariantStats>> stats = statsConverter.convert(resultSet);

            HashMap<Integer, StudyEntry> map = new HashMap<>();
            for (Integer studyId : studies) {
                int fillMissingColumnValue = resultSet.getInt(VariantPhoenixHelper.getFillMissingColumn(studyId).column());
                StudyMetadata studyMetadata = getStudyMetadata(studyId);
                StudyEntry studyEntry = convert(
                        sampleDataMap.getOrDefault(studyId, Collections.emptyList()),
                        filesMap.getOrDefault(studyId, Collections.emptyList()), variant, studyMetadata, fillMissingColumnValue);
                for (Map.Entry<Integer, VariantStats> entry : stats.getOrDefault(studyId, Collections.emptyMap()).entrySet()) {
                    studyEntry.setStats(getCohortName(studyMetadata.getId(), entry.getKey()), entry.getValue());
                }
                map.put(studyId, studyEntry);
            }

            return map;
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public Map<Integer, StudyEntry> convert(Result result) {
        Set<Integer> studies = new HashSet<>();
        Map<Integer, List<Pair<Integer, List<String>>>> sampleDataMap = new HashMap<>();
        Map<Integer, List<Pair<String, PhoenixArray>>> filesMap = new HashMap<>();

        for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(columnFamily).entrySet()) {
            byte[] qualifier = entry.getKey();
            byte[] bytes = entry.getValue();
            if (bytes == null || bytes.length == 0) {
                continue;
            }
            if (endsWith(qualifier, VariantPhoenixHelper.SAMPLE_DATA_SUFIX_BYTES)) {
                String columnName = Bytes.toString(qualifier);
                String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                Integer studyId = getStudyId(split);
                Integer sampleId = getSampleId(split);
                PhoenixArray array = (PhoenixArray) PVarcharArray.INSTANCE.toObject(bytes);
                List<String> sampleData = toModifiableList(array);
                studies.add(studyId);
                sampleDataMap.computeIfAbsent(studyId, s -> new ArrayList<>()).add(Pair.of(sampleId, sampleData));
            } else if (endsWith(qualifier, VariantPhoenixHelper.FILE_SUFIX_BYTES)) {
                String columnName = Bytes.toString(qualifier);
                String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                Integer studyId = getStudyId(split);
                String fileId = getFileId(split);
                studies.add(studyId);
                PhoenixArray array = (PhoenixArray) PVarcharArray.INSTANCE.toObject(bytes);
                filesMap.computeIfAbsent(studyId, s -> new ArrayList<>()).add(Pair.of(fileId, array));
            } else if (endsWith(qualifier, VariantPhoenixHelper.STUDY_SUFIX_BYTES)) {
                String columnName = Bytes.toString(qualifier);
                Integer studyId = VariantPhoenixHelper.extractStudyId(columnName, true);
                studies.add(studyId);
            }
        }

        Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());
        Map<Integer, Map<Integer, VariantStats>> stats = statsConverter.convert(result);

        HashMap<Integer, StudyEntry> map = new HashMap<>();
        for (Integer studyId : studies) {
            StudyMetadata studyMetadata = getStudyMetadata(studyId);
            byte[] bytes = result.getValue(columnFamily,
                    VariantPhoenixHelper.getFillMissingColumn(studyId).bytes());
            Integer fillMissingColumnValue = bytes == null || bytes.length == 0 ? -1 : (Integer) PInteger.INSTANCE.toObject(bytes);
            StudyEntry studyEntry = convert(
                    sampleDataMap.getOrDefault(studyId, Collections.emptyList()),
                    filesMap.getOrDefault(studyId, Collections.emptyList()), variant, studyMetadata, fillMissingColumnValue);
            for (Map.Entry<Integer, VariantStats> entry : stats.getOrDefault(studyId, Collections.emptyMap()).entrySet()) {
                studyEntry.setStats(getCohortName(studyMetadata.getId(), entry.getKey()), entry.getValue());
            }
            map.put(studyId, studyEntry);
        }

        return map;
    }

    protected StudyEntry convert(List<Pair<Integer, List<String>>> sampleDataMap,
                                 List<Pair<String, PhoenixArray>> filesMap,
                                 Variant variant, Integer studyId) {
        return convert(sampleDataMap, filesMap, variant, getStudyMetadata(studyId), -1);
    }

    protected StudyEntry convert(List<Pair<Integer, List<String>>> sampleDataMap,
                                 List<Pair<String, PhoenixArray>> filesMap,
                                 Variant variant, StudyMetadata studyMetadata, int fillMissingColumnValue) {
        List<String> fixedFormat = HBaseToVariantConverter.getFixedFormat(studyMetadata.getAttributes());
        StudyEntry studyEntry = newStudyEntry(studyMetadata, fixedFormat);

        int[] formatsMap = getFormatsMap(fixedFormat);

        for (Pair<Integer, List<String>> pair : sampleDataMap) {
            Integer sampleId = pair.getKey();
            List<String> sampleData = pair.getValue();
            addMainSampleDataColumn(studyMetadata, studyEntry, formatsMap, sampleId, sampleData);
        }

        Map<String, List<String>> alternateFileMap = new HashMap<>();
        for (Pair<String, PhoenixArray> pair : filesMap) {
            String fileId = pair.getKey();
            PhoenixArray fileColumn = pair.getValue();
            addFileEntry(studyMetadata, variant, studyEntry, fileId, fileColumn, alternateFileMap);
        }
        addSecondaryAlternates(variant, studyEntry, studyMetadata, alternateFileMap);

        fillEmptySamplesData(studyEntry, studyMetadata, fillMissingColumnValue);

        return studyEntry;
    }

    protected StudyEntry newStudyEntry(StudyMetadata studyMetadata, List<String> fixedFormat) {
        StudyEntry studyEntry;
        if (studyNameAsStudyId) {
            studyEntry = new StudyEntry(studyMetadata.getName());
        } else {
            studyEntry = new StudyEntry(String.valueOf(studyMetadata.getId()));
        }

        if (expectedFormat == null) {
            studyEntry.setFormat(new ArrayList<>(fixedFormat));
        } else {
            studyEntry.setFormat(new ArrayList<>(expectedFormat));
        }

        LinkedHashMap<String, Integer> returnedSamplesPosition;
        if (mutableSamplesPosition) {
            returnedSamplesPosition = new LinkedHashMap<>(getReturnedSamplesPosition(studyMetadata));
        } else {
            returnedSamplesPosition = getReturnedSamplesPosition(studyMetadata);
        }
        studyEntry.setSamplesData(new ArrayList<>(returnedSamplesPosition.size()));
        studyEntry.setSortedSamplesPosition(returnedSamplesPosition);
        return studyEntry;
    }

    protected void addMainSampleDataColumn(StudyMetadata studyMetadata, StudyEntry studyEntry,
                                           int[] formatsMap, Integer sampleId, List<String> sampleData) {
        sampleData = remapSamplesData(sampleData, formatsMap);
        Integer gtIdx = studyEntry.getFormatPositions().get("GT");
        // Replace UNKNOWN_GENOTYPE, if any
        if (gtIdx != null && GenotypeClass.UNKNOWN_GENOTYPE.equals(sampleData.get(gtIdx))) {
            sampleData.set(gtIdx, unknownGenotype);
        }

        String sampleName = getSampleName(studyMetadata.getId(), sampleId);
        studyEntry.addSampleData(sampleName, sampleData);
    }

    private int[] getFormatsMap(List<String> fixedFormat) {
        int[] formatsMap;
        if (expectedFormat != null && !expectedFormat.equals(fixedFormat)) {
            formatsMap = new int[expectedFormat.size()];
            for (int i = 0; i < expectedFormat.size(); i++) {
                formatsMap[i] = fixedFormat.indexOf(expectedFormat.get(i));
            }
        } else {
            formatsMap = null;
        }
        return formatsMap;
    }

    private List<String> remapSamplesData(List<String> sampleData, int[] formatsMap) {
        if (formatsMap == null) {
            // Nothing to do!
            return sampleData;
        } else {
            List<String> filteredSampleData = new ArrayList<>(formatsMap.length);
            for (int i : formatsMap) {
                if (i < 0) {
                    filteredSampleData.add(UNKNOWN_SAMPLE_DATA);
                } else {
                    if (i < sampleData.size()) {
                        filteredSampleData.add(sampleData.get(i));
                    } else {
                        filteredSampleData.add(UNKNOWN_SAMPLE_DATA);
                    }
                }
            }
            return filteredSampleData;
        }
    }

    private void addFileEntry(StudyMetadata studyMetadata, Variant variant, StudyEntry studyEntry, String fileIdStr,
                              PhoenixArray fileColumn, Map<String, List<String>> alternateFileMap) {
        int fileId = Integer.parseInt(fileIdStr);
        String alternate = normalizeNonRefAlternateCoordinate(variant, (String) (fileColumn.getElement(FILE_SEC_ALTS_IDX)));
        String fileName = getFileName(studyMetadata.getId(), fileId);

        // Add all combinations of secondary alternates, even the combination of "none secondary alternates", i.e. empty string
        alternateFileMap.computeIfAbsent(alternate, (key) -> new ArrayList<>()).add(fileName);
        String call = (String) (fileColumn.getElement(FILE_CALL_IDX));
        if (!selectVariantElements.getFiles().get(studyMetadata.getId()).contains(fileId)) {
            // TODO: Should we return the original CALL?
//            if (call != null && !call.isEmpty()) {
//                studyEntry.getFiles().add(new FileEntry(fileName, call, Collections.emptyMap()));
//            }
            return;
        }

        HashMap<String, String> attributes = new HashMap<>(fileColumn.getDimensions() - 1);
        String qual = (String) (fileColumn.getElement(FILE_QUAL_IDX));
        if (qual != null) {
            attributes.put(StudyEntry.QUAL, qual);
        }
        String filter = (String) (fileColumn.getElement(FILE_FILTER_IDX));
        if (filter != null) {
            attributes.put(StudyEntry.FILTER, filter);
        }

        List<String> fixedAttributes = HBaseToVariantConverter.getFixedAttributes(studyMetadata);
        int i = FILE_INFO_START_IDX;
        for (String attribute : fixedAttributes) {
            if (i >= fileColumn.getDimensions()) {
                break;
            }
            String value = (String) (fileColumn.getElement(i));
            if (value != null) {
                attributes.put(attribute, value);
            }
            i++;
        }
        // fileColumn.getElement(FILE_VARIANT_OVERLAPPING_STATUS_IDX);
        studyEntry.getFiles().add(new FileEntry(fileName, call, attributes));
    }

    private void fillEmptySamplesData(StudyEntry studyEntry, StudyMetadata studyMetadata, int fillMissingColumnValue) {
        List<String> format = studyEntry.getFormat();
        List<String> emptyData = new ArrayList<>(format.size());
        List<String> emptyDataReferenceGenotype = new ArrayList<>(format.size());
        String defaultGenotype = getDefaultGenotype(studyMetadata);
        for (String formatKey : format) {
            if (VariantMerger.GT_KEY.equals(formatKey)) {
                emptyData.add(defaultGenotype);
                emptyDataReferenceGenotype.add("0/0");
            } else {
                emptyData.add(UNKNOWN_SAMPLE_DATA);
                emptyDataReferenceGenotype.add(UNKNOWN_SAMPLE_DATA);
            }
        }
        // Make unmodifiable. All samples will share this information
        List<String> unmodifiableEmptyData = Collections.unmodifiableList(emptyData);
        List<String> unmodifiableEmptyDataReferenceGenotype = Collections.unmodifiableList(emptyDataReferenceGenotype);

        Set<Integer> filesInThisVariant = studyEntry.getFiles().stream()
                .map(FileEntry::getFileId)
                .map(file -> getFileId(studyMetadata.getId(), file))
                .collect(Collectors.toSet());

        List<Boolean> sampleWithVariant = getSampleWithVariant(studyMetadata, filesInThisVariant);
        List<Boolean> missingUpdatedList = getMissingUpdatedSamples(studyMetadata, fillMissingColumnValue);

        ListIterator<List<String>> sampleIterator = studyEntry.getSamplesData().listIterator();
        while (sampleIterator.hasNext()) {
            List<String> sampleData = sampleIterator.next();
            if (sampleData == null) {
                int sampleIdx = sampleIterator.previousIndex();
                if (missingUpdatedList.get(sampleIdx) || sampleWithVariant.get(sampleIdx)) {
                    sampleIterator.set(unmodifiableEmptyDataReferenceGenotype);
                } else {
                    sampleIterator.set(unmodifiableEmptyData);
                }
            } else {
                sampleData.replaceAll(s -> s == null ? UNKNOWN_SAMPLE_DATA : s);
                if (sampleData.size() < unmodifiableEmptyData.size()) {
                    for (int i = sampleData.size(); i < unmodifiableEmptyData.size(); i++) {
                        sampleData.add(unmodifiableEmptyData.get(i));
                    }
                }
            }
        }

    }

    /**
     * Given a study and the value of the fillMissingColumnValue {@link VariantPhoenixHelper::getFillMissingColumn}, gets a list of
     * booleans, one per sample, ordered by the position in the StudyEntry indicating if that sample has known information for that
     * position, of if it is unknown.
     *
     * @param studyMetadata The study configuration
     * @param fillMissingColumnValue    Value of the column {@link VariantPhoenixHelper::getFillMissingColumn} containing the last
     *                                  file updated by the fillMissing task
     * @return List of boolean values, one per sample.
     */
    protected List<Boolean> getMissingUpdatedSamples(StudyMetadata studyMetadata, int fillMissingColumnValue) {
        Pair<Integer, Integer> pair = Pair.of(studyMetadata.getId(), fillMissingColumnValue);
        return missingUpdatedSamplesMap.computeIfAbsent(pair, key -> {
            // If not found, no sample has been processed
            if (fillMissingColumnValue <= 0) {
                return Collections.emptyList();
            }
            Set<Integer> sampleIds = getReturnedSampleIds(studyMetadata);
            List<Boolean> missingUpdatedList = Arrays.asList(new Boolean[sampleIds.size()]);
            // If fillMissingColumnValue has an invalid value, the variant is new, so gaps must be returned as ?/? for every sample
            LinkedHashSet<Integer> indexedFiles = getIndexedFiles(studyMetadata.getId());
            if (indexedFiles.contains(fillMissingColumnValue)) {
                LinkedHashMap<String, Integer> returnedSamplesPosition = getReturnedSamplesPosition(studyMetadata);
                boolean missingUpdated = true;
                int count = 0;
                for (Integer indexedFile : indexedFiles) {
                    LinkedHashSet<Integer> samples = getFileMetadata(studyMetadata.getId(), indexedFile).getSamples();

                    // Do not skip the not returned files, as they may have returned samples
                    // if (selectVariantElements != null && selectVariantElements.getFiles().containsKey(indexedFile))
                    for (Integer sampleId : samples) {
                        if (sampleIds.contains(sampleId)) {
                            String sampleName = getSampleName(studyMetadata.getId(), sampleId);
                            if (null == missingUpdatedList.set(returnedSamplesPosition.get(sampleName), missingUpdated)) {
                                count++;
                            } // else, the sample was found in two different files. Data may be split in one file per chromosome
                        }
                    }

                    if (indexedFile == fillMissingColumnValue) {
                        missingUpdated = false;
                    }
                }
                if (count != missingUpdatedList.size()) {
                    logger.error("Missing updatedList values!");
                    missingUpdatedList.replaceAll(b -> b == null ? false : b);
                }
            } else {
                if (fillMissingColumnValue > 0) {
                    logger.warn("Last file updated '" + fillMissingColumnValue + "' is not indexed!");
                }
                for (int i = 0; i < missingUpdatedList.size(); i++) {
                    missingUpdatedList.set(i, Boolean.FALSE);
                }
            }
            return missingUpdatedList;
        });
    }

    /**
     * Given a study and a set of files in this variant, gets a list of booleans, one per sample, ordered by the position in the StudyEntry
     * indicating if that sample has known information for that position, of if it is unknown.
     *
     * It may happen, in multi sample VCFs, that the information from one sample is not present because it was a reference genome.
     * If the file is present in that variant, indicated that the sample data was left empty on purpose, so it should be returned
     * as a reference genotype.
     *
     * Do not cache this values as it depends on the "filesInThisVariant", and there may be a huge number of combinations.
     *
     * @param studyMetadata    The study configuration
     * @param filesInThisVariant    Files existing in this variant
     * @return List of boolean values, one per sample.
     */
    protected List<Boolean> getSampleWithVariant(StudyMetadata studyMetadata, Set<Integer> filesInThisVariant) {
        LinkedHashMap<String, Integer> returnedSamplesPosition = getReturnedSamplesPosition(studyMetadata);
        List<Boolean> samplesWithVariant = new ArrayList<>(returnedSamplesPosition.size());
        for (int i = 0; i < returnedSamplesPosition.size(); i++) {
            samplesWithVariant.add(false);
        }
        if (filesInThisVariant.isEmpty()) {
            return samplesWithVariant;
        } else {
            for (Integer file : filesInThisVariant) {
                for (String sample : getSamplesInFile(studyMetadata.getId(), file)) {
                    Integer sampleIdx = returnedSamplesPosition.get(sample);
                    if (sampleIdx != null) {
                        samplesWithVariant.set(sampleIdx, true);
                    }
                }
            }
        }
        return samplesWithVariant;
    }

    protected String getDefaultGenotype(StudyMetadata studyMetadata) {
        String defaultGenotype;
        if (VariantStorageEngine.MergeMode.from(studyMetadata.getAttributes()).equals(VariantStorageEngine.MergeMode.ADVANCED)
                || studyMetadata.getAttributes().getBoolean(MISSING_GENOTYPES_UPDATED)) {
            defaultGenotype = "0/0";
        } else {
            defaultGenotype = unknownGenotype;
        }
        return defaultGenotype;
    }

    private String getSimpleGenotype(String genotype) {
        int idx = genotype.indexOf(',');
        if (idx > 0) {
            return genotype.substring(0, idx);
        } else {
            return genotype;
        }
    }

    private void wrongVariant(String message) {
        if (failOnWrongVariants) {
            throw new IllegalStateException(message);
        } else {
            logger.warn(message);
        }
    }

    /**
     * Add secondary alternates to the StudyEntry. Merge secondary alternates if needed.
     *
     * @param variant            Variant coordinates.
     * @param studyEntry         Study Entry all samples data
     * @param studyMetadata      StudyMetadata from the study
     * @param alternateFileIdMap Map from SecondaryAlternate to FileId
     */
    protected void addSecondaryAlternates(Variant variant, StudyEntry studyEntry, StudyMetadata studyMetadata,
                                        Map<String, List<String>> alternateFileIdMap) {

        final List<AlternateCoordinate> alternateCoordinates;
        if (alternateFileIdMap.isEmpty()) {
            alternateCoordinates = Collections.emptyList();
        } else if (alternateFileIdMap.size() == 1) {
            alternateCoordinates = getAlternateCoordinates(alternateFileIdMap.keySet().iterator().next());
        } else {
            // There are multiple secondary alternates.
            // We need to rearrange the genotypes to match with the secondary alternates order.
            VariantMerger variantMerger = new VariantMerger(false);
            variantMerger.setExpectedFormats(studyEntry.getFormat());
            variantMerger.setStudyId("0");
            variantMerger.configure(studyMetadata.getVariantHeader());


            // Create one variant for each alternate with the samples data
            List<Variant> variants = new ArrayList<>(alternateFileIdMap.size());

            Map<String, List<String>> samplesWithUnknownGenotype = new HashMap<>();
            Integer gtIndex = studyEntry.getFormatPositions().get("GT");
            for (Map.Entry<String, List<String>> entry : alternateFileIdMap.entrySet()) {
                String secondaryAlternates = entry.getKey();
                List<String> fileIds = entry.getValue();

                Variant sampleVariant = new Variant(
                        variant.getChromosome(),
                        variant.getStart(),
                        variant.getEnd(),
                        variant.getReference(),
                        variant.getAlternate())
                        .setSv(variant.getSv());
                StudyEntry se = new StudyEntry("0");
                se.setSecondaryAlternates(getAlternateCoordinates(secondaryAlternates));
                se.setFormat(studyEntry.getFormat());
                se.setSamplesData(new ArrayList<>(fileIds.size()));

                for (String fileId : fileIds) {
                    FileEntry fileEntry = studyEntry.getFile(fileId);
                    if (fileEntry != null) {
                        se.getFiles().add(fileEntry);
                    }
                    List<String> samples = getSamplesInFile(studyMetadata.getId(), fileId);
                    for (String sample : samples) {
                        List<String> sampleData = studyEntry.getSampleData(sample);
                        if (gtIndex == null || !sampleData.get(0).equals(UNKNOWN_GENOTYPE)) {
                            se.addSampleData(sample, sampleData);
                        } else {
                            // Add dummy data for this sample
                            ArrayList<String> dummySampleData = new ArrayList<>();
                            dummySampleData.add("0/0");
                            for (int i = 1; i < studyEntry.getFormat().size(); i++) {
                                dummySampleData.add("");
                            }
                            se.addSampleData(sample, dummySampleData);
                            samplesWithUnknownGenotype.put(sample, sampleData);
                        }
                    }
                }
                sampleVariant.addStudyEntry(se);
                variants.add(sampleVariant);
            }

            // Merge the variants in the first variant
            Variant newVariant = variantMerger.merge(variants.get(0), variants.subList(1, variants.size()));

            // Update samplesData information
            StudyEntry newSe = newVariant.getStudies().get(0);
            for (String sample : newSe.getSamplesName()) {
                List<String> unknownGenotypeData = samplesWithUnknownGenotype.get(sample);
                if (unknownGenotypeData != null) {
                    studyEntry.addSampleData(sample, unknownGenotypeData);
                } else {
                    studyEntry.addSampleData(sample, newSe.getSampleData(sample));
                }
            }
            for (FileEntry fileEntry : newSe.getFiles()) {
                studyEntry.getFile(fileEntry.getFileId()).setAttributes(fileEntry.getAttributes());
            }
//            for (Map.Entry<String, Integer> entry : newSe.getSamplesPosition().entrySet()) {
//                List<String> data = newSe.getSamplesData().get(entry.getValue());
//                Integer sampleId = Integer.valueOf(entry.getKey());
//                samplesDataMap.put(sampleId, data);
//            }
            alternateCoordinates = newSe.getSecondaryAlternates();
        }
        studyEntry.setSecondaryAlternates(alternateCoordinates);
    }

    public static List<AlternateCoordinate> getAlternateCoordinates(String s) {
        if (StringUtils.isEmpty(s)) {
            return Collections.emptyList();
        } else {
            return Arrays.stream(s.split(","))
                    .map(HBaseToStudyEntryConverter::getAlternateCoordinate)
                    .collect(Collectors.toList());
        }
    }

    // Alternate field may contain the separator char
    public static AlternateCoordinate getAlternateCoordinate(String s) {
        String[] split = s.split(ALTERNATE_COORDINATE_SEPARATOR, 5);
        int idx = split[4].lastIndexOf(ALTERNATE_COORDINATE_SEPARATOR);
        String alternate = split[4].substring(0, idx);
        VariantType type = VariantType.valueOf(split[4].substring(idx + 1));
        return new AlternateCoordinate(
                split[0],
                Integer.parseInt(split[1]),
                Integer.parseInt(split[2]),
                split[3],
                alternate,
                type
        );
    }

    public static String normalizeNonRefAlternateCoordinate(Variant variant, String s) {
        if (s != null && s.contains(REF_ONLY_ALT)) {
            StringBuilder sb = new StringBuilder();
            String reference = variant.getReference();
            for (String s1 : s.split(",")) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                if (s1.contains(REF_ONLY_ALT)) {
                    StudyEntryToHBaseConverter.buildSecondaryAlternate(variant.getChromosome(), variant.getStart(), variant.getEnd(),
                            reference, REF_ONLY_ALT, VariantType.NO_VARIATION, sb);
                } else {
                    sb.append(s1);
                }
            }
            return sb.toString();
        } else {
            return s;
        }
    }

    protected static Integer getStudyId(String[] split) {
        return Integer.valueOf(split[0]);
    }

    protected static Integer getSampleId(String[] split) {
        return Integer.valueOf(split[1]);
    }

    protected static String getFileId(String[] split) {
        return split[1];
    }

    ////// Caching methods

    /**
     * Creates a SORTED MAP with the required samples position.
     *
     * @param studyMetadata Study metadata
     * @return Sorted linked hash map
     */
    private LinkedHashMap<String, Integer> getReturnedSamplesPosition(StudyMetadata studyMetadata) {
        return returnedSamplesPositionMap.computeIfAbsent(studyMetadata.getId(), studyId -> {
            if (selectVariantElements == null) {
                return metadataManager.getSamplesPosition(studyMetadata, null);
            } else {
                List<Integer> sampleIds = selectVariantElements.getSamples().get(studyMetadata.getId());
                return metadataManager.getSamplesPosition(studyMetadata, new LinkedHashSet<>(sampleIds));
            }
        });
    }

    private Set<Integer> getReturnedSampleIds(StudyMetadata studyMetadata) {
        return returnedSampleIds.computeIfAbsent(studyMetadata.getId(), studyId -> {
            if (selectVariantElements == null) {
                return new HashSet<>(metadataManager.getIndexedSamples(studyId));
            } else {
                return new HashSet<>(selectVariantElements.getSamples().get(studyId));
            }
        });
    }

    private List<String> getSamplesInFile(int studyId, int fileId) {
        List<String> samples = samplesFromFileMap.get(studyId + "_" + fileId);
        if (samples == null) {
            FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, fileId);
            LinkedHashSet<Integer> sampleIds = fileMetadata.getSamples();
            samples = new ArrayList<>(sampleIds.size());
            for (Integer sample : sampleIds) {
                samples.add(metadataManager.getSampleName(studyId, sample));
            }
            samplesFromFileMap.put(studyId + "_" + fileMetadata.getId(), samples);
            samplesFromFileMap.put(studyId + "_" + fileMetadata.getName(), samples);
            fileIdToFileMetadata.put(studyId + "_" + fileMetadata.getId(), fileMetadata);
        }
        return samples;
    }

    private List<String> getSamplesInFile(int studyId, String fileName) {
        List<String> samples = samplesFromFileMap.get(studyId + "_" + fileName);
        if (samples == null) {
            Integer fileId = metadataManager.getFileId(studyId, fileName);
            return getSamplesInFile(studyId, fileId);
        }
        return samples;
    }

    private String getSampleName(int studyId, int sampleId) {
        return sampleIdToNameMap.computeIfAbsent(studyId + "_" + sampleId, s -> metadataManager.getSampleName(studyId, sampleId));
    }

    private String getFileName(int studyId, int fileId) {
        return fileIdToNameMap.computeIfAbsent(studyId + "_" + fileId, s -> metadataManager.getFileName(studyId, fileId));
    }

    private Integer getFileId(int studyId, String fileName) {
        return fileNameToIdMap.computeIfAbsent(studyId + "_" + fileName, s -> metadataManager.getFileId(studyId, fileName));
    }

    private LinkedHashSet<Integer> getIndexedFiles(int studyId) {
        return indexedFiles.computeIfAbsent(studyId, metadataManager::getIndexedFiles);
    }

    private FileMetadata getFileMetadata(int studyId, int fileId) {
        return fileIdToFileMetadata.computeIfAbsent(studyId + "_" + fileId, s -> metadataManager.getFileMetadata(studyId, fileId));
    }

    private String getCohortName(int studyId, int cohortId) {
        return cohortIdToNameMap.computeIfAbsent(studyId + "_" + cohortId, s -> metadataManager.getCohortName(studyId, cohortId));
    }

}
