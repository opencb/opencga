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

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Result;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.variant.VariantBuilder.REF_ONLY_ALT;
import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.*;
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

    // Cached ids and stuff
    private final Map<Integer, LinkedHashMap<String, Integer>> returnedSamplesPositionMap = new ConcurrentHashMap<>();
    private final Map<Pair<Integer, Integer>, List<Boolean>> missingUpdatedSamplesMap = new ConcurrentHashMap<>();
    private final Map<Integer, Set<Integer>> returnedSampleIds = new ConcurrentHashMap<>();
    private final Map<String, List<String>> samplesFromFileMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> fileNameToIdMap = new ConcurrentHashMap<>();
    private final Map<Integer, LinkedHashSet<Integer>> indexedFiles = new ConcurrentHashMap<>();
    private final Map<Integer, Set<Integer>> filesFromReturnedSamples = new ConcurrentHashMap<>();
    private final Map<Integer, List<String>> fixedFormatsMap = new ConcurrentHashMap<>();
    private Map<Integer, List<String>> expectedFormatPerStudy = new ConcurrentHashMap<>();

    protected final Logger logger = LoggerFactory.getLogger(HBaseToStudyEntryConverter.class);
    private HBaseVariantConverterConfiguration configuration;

    public HBaseToStudyEntryConverter(VariantStorageMetadataManager metadataManager,
                                      HBaseToVariantStatsConverter statsConverter) {
        super(GenomeHelper.COLUMN_FAMILY_BYTES);
        this.metadataManager = metadataManager;
        this.statsConverter = statsConverter;
    }

    public HBaseToStudyEntryConverter configure(HBaseVariantConverterConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }

    public HBaseVariantConverterConfiguration getConfiguration() {
        return configuration;
    }

    protected StudyMetadata getStudyMetadata(Integer studyId) {
        StudyMetadata studyMetadata = configuration.getProjection() == null ? null
                : configuration.getProjection().getStudy(studyId).getStudyMetadata();
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

    public Map<Integer, StudyEntry> convert(Result result) {
        return convert(new VariantRow(result));
    }

    public Map<Integer, StudyEntry> convert(ResultSet resultSet) {
        return convert(new VariantRow(resultSet));
    }

    public Map<Integer, StudyEntry> convert(VariantRow row) {
        Set<Integer> studies = new HashSet<>();
        Map<Integer, Integer> fillMissing = new HashMap<>();
        Map<Integer, List<VariantRow.SampleColumn>> sampleDataMap = new HashMap<>();
        Map<Integer, List<Pair<String, PhoenixArray>>> filesMap = new HashMap<>();
        Map<Integer, List<VariantStats>> stats = new HashMap<>();
        Map<Integer, List<VariantScore>> scores = new HashMap<>();

        Variant variant = row.walker()
                .onStudy(studies::add)
                .onFillMissing(fillMissing::put)
                .onSample(sampleColumn -> {
                    studies.add(sampleColumn.getStudyId());
                    List<Integer> multiFiles;
                    if (configuration.getProjection() == null) {
                        multiFiles = Collections.emptyList();
                    } else {
                        multiFiles = configuration.getProjection()
                                .getStudy(sampleColumn.getStudyId())
                                .getMultiFileSamples()
                                .get(sampleColumn.getSampleId());
                        if (multiFiles == null) {
                            multiFiles = Collections.emptyList();
                        }
                    }
                    if (!multiFiles.isEmpty()) {
                        if (sampleColumn.getFileId() == null) {
                            // First file from multiFiles
                            // Add fileId to sampleColumn
                            Integer fileId = getFileIdFromMultiFileSample(sampleColumn.getStudyId(), sampleColumn.getSampleId());
                            sampleColumn = new SampleColumnWithFileId(sampleColumn, fileId);
                        }
                        if (!multiFiles.contains(sampleColumn.getFileId())) {
                            // Skip this file
                            return;
                        }
                    }
                    sampleDataMap.computeIfAbsent(sampleColumn.getStudyId(), s -> new ArrayList<>())
                            .add(sampleColumn);
                })
                .onFile(fileColumn -> {
                    studies.add(fileColumn.getStudyId());
                    filesMap.computeIfAbsent(fileColumn.getStudyId(), s -> new ArrayList<>())
                            .add(Pair.of(String.valueOf(fileColumn.getFileId()), fileColumn.raw()));
                })
                .onCohortStats(statsColumn -> {
                    studies.add(statsColumn.getStudyId());
                    VariantStats variantStats = statsConverter.convert(statsColumn);
                    variantStats.setCohortId(getCohortName(statsColumn.getStudyId(), statsColumn.getCohortId()));
                    stats.computeIfAbsent(statsColumn.getStudyId(), s -> new ArrayList<>())
                            .add(variantStats);
                })
                .onVariantScore(variantScoreColumn -> {
                    int studyId = variantScoreColumn.getStudyId();
                    for (VariantScoreMetadata variantScoreMetadata : getStudyMetadata(studyId).getVariantScores()) {
                        if (variantScoreMetadata.getId() == variantScoreColumn.getScoreId()) {
                            String cohortId1 = metadataManager.getCohortName(studyId, variantScoreMetadata.getCohortId1());
                            String cohortId2 = variantScoreMetadata.getCohortId2() == null
                                    ? null
                                    : metadataManager.getCohortName(studyId, variantScoreMetadata.getCohortId2());
                            VariantScore variantScore = new VariantScore(
                                    variantScoreMetadata.getName(),
                                    cohortId1,
                                    cohortId2,
                                    variantScoreColumn.getScore(),
                                    variantScoreColumn.getPValue());
                            scores.computeIfAbsent(studyId, s -> new LinkedList<>()).add(variantScore);
                            return;
                        }
                    }
                    // This is highly unlikely
                    throw VariantQueryException.scoreNotFound(variantScoreColumn.getScoreId(), metadataManager.getStudyName(studyId));
                })
                .walk();

        HashMap<Integer, StudyEntry> map = new HashMap<>();
        for (Integer studyId : studies) {
            int fillMissingColumnValue = fillMissing.getOrDefault(studyId, -1);
            StudyMetadata studyMetadata = getStudyMetadata(studyId);
            List<VariantRow.SampleColumn> samplesData = sampleDataMap.getOrDefault(studyId, Collections.emptyList());
            List<Pair<String, PhoenixArray>> files = filesMap.getOrDefault(studyId, Collections.emptyList());

            StudyEntry studyEntry = convert(samplesData, files, variant, studyMetadata, fillMissingColumnValue);
            studyEntry.setScores(scores.getOrDefault(studyId, Collections.emptyList()));
            studyEntry.setStats(stats.getOrDefault(studyId, Collections.emptyList()));
            map.put(studyId, studyEntry);
        }

        return map;
    }

    public StudyEntry convert(List<VariantRow.SampleColumn> sampleDataMap,
                                 List<Pair<String, PhoenixArray>> filesMap,
                                 Variant variant, Integer studyId) {
        return convert(sampleDataMap, filesMap, variant, getStudyMetadata(studyId), -1);
    }

    protected StudyEntry convert(List<VariantRow.SampleColumn> sampleDataMap,
                                 List<Pair<String, PhoenixArray>> filesMap,
                                 Variant variant, StudyMetadata studyMetadata, int fillMissingColumnValue) {
        List<String> fixedSampleDataKeys = getFixedSampleDataKeys(studyMetadata);
        StudyEntry studyEntry = newStudyEntry(studyMetadata, fixedSampleDataKeys);

        int[] sampleDataKeysMap = getFormatsMap(studyMetadata.getId(), fixedSampleDataKeys);

        for (VariantRow.SampleColumn sampleColumn : sampleDataMap) {
            addMainSampleDataColumn(studyMetadata, studyEntry, sampleDataKeysMap, sampleColumn);
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

    protected StudyEntry newStudyEntry(StudyMetadata studyMetadata, List<String> fixedSampleDataKeys) {
        StudyEntry studyEntry;
        if (configuration.getStudyNameAsStudyId()) {
            studyEntry = new StudyEntry(studyMetadata.getName());
        } else {
            studyEntry = new StudyEntry(String.valueOf(studyMetadata.getId()));
        }

        studyEntry.setSampleDataKeys(new ArrayList<>(getSampleDataKeys(studyMetadata.getId(), fixedSampleDataKeys)));

        LinkedHashMap<String, Integer> returnedSamplesPosition;
        if (configuration.getMutableSamplesPosition()) {
            returnedSamplesPosition = new LinkedHashMap<>(getReturnedSamplesPosition(studyMetadata));
        } else {
            returnedSamplesPosition = getReturnedSamplesPosition(studyMetadata);
        }
        studyEntry.setSamples(new ArrayList<>(returnedSamplesPosition.size()));
        studyEntry.setSortedSamplesPosition(returnedSamplesPosition);
        return studyEntry;
    }

    private List<String> getSampleDataKeys(int studyId, List<String> fixedSampleDataKeys) {
        if (configuration.getSampleDataKeys() == null) {
            return fixedSampleDataKeys;
        } else {
            return expectedFormatPerStudy.computeIfAbsent(studyId, id -> {
                if (configuration.getSampleDataKeys().size() == 1
                        && configuration.getSampleDataKeys().get(0).equals(VariantQueryUtils.NONE)) {
                    return Collections.emptyList();
                } else if (configuration.getSampleDataKeys().contains(VariantQueryUtils.ALL)) {
                    List<String> format = new ArrayList<>(configuration.getSampleDataKeys().size() + fixedSampleDataKeys.size());
                    for (String f : configuration.getSampleDataKeys()) {
                        if (f.equals(VariantQueryUtils.ALL)) {
                            format.addAll(fixedSampleDataKeys);
                        } else {
                            format.add(f);
                        }
                    }
                    return format;
                } else {
                    return configuration.getSampleDataKeys();
                }
            });
        }
    }

    protected void addMainSampleDataColumn(StudyMetadata studyMetadata, StudyEntry studyEntry,
                                           int[] sampleDataKeysMap, VariantRow.SampleColumn sampleColumn) {
        int sampleId = sampleColumn.getSampleId();
        List<String> sampleData = remapSamplesData(sampleColumn.getMutableSampleData(), sampleDataKeysMap);
        Integer gtIdx = studyEntry.getSampleDataKeyPosition("GT");
        // Replace UNKNOWN_GENOTYPE, if any
        if (gtIdx != null) {
            String gt = sampleData.get(gtIdx);
            if (gt == null) {
                sampleData.set(gtIdx, NA_GT_VALUE);
            } else if (UNKNOWN_GENOTYPE.equals(gt)) {
                sampleData.set(gtIdx, configuration.getUnknownGenotype());
            }
        }

        String sampleName = getSampleName(studyMetadata.getId(), sampleId);
        Integer samplePosition = studyEntry.getSamplesPosition().get(sampleName);
        SampleEntry sampleEntry = new SampleEntry(null, sampleColumn.getFileId(), sampleData);
        SampleEntry oldSampleEntry = studyEntry.getSamples().set(samplePosition, sampleEntry);
        if (oldSampleEntry != null) {
            if (MAIN_ALT.test(oldSampleEntry.getData().get(0)) && !MAIN_ALT.test(sampleEntry.getData().get(0))) {
                // Use the most significant genotype as "main" sample entry
                SampleEntry aux = sampleEntry;
                sampleEntry = oldSampleEntry;
                oldSampleEntry = aux;
                studyEntry.getSamples().set(samplePosition, sampleEntry);
            }
            if (sampleEntry.getFileIndex() == null) {
                Integer fileId = getFileIdFromMultiFileSample(sampleColumn.getStudyId(), sampleColumn.getSampleId());
                sampleEntry.setFileIndex(fileId);
            }
            if (oldSampleEntry.getFileIndex() == null) {
                Integer fileId = getFileIdFromMultiFileSample(sampleColumn.getStudyId(), sampleColumn.getSampleId());
                oldSampleEntry.setFileIndex(fileId);
            }
            oldSampleEntry.setSampleId(sampleName);
            studyEntry.getIssues().add(new IssueEntry(IssueType.DISCREPANCY, oldSampleEntry));
        }
    }

    private Integer getFileIdFromMultiFileSample(int studyId, int sampleId) {
        // First file from multiFiles
        // Add fileId to sampleColumn
//        metadataManager.getLoadSplitData(sampleColumn.getStudyId(), sampleColumn.getSampleId())
//                .equals(VariantStorageEngine.SplitData.MULTI)
        return metadataManager
                .getFileIdsFromSampleId(studyId, sampleId).get(0);
    }

    private int[] getFormatsMap(int studyId, List<String> fixedFormat) {
        int[] formatsMap;
        List<String> format = getSampleDataKeys(studyId, fixedFormat);
        if (format != null && !format.equals(fixedFormat)) {
            formatsMap = new int[format.size()];
            for (int i = 0; i < format.size(); i++) {
                formatsMap[i] = fixedFormat.indexOf(format.get(i));
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
        String alternateRaw = (String) (fileColumn.getElement(FILE_SEC_ALTS_IDX));
        String alternate = normalizeNonRefAlternateCoordinate(variant, alternateRaw);
        String fileName = getFileName(studyMetadata.getId(), fileId);

        // Add all combinations of secondary alternates, even the combination of "none secondary alternates", i.e. empty string
        alternateFileMap.computeIfAbsent(alternate, (key) -> new ArrayList<>()).add(fileName);
        String call = (String) (fileColumn.getElement(FILE_CALL_IDX));

        if (configuration.getProjection() != null
                && !configuration.getProjection().getStudy(studyMetadata.getId()).getFiles().contains(fileId)) {
            // TODO: Should we return the original CALL?
//            if (call != null && !call.isEmpty()) {
//                studyEntry.getFiles().add(new FileEntry(fileName, call, Collections.emptyMap()));
//            }
            return;
        }

        List<String> fixedAttributes = HBaseToVariantConverter.getFixedAttributes(studyMetadata);
        HashMap<String, String> attributes = convertFileAttributes(fileColumn, fixedAttributes);
        OriginalCall originalCall = null;
        VariantOverlappingStatus overlappingStatus =
                VariantOverlappingStatus.valueFromShortString((String) (fileColumn.getElement(FILE_VARIANT_OVERLAPPING_STATUS_IDX)));
        if (call != null && !call.isEmpty()) {
            int i = call.lastIndexOf(':');
            originalCall = new OriginalCall(call.substring(0, i), Integer.valueOf(call.substring(i + 1)));
        } else if (overlappingStatus.equals(VariantOverlappingStatus.MULTI)) {
            attributes.put(StudyEntry.FILTER, "SiteConflict");
            AlternateCoordinate alternateCoordinate = getAlternateCoordinate(alternateRaw);
            originalCall = new OriginalCall(new Variant(
                    alternateCoordinate.getChromosome(),
                    alternateCoordinate.getStart(),
                    alternateCoordinate.getEnd(),
                    alternateCoordinate.getReference(),
                    alternateCoordinate.getAlternate()).toString(), 0);
        }
        studyEntry.getFiles().add(new FileEntry(fileName, originalCall, attributes));
    }

    public static HashMap<String, String> convertFileAttributes(PhoenixArray fileColumn, List<String> fixedAttributes) {
        HashMap<String, String> attributes = new HashMap<>(fileColumn.getDimensions() - 1);
        String qual = (String) (fileColumn.getElement(FILE_QUAL_IDX));
        if (qual != null) {
            attributes.put(StudyEntry.QUAL, qual);
        }
        String filter = (String) (fileColumn.getElement(FILE_FILTER_IDX));
        if (filter != null) {
            attributes.put(StudyEntry.FILTER, filter);
        }

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
        return attributes;
    }

    private void fillEmptySamplesData(StudyEntry studyEntry, StudyMetadata studyMetadata, int fillMissingColumnValue) {
        List<String> format = studyEntry.getSampleDataKeys();
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

        List<Integer> filesInThisVariant = new ArrayList<>(studyEntry.getFiles().size());
        for (FileEntry fileEntry : studyEntry.getFiles()) {
            String file = fileEntry.getFileId();
            Integer id = getFileId(studyMetadata.getId(), file);
            filesInThisVariant.add(id);
        }

        List<Boolean> sampleWithVariant = getSampleWithVariant(studyMetadata, filesInThisVariant);
        List<Boolean> missingUpdatedList = getMissingUpdatedSamples(studyMetadata, fillMissingColumnValue);

        Map<Integer, Integer> fileIdToFileIdxMap;
        Map<Integer, Integer> sampleIdTofileIdxMap;
        Set<Integer> multiFileSample;
        if (!filesInThisVariant.isEmpty()) {
            sampleIdTofileIdxMap = new HashMap<>();
            fileIdToFileIdxMap = new HashMap<>(studyEntry.getFiles().size());
            multiFileSample = new HashSet<>();
            for (int idx = 0; idx < filesInThisVariant.size(); idx++) {
                Integer fileId = filesInThisVariant.get(idx);
                fileIdToFileIdxMap.put(fileId, idx);
                for (Integer sampleId : metadataManager.getSampleIdsFromFileId(studyMetadata.getId(), fileId)) {
                    Integer old = sampleIdTofileIdxMap.put(sampleId, idx);
                    if (old != null) {
                        multiFileSample.add(sampleId);
                    }
                }
            }
        } else {
            sampleIdTofileIdxMap = Collections.emptyMap();
            fileIdToFileIdxMap = Collections.emptyMap();
            multiFileSample = Collections.emptySet();
        }
        int sampleIdx = 0;
        List<SampleEntry> samplesData = studyEntry.getSamples();
        for (Iterator<String> iterator = studyEntry.getSamplesPosition().keySet().iterator(); iterator.hasNext(); sampleIdx++) {
            String sampleName = iterator.next();
            SampleEntry sampleEntry = samplesData.get(sampleIdx);
            if (sampleEntry == null) {
                List<String> data;
                if (missingUpdatedList.get(sampleIdx) || sampleWithVariant.get(sampleIdx)) {
                    data = unmodifiableEmptyDataReferenceGenotype;
                } else {
                    data = unmodifiableEmptyData;
                }
                sampleEntry = new SampleEntry(null, null, data);
                if (configuration.getIncludeSampleId()) {
                    sampleEntry.setSampleId(sampleName);
                }
                samplesData.set(sampleIdx, sampleEntry);
            } else {
                List<String> data = sampleEntry.getData();
                data.replaceAll(s -> s == null ? UNKNOWN_SAMPLE_DATA : s);
                if (data.size() < unmodifiableEmptyData.size()) {
                    for (int i = data.size(); i < unmodifiableEmptyData.size(); i++) {
                        data.add(unmodifiableEmptyData.get(i));
                    }
                }
                if (configuration.getIncludeSampleId()) {
                    sampleEntry.setSampleId(sampleName);
                }

                // Ensure fileIndex is present
                if (sampleEntry.getFileIndex() != null) {
                    // If fileIndex is preset at this point, it is actually the fileId
                    sampleEntry.setFileIndex(fileIdToFileIdxMap.get(sampleEntry.getFileIndex()));
                } else if (!sampleIdTofileIdxMap.isEmpty()) {
                    Integer sampleId = metadataManager.getSampleId(studyMetadata.getId(), sampleName);
                    if (multiFileSample.contains(sampleId)) {
                        // If is a multiFileSample, and the fileIndex is not defined, it was the first file from that sample
                        // Then, read the first file from this sample
                        Integer fileId = getFileIdFromMultiFileSample(studyMetadata.getId(), sampleId);
                        sampleEntry.setFileIndex(fileIdToFileIdxMap.get(fileId));
                    } else {
                        // Otherwise, check where was its only file
                        sampleEntry.setFileIndex(sampleIdTofileIdxMap.get(sampleId));
                    }
                }
            }
        }
        if (studyEntry.getIssues() != null) {
            for (IssueEntry issue : studyEntry.getIssues()) {
                SampleEntry sampleEntry = issue.getSample();

                // Ensure fileIndex is present
                if (sampleEntry.getFileIndex() != null) {
                    // If fileIndex is preset at this point, it is actually the fileId
                    sampleEntry.setFileIndex(fileIdToFileIdxMap.get(sampleEntry.getFileIndex()));
                } else if (!sampleIdTofileIdxMap.isEmpty()) {
                    Integer sampleId = metadataManager.getSampleId(studyMetadata.getId(), sampleEntry.getSampleId());
                    if (multiFileSample.contains(sampleId)) {
                        // If is a multiFileSample, and the fileIndex is not defined, it was the first file from that sample
                        // Then, read the first file from this sample
                        Integer fileId = getFileIdFromMultiFileSample(studyMetadata.getId(), sampleId);
                        sampleEntry.setFileIndex(fileIdToFileIdxMap.get(fileId));
                    } else {
                        // Otherwise, check where was its only file
                        sampleEntry.setFileIndex(sampleIdTofileIdxMap.get(sampleId));
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
        List<Boolean> booleans = missingUpdatedSamplesMap.get(pair);
        if (booleans != null) {
            return booleans;
        }
        return missingUpdatedSamplesMap.computeIfAbsent(pair, key -> {
            // If not found, no sample has been processed
            Set<Integer> sampleIds = getReturnedSampleIds(studyMetadata.getId());
            Set<Integer> fileIds = getFilesFromReturnedSamples(studyMetadata.getId());
            List<Boolean> missingUpdatedList = Arrays.asList(new Boolean[sampleIds.size()]);
            if (fillMissingColumnValue <= 0) {
                for (int i = 0; i < sampleIds.size(); i++) {
                    missingUpdatedList.set(i, false);
                }
                return missingUpdatedList;
            }
            // If fillMissingColumnValue has an invalid value, the variant is new, so gaps must be returned as ?/? for every sample
            LinkedHashSet<Integer> indexedFiles = getIndexedFiles(studyMetadata.getId());
            if (indexedFiles.contains(fillMissingColumnValue)) {
                LinkedHashMap<String, Integer> returnedSamplesPosition = getReturnedSamplesPosition(studyMetadata);
                boolean missingUpdated = true;
                int count = 0;
                for (Integer indexedFile : indexedFiles) {

                    if (fileIds.contains(indexedFile)) {
                        LinkedHashSet<Integer> samples = metadataManager.getSampleIdsFromFileId(studyMetadata.getId(), indexedFile);

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
    protected List<Boolean> getSampleWithVariant(StudyMetadata studyMetadata, Collection<Integer> filesInThisVariant) {
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
            defaultGenotype = configuration.getUnknownGenotype();
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
        if (configuration.getFailOnWrongVariants()) {
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
            variantMerger.setExpectedFormats(studyEntry.getSampleDataKeys());
            variantMerger.setStudyId("0");
            variantMerger.configure(studyMetadata.getVariantHeader());


            // Create one variant for each alternate with the samples data
            List<Variant> variants = new ArrayList<>(alternateFileIdMap.size());

            Map<String, List<String>> samplesWithUnknownGenotype = new HashMap<>();
            Integer gtIndex = studyEntry.getSampleDataKeyPositions().get("GT");
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
                se.setSampleDataKeys(studyEntry.getSampleDataKeys());
                se.setSamples(new ArrayList<>(fileIds.size()));

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
                            for (int i = 1; i < studyEntry.getSampleDataKeys().size(); i++) {
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
                studyEntry.getFile(fileEntry.getFileId()).setData(fileEntry.getData());
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
        LinkedHashMap<String, Integer> map = returnedSamplesPositionMap.get(studyMetadata.getId());
        if (map != null) {
            return map;
        }
        return returnedSamplesPositionMap.computeIfAbsent(studyMetadata.getId(), studyId -> {
            if (configuration.getProjection() == null) {
                return metadataManager.getSamplesPosition(studyMetadata, null);
            } else {
                List<Integer> sampleIds = configuration.getProjection().getStudy(studyMetadata.getId()).getSamples();
                return metadataManager.getSamplesPosition(studyMetadata, new LinkedHashSet<>(sampleIds));
            }
        });
    }

    private Set<Integer> getReturnedSampleIds(int studyId) {
        Set<Integer> set = returnedSampleIds.get(studyId);
        if (set != null) {
            return set;
        }
        return returnedSampleIds.computeIfAbsent(studyId, id -> {
            if (configuration.getProjection() == null) {
                return new HashSet<>(metadataManager.getIndexedSamples(id));
            } else {
                return new HashSet<>(configuration.getProjection().getStudy(id).getSamples());
            }
        });
    }

    private List<String> getFixedSampleDataKeys(StudyMetadata studyMetadata) {
        return fixedFormatsMap.computeIfAbsent(studyMetadata.getId(),
                (s) -> HBaseToVariantConverter.getFixedFormat(studyMetadata));
    }

    private Set<Integer> getFilesFromReturnedSamples(int studyId) {
        Set<Integer> set = filesFromReturnedSamples.get(studyId);
        if (set != null) {
            return set;
        }
        return filesFromReturnedSamples.computeIfAbsent(studyId,
                id -> metadataManager.getFileIdsFromSampleIds(id, getReturnedSampleIds(id)));
    }

    private List<String> getSamplesInFile(int studyId, int fileId) {
        List<String> list = samplesFromFileMap.get(studyId + "_" + fileId);
        if (list != null) {
            return list;
        }

        return samplesFromFileMap.computeIfAbsent(studyId + "_" + fileId, s -> {
            LinkedHashSet<Integer> sampleIds = metadataManager.getSampleIdsFromFileId(studyId, fileId);
            List<String> samples = new ArrayList<>(sampleIds.size());
            for (Integer sample : sampleIds) {
                samples.add(metadataManager.getSampleName(studyId, sample));
            }
            return samples;
        });
    }

    private List<String> getSamplesInFile(int studyId, String fileName) {
        Integer fileId = metadataManager.getFileId(studyId, fileName);
        return getSamplesInFile(studyId, fileId);
    }

    private String getSampleName(int studyId, int sampleId) {
        return metadataManager.getSampleName(studyId, sampleId);
    }

    private String getFileName(int studyId, int fileId) {
        return metadataManager.getFileName(studyId, fileId);
    }

    private Integer getFileId(int studyId, String fileName) {
        Integer id = fileNameToIdMap.get(studyId + "_" + fileName);
        if (id != null) {
            return id;
        }
        return fileNameToIdMap.computeIfAbsent(studyId + "_" + fileName, s -> metadataManager.getFileId(studyId, fileName));
    }

    private LinkedHashSet<Integer> getIndexedFiles(int studyId) {
        LinkedHashSet<Integer> set = indexedFiles.get(studyId);
        if (set != null) {
            return set;
        }
        return indexedFiles.computeIfAbsent(studyId, metadataManager::getIndexedFiles);
    }

    private String getCohortName(int studyId, int cohortId) {
        return metadataManager.getCohortName(studyId, cohortId);
    }

    private static class SampleColumnWithFileId implements VariantRow.SampleColumn {
        private final VariantRow.SampleColumn sampleColumnWithoutFile;
        private final Integer fileId;

        public SampleColumnWithFileId(VariantRow.SampleColumn sampleColumnWithoutFile, Integer fileId) {
            this.sampleColumnWithoutFile = sampleColumnWithoutFile;
            this.fileId = fileId;
        }

        @Override
        public int getStudyId() {
            return sampleColumnWithoutFile.getStudyId();
        }

        @Override
        public int getSampleId() {
            return sampleColumnWithoutFile.getSampleId();
        }

        @Override
        public Integer getFileId() {
            return fileId;
        }

        @Override
        public List<String> getSampleData() {
            return sampleColumnWithoutFile.getSampleData();
        }

        @Override
        public List<String> getMutableSampleData() {
            return sampleColumnWithoutFile.getMutableSampleData();
        }

        @Override
        public String getSampleData(int idx) {
            return sampleColumnWithoutFile.getSampleData(idx);
        }
    }
}
