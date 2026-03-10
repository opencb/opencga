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

package org.opencb.opencga.storage.mongodb.variant.converters;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.IssueEntry;
import org.opencb.biodata.models.variant.avro.IssueType;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.query.ResourceId;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.protobuf.VariantMongoDBProto;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.zip.DataFormatException;

import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.MAIN_ALT;
import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.UNKNOWN_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions.DEFAULT_GENOTYPE;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToSamplesConverter extends AbstractDocumentConverter {

    public static final String UNKNOWN_FIELD = ".";

    private final Map<Integer, StudyMetadata> studyMetadatas;
    private final Map<Integer, Map<String, Integer>> studySamplesId; //Inverse map from "sampleIds". Do not use directly, can be null
    // . Use "getIndexedIdSamplesMap()"
    private final Map<Integer, LinkedHashMap<String, Integer>> samplesPosition;
    private final Map<Integer, Map<Integer, String>> studySampleNames;
    private final Map<Integer, List<Integer>> __samplesInFile;
    private final Map<Integer, Set<String>> studyDefaultGenotypeSet;
    private final VariantStorageMetadataManager metadataManager;
    private Map<Integer, LinkedHashSet<Integer>> includeSamples;
    private Map<Integer, List<Integer>> includeFiles;
    private final Map<Integer, List<String>> sampleDataKeysPerStudy;
    private final VariantQueryProjection variantQueryProjection;

    private String unknownGenotype;
    private List<String> expectedExtraFields;
    private boolean includeSampleId = false;
    private boolean sparse = false;

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(DocumentToSamplesConverter.class.getName());


    /**
     * Converts Integer FORMAT fields.
     */
    public static final ComplexTypeConverter<String, Integer> INTEGER_COMPLEX_TYPE_CONVERTER = new ComplexTypeConverter<String, Integer>() {
        @Override
        public String convertToDataModelType(Integer anInt) {
            return anInt == 0 ? UNKNOWN_FIELD : Integer.toString(anInt > 0 ? anInt - 1 : anInt);
        }

        @Override
        public Integer convertToStorageType(String stringValue) {
            try {
                int anInt = ((int) Float.parseFloat(stringValue));
                return anInt >= 0 ? anInt + 1 : anInt;
            } catch (NumberFormatException e) {
                return 0;
            }
        }
    };

    /**
     * Converts Float FORMAT fields.
     */
    public static final ComplexTypeConverter<String, Integer> FLOAT_COMPLEX_TYPE_CONVERTER = new ComplexTypeConverter<String, Integer>() {
        @Override
        public String convertToDataModelType(Integer anInt) {
            return anInt == 0 ? UNKNOWN_FIELD : Double.toString((anInt > 0 ? anInt - 1 : anInt) / 1000.0);
        }

        @Override
        public Integer convertToStorageType(String stringValue) {
            try {
                int anInt = (int) (Float.parseFloat(stringValue) * 1000);
                return anInt >= 0 ? anInt + 1 : anInt;
            } catch (NumberFormatException e) {
                return 0;
            }
        }
    };

    public DocumentToSamplesConverter(VariantStorageMetadataManager metadataManager, VariantQueryProjection variantQueryProjection) {
        this.metadataManager = metadataManager;
        this.variantQueryProjection = variantQueryProjection;
        includeSamples = new HashMap<>();


        includeSamples.forEach((studyId, sampleIds) -> this.includeSamples.put(studyId, new LinkedHashSet<>(sampleIds)));

        includeFiles = variantQueryProjection.getFiles();
        studyMetadatas = new HashMap<>();
        studyDefaultGenotypeSet = new HashMap<>();
        samplesPosition = new HashMap<>();
        studySampleNames = new HashMap<>();
        studySamplesId = new HashMap<>();
        this.includeSamples = new HashMap<>();

        for (VariantQueryProjection.StudyVariantQueryProjection studyProjection : variantQueryProjection.getStudies().values()) {
            StudyMetadata studyMetadata = studyProjection.getStudyMetadata();
            int studyId = studyMetadata.getId();
            studyMetadatas.put(studyId, studyMetadata);
            LinkedHashMap<String, Integer> samplesPosition = new LinkedHashMap<>();

            Set<String> defGenotypeSet;
            List<String> defGenotype = studyMetadata.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key());
            if (defGenotype.size() == 0) {
                defGenotypeSet = Collections.emptySet();
            } else if (defGenotype.size() == 1) {
                defGenotypeSet = Collections.singleton(defGenotype.get(0));
            } else {
                defGenotypeSet = new LinkedHashSet<>(defGenotype);
            }
            this.studyDefaultGenotypeSet.put(studyMetadata.getId(), defGenotypeSet);

            Map<String, Integer> samplesMap = new HashMap<>();
            Map<Integer, String> samplesIdMap = new HashMap<>();
            LinkedHashSet<Integer> sampleIds = new LinkedHashSet<>();
            for (ResourceId sample : studyProjection.getSamples()) {
                samplesPosition.put(sample.getName(), samplesPosition.size());
                samplesMap.put(sample.getName(), sample.getId());
                samplesIdMap.put(sample.getId(), sample.getName());
                sampleIds.add(sample.getId());
            }
            includeSamples.put(studyId, sampleIds);
            studySampleNames.put(studyId, samplesIdMap);
            studySamplesId.put(studyId, samplesMap);
            this.samplesPosition.put(studyId, samplesPosition);
        }



        __samplesInFile = new HashMap<>();
        sampleDataKeysPerStudy = new HashMap<>();
        unknownGenotype = UNKNOWN_GENOTYPE;
    }

    /**
     * @param fileDocuments List of file documents (from the "files" field of the study document).
     *                      If null, files will not be loaded and only samples from the "samplesPosition" field will be returned.
     * @param study         If not null, will be filled with Format, SamplesData and SamplesPosition
     * @param studyId       StudyIds
     * @return Samples Data
     */
    public List<SampleEntry> convertToDataModelType(List<Document> fileDocuments, StudyEntry study, int studyId) {
        StudyMetadata studyMetadata = getStudyMetadata(studyId);
        if (studyMetadata == null) {
            return Collections.emptyList();
        }

        final LinkedHashMap<String, Integer> samplesPositionToReturn = getSamplesPosition(studyMetadata.getId());
        Map<String, Integer> sampleIds = studySamplesId.get(studyId);
        Map<Integer, String> sampleNames = studySampleNames.get(studyId);

        // Genotype data is now stored in files[].mgt (FILE_GENOTYPE_FIELD) at root level.
        // The study-level "gt" field (GENOTYPES_FIELD) is no longer written for new data.
        boolean excludeGenotypes = studyMetadata.getAttributes().getBoolean(VariantStorageOptions.EXCLUDE_GENOTYPES.key(),
                VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue());
        boolean compressExtraParams = studyMetadata.getAttributes()
                .getBoolean(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(),
                        MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.defaultValue());
        List<String> projectionSampleDataKeys = variantQueryProjection.getStudy(studyId) != null
                ? variantQueryProjection.getStudy(studyId).getSampleDataKeys()
                : null;
        if (projectionSampleDataKeys != null) {
            excludeGenotypes = !projectionSampleDataKeys.contains(VariantQueryUtils.GT);
        }
        if (samplesPositionToReturn == null || samplesPositionToReturn.isEmpty()) {
            fillStudyEntryFields(study, samplesPositionToReturn, Collections.emptyList(), Collections.emptyList(), excludeGenotypes);
            return Collections.emptyList();
        }

        final Set<Integer> filesWithSamplesData;
        final Map<Integer, Document> files;
        final List<Integer> includeFileIds;
        final Set<Integer> loadedSamples;
        List<String> extraFields;
        List<String> sampleDataKeys;

        // Read includeFiles list from studyEntry
        if (study.getFiles() != null && !study.getFiles().isEmpty()) {
            includeFileIds = new ArrayList<>(study.getFiles().size());
            for (FileEntry file : study.getFiles()) {
                int fileId = metadataManager.getFileIdOrFail(studyId, file.getFileId());
                includeFileIds.add(fileId);
            }
        } else {
            includeFileIds = Collections.emptyList();
        }
        if (fileDocuments != null) {
            files = new HashMap<>(fileDocuments.size());
            loadedSamples = new HashSet<>();
            filesWithSamplesData = new HashSet<>();
            for (Document fileObject : fileDocuments) {
                int fileId = fileObject.get(DocumentToStudyEntryConverter.FILEID_FIELD, Number.class).intValue();
                if (fileId < 0) {
                    fileId = -fileId;
                }
                files.put(fileId, fileObject);

                List<Integer> samplesInFile = getSamplesInFile(studyId, fileId);
                // File indexed and contains any sample (not disjoint)
                if (!Collections.disjoint(samplesInFile, sampleIds.values())) {
                    filesWithSamplesData.add(fileId);
                }
                if (files.containsKey(fileId)) {
                    loadedSamples.addAll(samplesInFile);
                }
            }
        } else {
            files = Collections.emptyMap();
            filesWithSamplesData = Collections.emptySet();
            loadedSamples = Collections.emptySet();
        }
        if (projectionSampleDataKeys != null) {
            extraFields = new ArrayList<>(projectionSampleDataKeys);
            extraFields.remove(VariantQueryUtils.GT);
        } else {
            extraFields = getExtraFormatFields(studyId, filesWithSamplesData, files);
        }
        sampleDataKeys = getSampleDataKeys(excludeGenotypes, extraFields);

        // An array of genotypes is initialized with the most common one
//        String defaultGenotype = mongoGenotypes.getString("def");
        Set<String> defaultGenotypes = studyDefaultGenotypeSet.get(studyId);
        String defaultGenotype = defaultGenotypes.isEmpty() ? null : defaultGenotypes.iterator().next();
        if (UNKNOWN_GENOTYPE.equals(defaultGenotype)) {
            defaultGenotype = unknownGenotype;
        }
        if (defaultGenotype == null) {
            defaultGenotype = UNKNOWN_GENOTYPE;
        }

        // In sparse mode, pre-scan mgt maps to identify samples with variant data.
        // Reduce samplesPositionToReturn so all downstream phases only process sparse samples.
        // This works for both normal (GT present) and somatic/excludeGenotypes data (mgt has "NA" keys).
        LinkedHashMap<String, Integer> effectiveSamplesPosition;
        if (sparse) {
            Set<Integer> sparseSampleIds = new HashSet<>();
            for (Map.Entry<Integer, Document> fileEntry : files.entrySet()) {
                Document fileDoc = fileEntry.getValue();
                Document mgt = fileDoc.get(DocumentToStudyEntryConverter.FILE_GENOTYPE_FIELD, Document.class);
                if (mgt == null || mgt.isEmpty()) {
                    // No mgt or empty mgt — all samples have default genotype (0/0), skip in sparse mode
                    continue;
                }
                for (Map.Entry<String, Object> mgtEntry : mgt.entrySet()) {
                    String genotype = genotypeToDataModelType(mgtEntry.getKey());
                    if (!GenotypeClass.HOM_REF.test(genotype) && !GenotypeClass.MISS.test(genotype)) {
                        for (Integer sid : (List<Integer>) mgtEntry.getValue()) {
                            if (sampleNames.containsKey(sid)) {
                                sparseSampleIds.add(sid);
                            }
                        }
                    }
                }
            }
            effectiveSamplesPosition = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> entry : samplesPositionToReturn.entrySet()) {
                Integer sid = sampleIds.get(entry.getKey());
                if (sparseSampleIds.contains(sid)) {
                    effectiveSamplesPosition.put(entry.getKey(), effectiveSamplesPosition.size());
                }
            }
        } else {
            effectiveSamplesPosition = samplesPositionToReturn;
        }

        List<SampleEntry> sampleEntries = new ArrayList<>(effectiveSamplesPosition.size());

        // Add the samples to the file
        for (String sampleName : effectiveSamplesPosition.keySet()) {
            Integer sampleId = sampleIds.get(sampleName);

            String[] values;
            values = new String[sampleDataKeys.size()];
            Arrays.fill(values, UNKNOWN_FIELD);
            if (!excludeGenotypes) {
                if (loadedSamples.contains(sampleId)) {
                    values[0] = defaultGenotype;
                } else {
                    values[0] = unknownGenotype;
                }
            }
            sampleEntries.add(new SampleEntry(includeSampleId ? sampleName : null, null, Arrays.asList(values)));
        }


        // GT is now read from files[].mgt (FILE_GENOTYPE_FIELD) — see the mgt block below.
        // The study-level "gt" field (GENOTYPES_FIELD) is no longer used for new data.

        // Set fileIdx
        for (int fileIndex = 0; fileIndex < includeFileIds.size(); fileIndex++) {
            Integer fileId = includeFileIds.get(fileIndex);
            for (Integer sampleId : getSamplesInFile(studyId, fileId)) {
                String sampleName = getSampleName(studyId, sampleId);
                Integer samplePosition = effectiveSamplesPosition.get(sampleName);
                if (samplePosition != null) {
                    sampleEntries.get(samplePosition).setFileIndex(fileIndex);
                }
            }
        }

        // Track extra field values per file for all samples — needed to populate IssueEntries and ensure
        // the primary entry's extra fields match the primary file when a sample appears in multiple files.
        // Map: sampleId -> fileId -> extraFieldValues (indexed by extraField position)
        final boolean trackPerFileExtraValues = study != null && !excludeGenotypes && !extraFields.isEmpty();
        Map<Integer, Map<Integer, String[]>> multiFileExtraValues =
                trackPerFileExtraValues ? new HashMap<>() : Collections.emptyMap();

        if (!extraFields.isEmpty()) {
            // Process non-includeFile files first, then includeFile files in order.
            // This ensures ref-genotype samples' extra field values match their fileIndex.
            List<Integer> orderedFilesWithSamplesData = new ArrayList<>(filesWithSamplesData.size());
            for (Integer fid : filesWithSamplesData) {
                if (!includeFileIds.contains(fid)) {
                    orderedFilesWithSamplesData.add(fid);
                }
            }
            for (Integer fid : includeFileIds) {
                if (filesWithSamplesData.contains(fid)) {
                    orderedFilesWithSamplesData.add(fid);
                }
            }
            for (Integer fid : orderedFilesWithSamplesData) {
                Document samplesDataDocument = null;
                if (files.containsKey(fid) && files.get(fid).containsKey(DocumentToStudyEntryConverter.SAMPLE_DATA_FIELD)) {
                    samplesDataDocument = files.get(fid)
                            .get(DocumentToStudyEntryConverter.SAMPLE_DATA_FIELD, Document.class);
                }
                if (samplesDataDocument != null) {
                    int extraFieldPosition;
                    if (excludeGenotypes) {
                        extraFieldPosition = -1; //There are no GT
                    } else {
                        extraFieldPosition = 0; //Skip GT
                    }
                    int extraFieldIndex = -1;
                    for (String extraField : extraFields) {
                        extraFieldPosition++;
                        extraFieldIndex++;
                        extraField = extraField.toLowerCase();
                        byte[] byteArray = !samplesDataDocument.containsKey(extraField)
                                ? null
                                : samplesDataDocument.get(extraField, Binary.class).getData();

                        VariantMongoDBProto.OtherFields otherFields = null;
                        if (compressExtraParams && byteArray != null && byteArray.length > 0) {
                            try {
                                byteArray = CompressionUtils.decompress(byteArray);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            } catch (DataFormatException ignore) {
                                //It was not actually compressed, so it failed decompressing
                            }
                        }
                        try {
                            if (byteArray != null && byteArray.length > 0) {
                                otherFields = VariantMongoDBProto.OtherFields.parseFrom(byteArray);
                            }
                        } catch (InvalidProtocolBufferException e) {
                            throw new UncheckedIOException(e);
                        }
                        Supplier<String> supplier;
                        if (otherFields == null) {
                            supplier = () -> UNKNOWN_FIELD;
                        } else if (otherFields.getIntValuesCount() > 0) {
                            final Iterator<Integer> iterator = otherFields.getIntValuesList().iterator();
                            supplier = () -> iterator.hasNext() ? INTEGER_COMPLEX_TYPE_CONVERTER.convertToDataModelType(iterator.next())
                                    : UNKNOWN_FIELD;
                        } else if (otherFields.getFloatValuesCount() > 0) {
                            final Iterator<Integer> iterator = otherFields.getFloatValuesList().iterator();
                            supplier = () -> iterator.hasNext() ? FLOAT_COMPLEX_TYPE_CONVERTER.convertToDataModelType(iterator.next())
                                    : UNKNOWN_FIELD;
                        } else {
                            final Iterator<String> iterator = otherFields.getStringValuesList().iterator();
                            supplier = () -> iterator.hasNext() ? iterator.next() : UNKNOWN_FIELD;
                        }
                        for (Integer sampleId : getSamplesInFile(studyId, fid)) {
                            String sampleName = getSampleName(studyId, sampleId);
                            Integer samplePosition = effectiveSamplesPosition.get(sampleName);
                            if (samplePosition == null) {
                                // The sample on this position is not returned. Skip this value.
                                supplier.get();
                            } else {
                                String value = supplier.get();
                                sampleEntries.get(samplePosition).getData().set(extraFieldPosition, value);
                                // Track per-file extra field values for all samples (needed for IssueEntries
                                // when a sample appears in multiple files' mgt maps).
                                if (trackPerFileExtraValues) {
                                    multiFileExtraValues
                                            .computeIfAbsent(sampleId, k -> new HashMap<>())
                                            .computeIfAbsent(fid, k -> new String[extraFields.size()])
                                            [extraFieldIndex] = value;
                                }
                            }
                        }

                    }
                } else {
                    int extraFieldPosition;
                    if (excludeGenotypes) {
                        extraFieldPosition = 0; //There are no GT
                    } else {
                        extraFieldPosition = 1; //Skip GT
                    }
                    for (int i = 0; i < extraFields.size(); i++) {
                        for (Integer sampleId : getSamplesInFile(studyId, fid)) {
                            String sampleName = getSampleName(studyId, sampleId);
                            Integer samplePosition = effectiveSamplesPosition.get(sampleName);
                            if (samplePosition != null) {
                                if (sampleEntries.get(samplePosition).getData().get(extraFieldPosition) == null) {
                                    sampleEntries.get(samplePosition).getData().set(extraFieldPosition, UNKNOWN_FIELD);
                                }
                            }
                        }
                        extraFieldPosition++;
                    }
                }
            }
        }

//
//        int extraFieldPosition = 1; //Skip GT
//        for (String extraField : extraFields) {
//            if (object.containsKey(extraField.toLowerCase())) {
//                List values = (List) object.get(extraField.toLowerCase());
//
//                for (int i = 0; i < values.size(); i++) {
//                    Object value = values.get(i);
//                    String sampleName = samplesPosition.inverse().get(i);
//                    samplesData.get(samplesPositionToReturn.get(sampleName)).set(extraFieldPosition, value.toString());
//                }
//            }
//            extraFieldPosition++;
//        }

        // Read GT from per-file mgt (FILE_GENOTYPE_FIELD) for ALL samples.
        // In Stage 2, mgt is the only source of GT (written for all samples, not just MULTI).
        // Samples appearing in multiple file mgt maps get a primary GT and DISCREPANCY IssueEntries.
        // Read from ALL file documents (not only output files) so genotypes are available even when
        // STUDIES_FILES is excluded from the output projection.
        if (study != null && !excludeGenotypes) {
            // sampleId -> { fileId -> genotype } — collected from all available files' mgt maps.
            Map<Integer, Map<Integer, String>> sampleFileGts = new HashMap<>();

            for (Map.Entry<Integer, Document> fileEntry : files.entrySet()) {
                Integer fileId = fileEntry.getKey();
                Document fileDoc = fileEntry.getValue();
                Document mgt = fileDoc.get(DocumentToStudyEntryConverter.FILE_GENOTYPE_FIELD, Document.class);
                if (mgt == null) {
                    continue;
                }
                for (Map.Entry<String, Object> mgtEntry : mgt.entrySet()) {
                    String genotype = genotypeToDataModelType(mgtEntry.getKey());
                    for (Integer sampleId : (List<Integer>) mgtEntry.getValue()) {
                        if (sampleNames.containsKey(sampleId)) {
                            sampleFileGts.computeIfAbsent(sampleId, k -> new LinkedHashMap<>()).put(fileId, genotype);
                        }
                    }
                }
            }

            for (Map.Entry<Integer, Map<Integer, String>> sampleEntry : sampleFileGts.entrySet()) {
                Integer sampleId = sampleEntry.getKey();
                Map<Integer, String> fileGts = sampleEntry.getValue(); // fileId -> genotype
                String sampleName = getSampleName(studyId, sampleId);
                Integer samplePosition = effectiveSamplesPosition.get(sampleName);
                if (samplePosition == null) {
                    continue;
                }
                if (fileGts.size() == 1) {
                    // Single file: just override the GT (may have been set from legacy study-level gt or default).
                    Map.Entry<Integer, String> entry = fileGts.entrySet().iterator().next();
                    sampleEntries.get(samplePosition).getData().set(0, entry.getValue());
                    int outputIdx = includeFileIds.indexOf(entry.getKey());
                    if (outputIdx >= 0) {
                        sampleEntries.get(samplePosition).setFileIndex(outputIdx);
                    }
                    continue;
                }
                // Multiple files: select the primary file (the one with MAIN_ALT genotype; ties resolved by first).
                Map.Entry<Integer, String> primaryEntry = null;
                for (Map.Entry<Integer, String> fg : fileGts.entrySet()) {
                    if (primaryEntry == null || (MAIN_ALT.test(fg.getValue()) && !MAIN_ALT.test(primaryEntry.getValue()))) {
                        primaryEntry = fg;
                    }
                }
                // Update the primary SampleEntry with the winner GT and its fileIndex (if in output)
                sampleEntries.get(samplePosition).getData().set(0, primaryEntry.getValue());
                int primaryOutputIdx = includeFileIds.indexOf(primaryEntry.getKey());
                if (primaryOutputIdx >= 0) {
                    sampleEntries.get(samplePosition).setFileIndex(primaryOutputIdx);
                }
                // Update the primary entry's extra FORMAT fields from the primary file
                int primaryFileId = primaryEntry.getKey();
                String[] primaryExtraValues = multiFileExtraValues
                        .getOrDefault(sampleId, Collections.emptyMap()).get(primaryFileId);
                if (primaryExtraValues != null) {
                    for (int j = 0; j < primaryExtraValues.length; j++) {
                        int pos = excludeGenotypes ? j : j + 1;
                        sampleEntries.get(samplePosition).getData()
                                .set(pos, primaryExtraValues[j] != null ? primaryExtraValues[j] : UNKNOWN_FIELD);
                    }
                }
                // Create IssueEntries for the remaining files
                List<IssueEntry> issues = study.getIssues();
                if (issues == null) {
                    issues = new ArrayList<>();
                    study.setIssues(issues);
                }
                for (Map.Entry<Integer, String> fg : fileGts.entrySet()) {
                    if (fg == primaryEntry) {
                        continue;
                    }
                    List<String> issueData = new ArrayList<>(sampleDataKeys.size());
                    issueData.add(fg.getValue()); // GT
                    // Populate extra FORMAT fields from the secondary file's sampleData
                    int secondaryFileId = fg.getKey();
                    String[] secondaryExtraValues = multiFileExtraValues
                            .getOrDefault(sampleId, Collections.emptyMap()).get(secondaryFileId);
                    for (int j = 0; j < extraFields.size(); j++) {
                        if (secondaryExtraValues != null && secondaryExtraValues[j] != null) {
                            issueData.add(secondaryExtraValues[j]);
                        } else {
                            issueData.add(UNKNOWN_FIELD);
                        }
                    }
                    int secondaryOutputIdx = includeFileIds.indexOf(secondaryFileId);
                    SampleEntry issueEntry = new SampleEntry(sampleName, secondaryOutputIdx >= 0 ? secondaryOutputIdx : null, issueData);
                    issues.add(new IssueEntry(IssueType.DISCREPANCY, issueEntry, Collections.emptyMap()));
                }
            }
        }

        fillStudyEntryFields(study, sparse ? null : samplesPositionToReturn, extraFields, sampleEntries, excludeGenotypes);
        return sampleEntries;
    }

    public List<String> getExtraFormatFields(int studyId, Set<Integer> filesWithSamplesData, Map<Integer, Document> files) {
        final List<String> extraFields;
        if (expectedExtraFields != null) {
            if (expectedExtraFields.contains(VariantQueryUtils.ALL)) {
                extraFields = new ArrayList<>();
                for (String expectedExtraField : expectedExtraFields) {
                    if (expectedExtraField.equals(VariantQueryUtils.ALL)) {
                        extraFields.addAll(getStudyMetadata(studyId).getAttributes()
                                .getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key()));
                    } else {
                        extraFields.add(expectedExtraField);
                    }
                }
            } else {
                extraFields = expectedExtraFields;
            }
        } else if (!files.isEmpty()) {
            Set<String> extraFieldsSet = new HashSet<>();
            for (Integer fid : filesWithSamplesData) {
                if (files.containsKey(fid)) {
                    Document sampleData = (Document) files.get(fid).get(DocumentToStudyEntryConverter.SAMPLE_DATA_FIELD);
                    if (sampleData != null) {
                        extraFieldsSet.addAll(sampleData.keySet());
                    }
                }
            }
            extraFields = new ArrayList<>(extraFieldsSet.size());
            extraFieldsSet.stream().map(String::toUpperCase).sorted().forEach(extraFields::add);
//            Iterator<String> it = extraFields.iterator();
//            while (it.hasNext()) {
//                String extraField = it.next();
//                if (!extraFieldsSet.contains(extraField.toLowerCase())) {
//                    it.remove();
//                }
//            }
        } else {
            extraFields = Collections.emptyList();
        }
        return extraFields;
    }

    private void fillStudyEntryFields(StudyEntry study, LinkedHashMap<String, Integer> samplesPositionToReturn, List<String> extraFields,
                                      List<SampleEntry> samples, boolean excludeGenotypes) {
        if (study != null) {
            //Set FORMAT
            study.setSampleDataKeys(getSampleDataKeys(excludeGenotypes, extraFields));

            //Set Samples Position
            study.setSamplesPosition(samplesPositionToReturn);
            //Set Samples Data
            study.setSamples(samples);
        }
    }

    public String getUnknownGenotype() {
        return unknownGenotype;
    }

    public void setUnknownGenotype(String unknownGenotype) {
        this.unknownGenotype = unknownGenotype;
    }

    private List<String> getSampleDataKeys(int studyId, boolean excludeGenotypes, List<String> extraFields) {
        return sampleDataKeysPerStudy.computeIfAbsent(studyId, s -> {
            List<String> sampleDataKeys = getSampleDataKeys(excludeGenotypes, extraFields);
            if (sampleDataKeys.isEmpty()) {
                return Collections.emptyList();
            } else {
                return sampleDataKeys;
            }
        });
    }

    private List<String> getSampleDataKeys(boolean excludeGenotypes, List<String> extraFields) {
        List<String> sampleDataKeys;
        if (extraFields.isEmpty()) {
            if (excludeGenotypes) {
                sampleDataKeys = Collections.emptyList();
            } else {
                sampleDataKeys = Collections.singletonList("GT");
            }
        } else {
            sampleDataKeys = new ArrayList<>(1 + extraFields.size());
            if (!excludeGenotypes) {
                sampleDataKeys.add("GT");
            }
            sampleDataKeys.addAll(extraFields);
        }
        return sampleDataKeys;
    }

    public void setSampleDataKeys(List<String> sampleDataKeys) {
        if (sampleDataKeys != null && sampleDataKeys.contains(VariantQueryUtils.GT)) {
            this.expectedExtraFields = new ArrayList<>(sampleDataKeys);
            this.expectedExtraFields.remove(VariantQueryUtils.GT);
        } else {
            this.expectedExtraFields = sampleDataKeys;
        }
    }

    public void setIncludeSampleId(boolean includeSampleId) {
        this.includeSampleId = includeSampleId;
    }

    public void setSparse(boolean sparse) {
        this.sparse = sparse;
    }

    private StudyMetadata getStudyMetadata(int studyId) {
        return studyMetadatas.get(studyId);
    }

//    /**
//     * Lazy usage of loaded samplesIdMap.
//     **/
//    private BiMap<String, Integer> getIndexedSamplesIdMap(int studyId) {
//        BiMap<String, Integer> sampleIds;
//        if (this.__studySamplesId.get(studyId) == null) {
//            sampleIds = metadataManager.getIndexedSamplesMap(studyId);
//            if (includeSamples != null && includeSamples.containsKey(studyId)) {
//                BiMap<String, Integer> includeSampleIds = HashBiMap.create();
//                sampleIds.entrySet().stream()
//                        //ReturnedSamples could be sampleNames or sampleIds as a string
//                        .filter(e -> includeSamples.get(studyId).contains(e.getValue()))
//                        .forEach(stringIntegerEntry -> includeSampleIds.put(stringIntegerEntry.getKey(), stringIntegerEntry.getValue()));
//                sampleIds = includeSampleIds;
//            }
//            this.__studySamplesId.put(studyId, sampleIds);
//        } else {
//            sampleIds = this.__s tudySamplesId.get(studyId);
//        }
//
//        return sampleIds;
//    }

    private LinkedHashMap<String, Integer> getSamplesPosition(int studyId) {
        return samplesPosition.get(studyId);
    }

    private String getSampleName(int studyId, int sampleId) {
        return studySampleNames.get(studyId).get(sampleId);
    }

    private List<Integer> getSamplesInFile(int studyId, int fid) {
        return __samplesInFile.computeIfAbsent(fid, s -> new ArrayList<>(metadataManager.getFileMetadata(studyId, fid).getSamples()));
    }

    public static String genotypeToDataModelType(String genotype) {
        return StringUtils.replace(genotype, "-1", ".");
    }

    public static String genotypeToStorageType(String genotype) {
        return StringUtils.replace(genotype, ".", "-1");
    }


    /**
     * Parse a field from a file's sampleData document once and return a positional accessor.
     *
     * <p>The protobuf binary is decompressed and parsed a single time. The returned function
     * accepts a 0-based sample position within the file and returns the decoded string value for
     * that sample, or {@code null} if the position is out of range.
     *
     * @param sampleDataDoc The {@code sampleData} sub-document inside the file document.  May be null.
     * @param fieldKey      The format field name (e.g. "DP", "GQ"). Lowercased internally.
     * @param compressed    Whether the binary value may be compressed with DEFLATE.
     * @return A positional accessor function, or {@code null} if the field is absent or the document is null.
     */
    public static IntFunction<String> sampleFieldAccessor(
            Document sampleDataDoc, String fieldKey, boolean compressed) {
        if (sampleDataDoc == null) {
            return null;
        }
        String key = fieldKey.toLowerCase();
        Object raw = sampleDataDoc.get(key);
        if (raw == null) {
            return null;
        }
        byte[] byteArray;
        if (raw instanceof Binary) {
            byteArray = ((Binary) raw).getData();
        } else if (raw instanceof byte[]) {
            byteArray = (byte[]) raw;
        } else {
            throw new IllegalArgumentException("Expected Binary or byte[] for sampleData." + key + " field, got " + raw.getClass());
        }
        if (byteArray == null || byteArray.length == 0) {
            return null;
        }
        if (compressed) {
            try {
                byteArray = CompressionUtils.decompress(byteArray);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (DataFormatException ignore) {
                // not actually compressed
            }
        }
        try {
            VariantMongoDBProto.OtherFields otherFields = VariantMongoDBProto.OtherFields.parseFrom(byteArray);
            if (otherFields.getIntValuesCount() > 0) {
                return pos -> pos < otherFields.getIntValuesCount()
                        ? INTEGER_COMPLEX_TYPE_CONVERTER.convertToDataModelType(otherFields.getIntValues(pos))
                        : null;
            } else if (otherFields.getFloatValuesCount() > 0) {
                return pos -> pos < otherFields.getFloatValuesCount()
                        ? FLOAT_COMPLEX_TYPE_CONVERTER.convertToDataModelType(otherFields.getFloatValues(pos))
                        : null;
            } else {
                return pos -> pos < otherFields.getStringValuesCount()
                        ? otherFields.getStringValues(pos)
                        : null;
            }
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedIOException(e);
        }
    }

}
