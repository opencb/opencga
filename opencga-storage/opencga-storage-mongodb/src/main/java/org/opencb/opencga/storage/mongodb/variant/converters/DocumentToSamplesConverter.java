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
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.query.ResourceId;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.protobuf.VariantMongoDBProto;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Supplier;
import java.util.zip.DataFormatException;

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

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(DocumentToSamplesConverter.class.getName());


    /**
     * Converts Integer FORMAT fields.
     */
    static final ComplexTypeConverter<String, Integer> INTEGER_COMPLEX_TYPE_CONVERTER = new ComplexTypeConverter<String, Integer>() {
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
    static final ComplexTypeConverter<String, Integer> FLOAT_COMPLEX_TYPE_CONVERTER = new ComplexTypeConverter<String, Integer>() {
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

    public List<SampleEntry> convertToDataModelType(Document object, int studyId) {
        return convertToDataModelType(object, null, studyId);
    }

    /**
     * @param object  Mongo object
     * @param study   If not null, will be filled with Format, SamplesData and SamplesPosition
     * @param studyId StudyId
     * @return Samples Data
     */
    public List<SampleEntry> convertToDataModelType(Document object, StudyEntry study, int studyId) {
        StudyMetadata studyMetadata = getStudyMetadata(studyId);
        if (studyMetadata == null) {
            return Collections.emptyList();
        }

        final LinkedHashMap<String, Integer> samplesPositionToReturn = getSamplesPosition(studyMetadata.getId());
        Map<String, Integer> sampleIds = studySamplesId.get(studyId);
        Map<Integer, String> sampleNames = studySampleNames.get(studyId);

        boolean excludeGenotypes = !object.containsKey(DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD)
                || studyMetadata.getAttributes().getBoolean(VariantStorageOptions.EXCLUDE_GENOTYPES.key(),
                VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue());
        boolean compressExtraParams = studyMetadata.getAttributes()
                .getBoolean(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(),
                        MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.defaultValue());
        if (samplesPositionToReturn == null || samplesPositionToReturn.isEmpty()) {
            fillStudyEntryFields(study, samplesPositionToReturn, Collections.emptyList(), Collections.emptyList(), excludeGenotypes);
            return Collections.emptyList();
        }

        final Set<Integer> filesWithSamplesData;
        final Map<Integer, Document> files;
        final List<Integer> includeFileIds;
        final Set<Integer> loadedSamples;
        final List<String> extraFields;
        final List<String> sampleDataKeys;
        if (object.containsKey(DocumentToStudyVariantEntryConverter.FILES_FIELD)) {
            List<Document> fileObjects = getList(object, DocumentToStudyVariantEntryConverter.FILES_FIELD);
            includeFileIds = new ArrayList<>(fileObjects.size());
            files = new HashMap<>(fileObjects.size());
            loadedSamples = new HashSet<>();
            filesWithSamplesData = new HashSet<>();
            for (Document fileObject : fileObjects) {
                int fileId = fileObject.get(DocumentToStudyVariantEntryConverter.FILEID_FIELD, Number.class).intValue();
                if (fileId < 0) {
                    fileId = -fileId;
                }
                if (includeFiles.get(studyId).contains(fileId)) {
                    includeFileIds.add(fileId);
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
            includeFileIds = Collections.emptyList();
            filesWithSamplesData = Collections.emptySet();
            loadedSamples = Collections.emptySet();
        }
        extraFields = getExtraFormatFields(studyId, filesWithSamplesData, files);
        sampleDataKeys = getSampleDataKeys(excludeGenotypes, extraFields);
        List<SampleEntry> sampleEntries = new ArrayList<>(samplesPositionToReturn.size());


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

        // Add the samples to the file
        for (String sampleName : samplesPositionToReturn.keySet()) {
            Integer sampleId = sampleIds.get(sampleName);

            String[] values;
            values = new String[sampleDataKeys.size()];
            if (!excludeGenotypes) {
                if (loadedSamples.contains(sampleId)) {
                    values[0] = defaultGenotype;
                } else {
                    values[0] = unknownGenotype;
                }
            }
            sampleEntries.add(new SampleEntry(includeSampleId ? sampleName : null, null, Arrays.asList(values)));
        }


        // Loop through the non-most commmon genotypes, and set their defaultValue
        // in the position specified in the array, such as:
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ]
        // genotypes[41], genotypes[311], etc, will be set to "0|1"
//        Map<Integer, String> idSamples = getIndexedSamplesIdMap(studyId).inverse();
        if (!excludeGenotypes) {
            Document mongoGenotypes = (Document) object.get(DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD);
            for (Map.Entry<String, Object> dbo : mongoGenotypes.entrySet()) {
                final String genotype;
                if (dbo.getKey().equals(UNKNOWN_GENOTYPE)) {
                    // Skip this legacy genotype!
                    continue;
                } else {
                    genotype = genotypeToDataModelType(dbo.getKey());
                }
                for (Integer sampleId : (List<Integer>) dbo.getValue()) {
                    if (sampleNames.containsKey(sampleId)) {
                        sampleEntries.get(samplesPositionToReturn.get(sampleNames.get(sampleId))).getData().set(0, genotype);
                    }
                }
            }
        }
        // Set fileIdx
        for (int fileIndex = 0; fileIndex < includeFileIds.size(); fileIndex++) {
            Integer fileId = includeFileIds.get(fileIndex);
            for (Integer sampleId : getSamplesInFile(studyId, fileId)) {
                String sampleName = getSampleName(studyId, sampleId);
                Integer samplePosition = samplesPositionToReturn.get(sampleName);
                if (samplePosition != null) {
                    sampleEntries.get(samplePosition).setFileIndex(fileIndex);
                }
            }
        }

        if (!extraFields.isEmpty()) {
            for (Integer fid : filesWithSamplesData) {
                Document samplesDataDocument = null;
                if (files.containsKey(fid) && files.get(fid).containsKey(DocumentToStudyVariantEntryConverter.SAMPLE_DATA_FIELD)) {
                    samplesDataDocument = files.get(fid)
                            .get(DocumentToStudyVariantEntryConverter.SAMPLE_DATA_FIELD, Document.class);
                }
                if (samplesDataDocument != null) {
                    int extraFieldPosition;
                    if (excludeGenotypes) {
                        extraFieldPosition = -1; //There are no GT
                    } else {
                        extraFieldPosition = 0; //Skip GT
                    }
                    for (String extraField : extraFields) {
                        extraFieldPosition++;
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
                            Integer samplePosition = samplesPositionToReturn.get(sampleName);
                            if (samplePosition == null) {
                                // The sample on this position is not returned. Skip this value.
                                supplier.get();
                            } else {
                                sampleEntries.get(samplePosition).getData().set(extraFieldPosition, supplier.get());
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
                            Integer samplePosition = samplesPositionToReturn.get(sampleName);
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

        fillStudyEntryFields(study, samplesPositionToReturn, extraFields, sampleEntries, excludeGenotypes);
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
                    Document sampleData = (Document) files.get(fid).get(DocumentToStudyVariantEntryConverter.SAMPLE_DATA_FIELD);
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

}
