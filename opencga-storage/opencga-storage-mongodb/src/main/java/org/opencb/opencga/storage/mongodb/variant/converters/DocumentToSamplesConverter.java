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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.protobuf.VariantMongoDBProto;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.UNKNOWN_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions.DEFAULT_GENOTYPE;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToSamplesConverter extends AbstractDocumentConverter {

    public static final String UNKNOWN_FIELD = ".";

    private final Map<Integer, StudyMetadata> studyMetadatas;
    private final Map<Integer, BiMap<String, Integer>> __studySamplesId; //Inverse map from "sampleIds". Do not use directly, can be null
    // . Use "getIndexedIdSamplesMap()"
    private final Map<Integer, LinkedHashMap<String, Integer>> __samplesPosition;
    private final Map<Integer, String> __sampleNames;
    private final Map<String, Integer> __sampleIds;
    private final Map<Integer, List<Integer>> __samplesInFile;
    private final Map<Integer, Set<String>> studyDefaultGenotypeSet;
    private Map<Integer, LinkedHashSet<Integer>> includeSamples;
    private Map<Integer, List<Integer>> includeFiles;
    private VariantStorageMetadataManager metadataManager;
    private String unknownGenotype;
    private List<String> expectedExtraFields;

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


    /**
     * Create a converter from a Map of samples to Document entities.
     **/
    DocumentToSamplesConverter() {
        studyMetadatas = new ConcurrentHashMap<>();
        __studySamplesId = new ConcurrentHashMap<>();
        __samplesPosition = new ConcurrentHashMap<>();
        __sampleNames = new ConcurrentHashMap<>();
        __sampleIds = new ConcurrentHashMap<>();
        __samplesInFile = new ConcurrentHashMap<>();
        studyDefaultGenotypeSet = new ConcurrentHashMap<>();
        includeSamples = Collections.emptyMap();
        metadataManager = null;
        unknownGenotype = UNKNOWN_GENOTYPE;
    }

    public DocumentToSamplesConverter(VariantStorageMetadataManager metadataManager) {
        this();
        this.metadataManager = metadataManager;
    }

    public DocumentToSamplesConverter(VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata) {
        this();
        this.metadataManager = metadataManager;
        addStudyMetadata(studyMetadata);
    }

    public DocumentToSamplesConverter(VariantStorageMetadataManager metadataManager, VariantQueryProjection variantQueryProjection) {
        this();
        this.metadataManager = metadataManager;
        setIncludeSamples(variantQueryProjection.getSamples());
        includeFiles = variantQueryProjection.getFiles();
        for (StudyMetadata studyMetadata : variantQueryProjection.getStudyMetadatas().values()) {
            addStudyMetadata(studyMetadata);
        }
    }

    @Deprecated
    public DocumentToSamplesConverter(List<? extends StudyMetadata> list) {
        this();
        list.forEach(this::addStudyMetadata);
    }

    public List<List<String>> convertToDataModelType(Document object, int studyId) {
        return convertToDataModelType(object, null, studyId);
    }

    /**
     * @param object  Mongo object
     * @param study   If not null, will be filled with Format, SamplesData and SamplesPosition
     * @param studyId StudyId
     * @return Samples Data
     */
    public List<List<String>> convertToDataModelType(Document object, StudyEntry study, int studyId) {
        StudyMetadata studyMetadata = getStudyMetadata(studyId);
        if (studyMetadata == null) {
            return Collections.emptyList();
        }

        BiMap<String, Integer> sampleIds = getIndexedSamplesIdMap(studyId);
        final LinkedHashMap<String, Integer> samplesPositionToReturn = getSamplesPosition(studyMetadata);

        boolean excludeGenotypes = !object.containsKey(DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD)
                || studyMetadata.getAttributes().getBoolean(VariantStorageOptions.EXCLUDE_GENOTYPES.key(),
                VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue());
        boolean compressExtraParams = studyMetadata.getAttributes()
                .getBoolean(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(),
                        MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.defaultValue());
        if (sampleIds == null || sampleIds.isEmpty()) {
            fillStudyEntryFields(study, samplesPositionToReturn, Collections.emptyList(), Collections.emptyList(), excludeGenotypes);
            return Collections.emptyList();
        }

        final Set<Integer> filesWithSamplesData;
        final Map<Integer, Document> files;
        final List<Integer> includeFileIds;
        final Set<Integer> loadedSamples;
        final List<String> extraFields;
        final List<String> format;
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
        format = getFormat(excludeGenotypes, extraFields);
        List<List<String>> samplesData = new ArrayList<>(sampleIds.size());


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

        int sampleIdIdx = format.indexOf(VariantQueryParser.SAMPLE_ID);
        // Add the samples to the file
        for (String sampleName : samplesPositionToReturn.keySet()) {
            Integer sampleId = sampleIds.get(sampleName);

            String[] values;
            values = new String[format.size()];
            if (!excludeGenotypes) {
                if (loadedSamples.contains(sampleId)) {
                    values[0] = defaultGenotype;
                } else {
                    values[0] = unknownGenotype;
                }
            }
            if (sampleIdIdx >= 0) {
                values[sampleIdIdx] = sampleName;
            }
            samplesData.add(Arrays.asList(values));
        }


        // Loop through the non-most commmon genotypes, and set their defaultValue
        // in the position specified in the array, such as:
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ]
        // genotypes[41], genotypes[311], etc, will be set to "0|1"
        Map<Integer, String> idSamples = getIndexedSamplesIdMap(studyId).inverse();
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
                    if (idSamples.containsKey(sampleId)) {
                        samplesData.get(samplesPositionToReturn.get(idSamples.get(sampleId))).set(0, genotype);
                    }
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
                        if (extraField.equals(VariantQueryParser.SAMPLE_ID)) {
                            continue;
                        }
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
                            if (VariantQueryParser.FILE_ID.toLowerCase().equals(extraField)) {
                                if (includeFileIds.contains(fid)) {
                                    String fileName = metadataManager.getFileName(studyId, fid);
                                    supplier = () -> fileName;
                                } else {
                                    supplier = () -> UNKNOWN_FIELD;
                                }
                            } else if (VariantQueryParser.FILE_IDX.toLowerCase().equals(extraField)) {
                                int fileIdx = includeFileIds.indexOf(fid);
                                if (fileIdx < 0) {
                                    supplier = () -> UNKNOWN_FIELD;
                                } else {
                                    supplier = () -> String.valueOf(fileIdx);
                                }
                            } else {
                                supplier = () -> UNKNOWN_FIELD;
                            }
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
                                samplesData.get(samplePosition).set(extraFieldPosition, supplier.get());
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
                                if (samplesData.get(samplePosition).get(extraFieldPosition) == null) {
                                    samplesData.get(samplePosition).set(extraFieldPosition, UNKNOWN_FIELD);
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

        fillStudyEntryFields(study, samplesPositionToReturn, extraFields, samplesData, excludeGenotypes);
        return samplesData;
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
                List<List<String>> samplesData, boolean excludeGenotypes) {
        if (study != null) {
            //Set FORMAT
            study.setFormat(getFormat(excludeGenotypes, extraFields));

            //Set Samples Position
            study.setSamplesPosition(samplesPositionToReturn);
            //Set Samples Data
            study.setSamplesData(samplesData);
        }
    }

    public Document convertToStorageType(StudyEntry studyEntry, int studyId, Document otherFields, LinkedHashSet<String> samplesInFile) {
        Map<String, List<Integer>> genotypeCodes = new HashMap<>();

        final StudyMetadata studyMetadata = getStudyMetadata(studyId);
        boolean excludeGenotypes = studyMetadata.getAttributes().getBoolean(VariantStorageOptions.EXCLUDE_GENOTYPES.key(),
                VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue());
        boolean compressExtraParams = studyMetadata.getAttributes()
                .getBoolean(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(),
                        MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.defaultValue());

        Set<String> defaultGenotype = studyDefaultGenotypeSet.get(studyId).stream().collect(Collectors.toSet());

        // Classify samples by genotype
        int sampleIdx = 0;
        Integer gtIdx = studyEntry.getFormatPositions().get("GT");
        List<String> studyEntryOrderedSamplesName = studyEntry.getOrderedSamplesName();
        for (List<String> data : studyEntry.getSamplesData()) {
            String sampleName = studyEntryOrderedSamplesName.get(sampleIdx);
            sampleIdx++;
            if (!samplesInFile.contains(sampleName)) {
                continue;
            }
            String genotype;
            if (gtIdx == null) {
                genotype = ".";
            } else {
                genotype = data.get(gtIdx);
            }
            if (genotype == null) {
                genotype = ".";
            }
//                Genotype g = new Genotype(genotype);
            List<Integer> samplesWithGenotype = genotypeCodes.get(genotype);
            if (samplesWithGenotype == null) {
                samplesWithGenotype = new ArrayList<>();
                genotypeCodes.put(genotype, samplesWithGenotype);
            }
            samplesWithGenotype.add(getSampleId(studyId, sampleName));
        }

        // In Mongo, samples are stored in a map, classified by their genotype.
        // The most common genotype will be marked as "default" and the specific
        // positions where it is shown will not be stored. Example from 1000G:
        // "def" : 0|0,
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ],
        // "1|0" : [ 262, 290, 300, 331, 343, 369, 374, 391, 879, 918, 930 ]
        Document mongoSamples = new Document();
        Document mongoGenotypes = new Document();
        for (Map.Entry<String, List<Integer>> entry : genotypeCodes.entrySet()) {
            String genotypeStr = genotypeToStorageType(entry.getKey());
            if (!defaultGenotype.contains(entry.getKey())) {
                mongoGenotypes.append(genotypeStr, entry.getValue());
            }
        }

        if (!excludeGenotypes) {
            mongoSamples.append(DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD, mongoGenotypes);
        }


        //Position for samples in this file
        HashBiMap<String, Integer> samplesPosition = HashBiMap.create();
        int position = 0;
        for (String sample : samplesInFile) {
            samplesPosition.put(sample, position++);
        }

        List<String> extraFields = studyMetadata.getAttributes()
                .getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key());
        List<String> extraFieldsType = studyMetadata.getAttributes()
                .getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key());

        for (int i = 0; i < extraFields.size(); i++) {
            String extraField = extraFields.get(i);
            String extraFieldType = i < extraFieldsType.size() ? extraFieldsType.get(i) : "String";


            VariantMongoDBProto.OtherFields.Builder builder = VariantMongoDBProto.OtherFields.newBuilder();
//            List<Object> values = new ArrayList<>(samplesPosition.size());
//            for (int size = samplesPosition.size(); size > 0; size--) {
//                values.add(UNKNOWN_FIELD);
//            }
            sampleIdx = 0;
            if (studyEntry.getFormatPositions().containsKey(extraField)) {
                Integer formatIdx = studyEntry.getFormatPositions().get(extraField);
                for (List<String> sampleData : studyEntry.getSamplesData()) {
                    String sampleName = studyEntryOrderedSamplesName.get(sampleIdx);
                    sampleIdx++;
                    if (!samplesInFile.contains(sampleName)) {
                        continue;
                    }
//                    Integer index = samplesPosition.get(sampleName);
                    String stringValue = sampleData.get(formatIdx);
//                    Object value;
//                    if (NumberUtils.isNumber(stringValue)) {
//                        try {
//                            value = Integer.parseInt(stringValue);
//                        } catch (NumberFormatException e) {
//                            try {
//                                value = Double.parseDouble(stringValue);
//                            } catch (NumberFormatException e2) {
//                                value = stringValue;
//                            }
//                        }
//                    } else {
//                        value = stringValue;
//                    }
                    switch (extraFieldType) {
                        case "Integer": {
                            builder.addIntValues(INTEGER_COMPLEX_TYPE_CONVERTER.convertToStorageType(stringValue));
                            break;
                        }
                        case "Float": {
                            builder.addFloatValues(FLOAT_COMPLEX_TYPE_CONVERTER.convertToStorageType(stringValue));
                            break;
                        }
                        case "String":
                        default:
                            builder.addStringValues(stringValue);
                            break;
                    }
                }

                byte[] byteArray = builder.build().toByteArray();
                if (compressExtraParams) {
                    if (byteArray.length > 50) {
                        try {
                            byteArray = CompressionUtils.compress(byteArray);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }
                otherFields.append(extraField.toLowerCase(), byteArray);
            } // else { Don't set this field }
        }

        return mongoSamples;
    }

    public void setIncludeSamples(Map<Integer, List<Integer>> includeSamples) {
        this.includeSamples = includeSamples == null ? null : new HashMap<>(includeSamples.size());
        if (includeSamples != null) {
            this.includeSamples = new HashMap<>();
            includeSamples.forEach((studyId, sampleIds) -> this.includeSamples.put(studyId, new LinkedHashSet<>(sampleIds)));
        } else {
            this.includeSamples = null;
        }
        __studySamplesId.clear();
        __samplesPosition.clear();
    }

    public void addStudyMetadata(StudyMetadata studyMetadata) {
        this.studyMetadatas.put(studyMetadata.getId(), studyMetadata);
        this.__studySamplesId.remove(studyMetadata.getId());

        Set defGenotypeSet = studyMetadata.getAttributes().get(DEFAULT_GENOTYPE.key(), Set.class);
        if (defGenotypeSet == null) {
            List<String> defGenotype = studyMetadata.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key());
            if (defGenotype.size() == 0) {
                defGenotypeSet = Collections.<String>emptySet();
            } else if (defGenotype.size() == 1) {
                defGenotypeSet = Collections.singleton(defGenotype.get(0));
            } else {
                defGenotypeSet = new LinkedHashSet<>(defGenotype);
            }
        }
        this.studyDefaultGenotypeSet.put(studyMetadata.getId(), defGenotypeSet);
    }

    public String getUnknownGenotype() {
        return unknownGenotype;
    }

    public void setUnknownGenotype(String unknownGenotype) {
        this.unknownGenotype = unknownGenotype;
    }

    private List<String> getFormat(boolean excludeGenotypes, List<String> extraFields) {
        List<String> format;
        if (extraFields.isEmpty()) {
            if (excludeGenotypes) {
                format = Collections.emptyList();
            } else {
                format = Collections.singletonList("GT");
            }
        } else {
            format = new ArrayList<>(1 + extraFields.size());
            if (!excludeGenotypes) {
                format.add("GT");
            }
            format.addAll(extraFields);
        }
        return format;
    }

    public void setFormat(List<String> format) {
        if (format != null && format.contains(VariantQueryUtils.GT)) {
            this.expectedExtraFields = new ArrayList<>(format);
            this.expectedExtraFields.remove(VariantQueryUtils.GT);
        } else {
            this.expectedExtraFields = format;
        }
    }

    private StudyMetadata getStudyMetadata(int studyId) {
        return studyMetadatas.computeIfAbsent(studyId, s -> {
            if (metadataManager != null) {
                StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyId);
                addStudyMetadata(studyMetadata);
                return studyMetadata;
            } else {
                return null;
            }
        });
    }

    /**
     * Lazy usage of loaded samplesIdMap.
     **/
    private BiMap<String, Integer> getIndexedSamplesIdMap(int studyId) {
        BiMap<String, Integer> sampleIds;
        if (this.__studySamplesId.get(studyId) == null) {
            sampleIds = metadataManager.getIndexedSamplesMap(studyId);
            if (includeSamples != null && includeSamples.containsKey(studyId)) {
                BiMap<String, Integer> includeSampleIds = HashBiMap.create();
                sampleIds.entrySet().stream()
                        //ReturnedSamples could be sampleNames or sampleIds as a string
                        .filter(e -> includeSamples.get(studyId).contains(e.getValue()))
                        .forEach(stringIntegerEntry -> includeSampleIds.put(stringIntegerEntry.getKey(), stringIntegerEntry.getValue()));
                sampleIds = includeSampleIds;
            }
            this.__studySamplesId.put(studyId, sampleIds);
        } else {
            sampleIds = this.__studySamplesId.get(studyId);
        }

        return sampleIds;
    }

    private LinkedHashMap<String, Integer> getSamplesPosition(StudyMetadata studyMetadata) {
        int studyId = studyMetadata.getId();
        return __samplesPosition.computeIfAbsent(studyId,
                s -> metadataManager.getSamplesPosition(studyMetadata, this.includeSamples.get(studyId)));
    }

    private String getSampleName(int studyId, int sampleId) {
        return __sampleNames.computeIfAbsent(sampleId, s -> metadataManager.getSampleName(studyId, sampleId));
    }

    private int getSampleId(int studyId, String sampleName) {
        return __sampleIds.computeIfAbsent(sampleName, s -> metadataManager.getSampleId(studyId, sampleName));
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
