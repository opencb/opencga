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

package org.opencb.opencga.storage.mongodb.variant.converters;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.InvalidProtocolBufferException;
import org.bson.Document;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager.Options;
import org.opencb.opencga.storage.mongodb.variant.protobuf.VariantMongoDBProto;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager.MongoDBVariantOptions.DEFAULT_GENOTYPE;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToSamplesConverter /*implements ComplexTypeConverter<VariantSourceEntry, Document>*/ {

    public static final String UNKNOWN_GENOTYPE = "?/?";
    public static final String UNKNOWN_FIELD = ".";

    private final Map<Integer, StudyConfiguration> studyConfigurations;
    private final Map<Integer, BiMap<String, Integer>> __studySamplesId; //Inverse map from "sampleIds". Do not use directly, can be null
    // . Use "getIndexedIdSamplesMap()"
    private final Map<Integer, LinkedHashMap<String, Integer>> __returnedSamplesPosition;
    private final Map<Integer, Set<String>> studyDefaultGenotypeSet;
    private LinkedHashSet<String> returnedSamples;
    private StudyConfigurationManager studyConfigurationManager;
    private String returnedUnknownGenotype;

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
        studyConfigurations = new HashMap<>();
        __studySamplesId = new HashMap<>();
        __returnedSamplesPosition = new HashMap<>();
        studyDefaultGenotypeSet = new HashMap<>();
        returnedSamples = new LinkedHashSet<>();
        studyConfigurationManager = null;
        returnedUnknownGenotype = null;
    }

    /**
     * Create a converter from Document to a Map of samples, providing the list
     * of sample names.
     *
     * @param studyId StudyId
     * @param samples         The list of samples, if any
     * @param defaultGenotype Default genotype
     */
    public DocumentToSamplesConverter(int studyId, List<String> samples, String defaultGenotype) {
        this(studyId, null, samples, defaultGenotype);
    }

    /**
     * Create a converter from Document to a Map of samples, providing the list
     * of sample names.
     *
     * @param studyId StudyId
     * @param fileId File id
     * @param samples The list of samples, if any
     * @param defaultGenotype Default genotype
     */
    public DocumentToSamplesConverter(int studyId, Integer fileId, List<String> samples, String defaultGenotype) {
        this();
        setSamples(studyId, fileId, samples);
        studyConfigurations.get(studyId).getAttributes()
                .put(DEFAULT_GENOTYPE.key(), Collections.singleton(defaultGenotype));
        studyDefaultGenotypeSet.put(studyId, Collections.singleton(defaultGenotype));
    }

    public DocumentToSamplesConverter(StudyConfigurationManager studyConfigurationManager) {
        this();
        this.studyConfigurationManager = studyConfigurationManager;
    }

    public DocumentToSamplesConverter(StudyConfiguration studyConfiguration) {
        this(Collections.singletonList(studyConfiguration));
    }

    public DocumentToSamplesConverter(List<StudyConfiguration> studyConfigurations) {
        this();
        studyConfigurations.forEach(this::addStudyConfiguration);
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

        if (!studyConfigurations.containsKey(studyId) && studyConfigurationManager != null) { // Samples not set as constructor argument,
            // need to query
            QueryResult<StudyConfiguration> queryResult = studyConfigurationManager.getStudyConfiguration(studyId, null);
            if (queryResult.first() == null) {
                logger.warn("DocumentToSamplesConverter.convertToDataModelType StudyConfiguration {studyId: {}} not found! Looking for "
                        + "VariantSource", studyId);

//                if (sourceDbAdaptor != null) {
//                    QueryResult samplesBySource = sourceDbAdaptor
//                            .getSamplesBySource(object.get(DocumentToStudyVariantEntryConverter.FILEID_FIELD).toString(), null);
//                    if (samplesBySource.getResult().isEmpty()) {
//                        logger.warn("DocumentToSamplesConverter.convertToDataModelType VariantSource not found! Can't read sample names");
//                    } else {
//                        setSamples(studyId, null, (List<String>) samplesBySource.getResult().get(0));
//                    }
//                }
            } else {
                addStudyConfiguration(queryResult.first());
            }
        }

        if (!studyConfigurations.containsKey(studyId)) {
            return Collections.emptyList();
        }

        StudyConfiguration studyConfiguration = studyConfigurations.get(studyId);
        Map<String, Integer> sampleIds = getIndexedSamplesIdMap(studyId);
//        final BiMap<String, Integer> samplesPosition = StudyConfiguration.getIndexedSamplesPosition(studyConfiguration);
        final LinkedHashMap<String, Integer> samplesPositionToReturn = getReturnedSamplesPosition(studyConfiguration);

        List<String> extraFields = studyConfiguration.getAttributes()
                .getAsStringList(Options.EXTRA_GENOTYPE_FIELDS.key());
        boolean excludeGenotypes = !object.containsKey(DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD)
                || studyConfiguration.getAttributes().getBoolean(Options.EXCLUDE_GENOTYPES.key(), Options.EXCLUDE_GENOTYPES.defaultValue());
        boolean compressExtraParams = studyConfiguration.getAttributes()
                .getBoolean(Options.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(),
                        Options.EXTRA_GENOTYPE_FIELDS_COMPRESS.defaultValue());
        if (sampleIds == null || sampleIds.isEmpty()) {
            fillStudyEntryFields(study, samplesPositionToReturn, extraFields, Collections.emptyList(), excludeGenotypes);
            return Collections.emptyList();
        }


        List<List<String>> samplesData = new ArrayList<>(sampleIds.size());


        // An array of genotypes is initialized with the most common one
//        String mostCommonGtString = mongoGenotypes.getString("def");
        Set<String> defaultGenotypes = studyDefaultGenotypeSet.get(studyId);
        String mostCommonGtString = defaultGenotypes.isEmpty() ? null : defaultGenotypes.iterator().next();
        if (UNKNOWN_GENOTYPE.equals(mostCommonGtString)) {
            mostCommonGtString = returnedUnknownGenotype;
        }
        if (mostCommonGtString == null) {
            mostCommonGtString = UNKNOWN_GENOTYPE;
        }

        // Add the samples to the file
        for (int i = 0; i < sampleIds.size(); i++) {
            String[] values;
            if (excludeGenotypes) {
                values = new String[extraFields.size()];
            } else {
                values = new String[1 + extraFields.size()];
                values[0] = mostCommonGtString;
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
                    if (returnedUnknownGenotype == null) {
                        continue;
                    }
                    if (defaultGenotypes.contains(returnedUnknownGenotype)) {
                        continue;
                    } else {
                        genotype = returnedUnknownGenotype;
                    }
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

        if (object.containsKey(DocumentToStudyVariantEntryConverter.FILES_FIELD)) {
            List<Document> fileObjects = (List<Document>) object.get(DocumentToStudyVariantEntryConverter.FILES_FIELD);
            Map<Integer, Document> files = fileObjects.stream()
                    .collect(Collectors.toMap(f -> (Integer) f.get(DocumentToStudyVariantEntryConverter.FILEID_FIELD), f -> f));

            for (Integer fid : studyConfiguration.getIndexedFiles()) {
                if (files.containsKey(fid)) {
                    Document sampleDatas = (Document) files.get(fid).get(DocumentToStudyVariantEntryConverter.SAMPLE_DATA_FIELD);

                    int extraFieldPosition;
                    if (excludeGenotypes) {
                        extraFieldPosition = 0; //There are no GT
                    } else {
                        extraFieldPosition = 1; //Skip GT
                    }
                    for (String extraField : extraFields) {
                        extraField = extraField.toLowerCase();
                        byte[] byteArray = sampleDatas == null || !sampleDatas.containsKey(extraField)
                                ? null
                                : sampleDatas.get(extraField, Binary.class).getData();

                        VariantMongoDBProto.OtherFields otherFields = null;
                        if (compressExtraParams && byteArray != null) {
                            try {
                                byteArray = CompressionUtils.decompress(byteArray);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            } catch (DataFormatException ignore) {
                                //It was not actually compressed, so it failed decompressing
                            }
                        }
                        try {
                            if (byteArray != null) {
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
                        for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fid)) {
                            String sampleName = studyConfiguration.getSampleIds().inverse().get(sampleId);
                            Integer samplePosition = samplesPositionToReturn.get(sampleName);
                            if (samplePosition == null) {
                                // The sample on this position is not returned. Skip this value.
                                supplier.get();
                            } else {
                                samplesData.get(samplePosition).set(extraFieldPosition, supplier.get());
                            }
                        }

                        extraFieldPosition++;
                    }
                } else {
                    int extraFieldPosition;
                    if (excludeGenotypes) {
                        extraFieldPosition = 0; //There are no GT
                    } else {
                        extraFieldPosition = 1; //Skip GT
                    }
                    for (int i = 0; i < extraFields.size(); i++) {
                        for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fid)) {
                            String sampleName = studyConfiguration.getSampleIds().inverse().get(sampleId);
                            Integer samplePosition = samplesPositionToReturn.get(sampleName);
                            if (samplePosition != null) {
                                samplesData.get(samplePosition).set(extraFieldPosition, UNKNOWN_FIELD);
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

    private void fillStudyEntryFields(StudyEntry study, LinkedHashMap<String, Integer> samplesPositionToReturn, List<String> extraFields,
                List<List<String>> samplesData, boolean excludeGenotypes) {
        if (study != null) {
            //Set FORMAT
            if (extraFields.isEmpty()) {
                if (excludeGenotypes) {
                    study.setFormat(Collections.emptyList());
                } else {
                    study.setFormat(Collections.singletonList("GT"));
                }
            } else {
                List<String> format = new ArrayList<>(1 + extraFields.size());
                if (!excludeGenotypes) {
                    format.add("GT");
                }
                format.addAll(extraFields);
                study.setFormat(format);
            }

            //Set Samples Position
            study.setSamplesPosition(samplesPositionToReturn);
            //Set Samples Data
            study.setSamplesData(samplesData);
        }
    }

    public Document convertToStorageType(StudyEntry studyEntry, int studyId, int fileId, Document otherFields) {
        return convertToStorageType(studyEntry, studyId, fileId, otherFields, new HashSet<>());
    }

    public Document convertToStorageType(StudyEntry studyEntry, int studyId, int fileId, Document otherFields, Set<String> samplesInFile) {
        Map<String, List<Integer>> genotypeCodes = new HashMap<>();

        final StudyConfiguration studyConfiguration = studyConfigurations.get(studyId);
        boolean excludeGenotypes = studyConfiguration.getAttributes().getBoolean(Options.EXCLUDE_GENOTYPES.key(),
                Options.EXCLUDE_GENOTYPES.defaultValue());
        boolean compressExtraParams = studyConfiguration.getAttributes()
                .getBoolean(Options.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(),
                        Options.EXTRA_GENOTYPE_FIELDS_COMPRESS.defaultValue());

        Set<String> defaultGenotype = studyDefaultGenotypeSet.get(studyId).stream().collect(Collectors.toSet());

        HashBiMap<String, Integer> sampleIds = HashBiMap.create(studyConfiguration.getSampleIds());
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
                genotype = UNKNOWN_GENOTYPE;
            } else {
                genotype = data.get(gtIdx);
            }
            if (genotype == null) {
                genotype = UNKNOWN_GENOTYPE;
            }
//                Genotype g = new Genotype(genotype);
            List<Integer> samplesWithGenotype = genotypeCodes.get(genotype);
            if (samplesWithGenotype == null) {
                samplesWithGenotype = new ArrayList<>();
                genotypeCodes.put(genotype, samplesWithGenotype);
            }
            samplesWithGenotype.add(sampleIds.get(sampleName));
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
        for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fileId)) {
            samplesPosition.put(studyConfiguration.getSampleIds().inverse().get(sampleId), position++);
        }

        List<String> extraFields = studyConfiguration.getAttributes()
                .getAsStringList(Options.EXTRA_GENOTYPE_FIELDS.key());
        List<String> extraFieldsType = studyConfiguration.getAttributes()
                .getAsStringList(Options.EXTRA_GENOTYPE_FIELDS_TYPE.key());

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
                    try {
                        byteArray = CompressionUtils.compress(byteArray);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                otherFields.append(extraField.toLowerCase(), byteArray);
            } // else { Don't set this field }
        }

        return mongoSamples;
    }

    public void setSamples(int studyId, Integer fileId, List<String> samples) {
        int i = 0;
        int size = samples == null ? 0 : samples.size();
        LinkedHashMap<String, Integer> sampleIdsMap = new LinkedHashMap<>(size);
        LinkedHashSet<Integer> sampleIds = new LinkedHashSet<>(size);
        if (samples != null) {
            for (String sample : samples) {
                sampleIdsMap.put(sample, i);
                sampleIds.add(i);
                i++;
            }
        }
        StudyConfiguration studyConfiguration = new StudyConfiguration(studyId, "",
                Collections.<String, Integer>emptyMap(), sampleIdsMap,
                Collections.<String, Integer>emptyMap(),
                Collections.<Integer, Set<Integer>>emptyMap());
        if (fileId != null) {
            studyConfiguration.setSamplesInFiles(Collections.singletonMap(fileId, sampleIds));
        }
        addStudyConfiguration(studyConfiguration);
    }

    public void setReturnedSamples(List<String> returnedSamples) {
        this.returnedSamples = new LinkedHashSet<>(returnedSamples);
        __studySamplesId.clear();
        __returnedSamplesPosition.clear();
    }

    public void addStudyConfiguration(StudyConfiguration studyConfiguration) {
        this.studyConfigurations.put(studyConfiguration.getStudyId(), studyConfiguration);
        this.__studySamplesId.put(studyConfiguration.getStudyId(), null);

        Set defGenotypeSet = studyConfiguration.getAttributes().get(DEFAULT_GENOTYPE.key(), Set.class);
        if (defGenotypeSet == null) {
            List<String> defGenotype = studyConfiguration.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key());
            if (defGenotype.size() == 0) {
                defGenotypeSet = Collections.<String>emptySet();
            } else if (defGenotype.size() == 1) {
                defGenotypeSet = Collections.singleton(defGenotype.get(0));
            } else {
                defGenotypeSet = new LinkedHashSet<>(defGenotype);
            }
        }
        this.studyDefaultGenotypeSet.put(studyConfiguration.getStudyId(), defGenotypeSet);
    }

    public String getReturnedUnknownGenotype() {
        return returnedUnknownGenotype;
    }

    public void setReturnedUnknownGenotype(String returnedUnknownGenotype) {
        this.returnedUnknownGenotype = returnedUnknownGenotype;
    }

    /**
     * Lazy usage of loaded samplesIdMap.
     **/
    private BiMap<String, Integer> getIndexedSamplesIdMap(int studyId) {
        BiMap<String, Integer> sampleIds;
        if (this.__studySamplesId.get(studyId) == null) {
            StudyConfiguration studyConfiguration = studyConfigurations.get(studyId);
            sampleIds = StudyConfiguration.getIndexedSamples(studyConfiguration);
            if (!returnedSamples.isEmpty()) {
                BiMap<String, Integer> returnedSampleIds = HashBiMap.create();
                sampleIds.entrySet().stream()
                        //ReturnedSamples could be sampleNames or sampleIds as a string
                        .filter(e -> returnedSamples.contains(e.getKey()) || returnedSamples.contains(e.getValue().toString()))
                        .forEach(stringIntegerEntry -> returnedSampleIds.put(stringIntegerEntry.getKey(), stringIntegerEntry.getValue()));
                sampleIds = returnedSampleIds;
            }
            this.__studySamplesId.put(studyId, sampleIds);
        } else {
            sampleIds = this.__studySamplesId.get(studyId);
        }

        return sampleIds;
    }

    private LinkedHashMap<String, Integer> getReturnedSamplesPosition(StudyConfiguration studyConfiguration) {
        if (!__returnedSamplesPosition.containsKey(studyConfiguration.getStudyId())) {
            LinkedHashMap<String, Integer> samplesPosition;
            samplesPosition = StudyConfiguration.getReturnedSamplesPosition(studyConfiguration,
                    this.returnedSamples, sc -> getIndexedSamplesIdMap(sc.getStudyId()));
            __returnedSamplesPosition.put(studyConfiguration.getStudyId(), samplesPosition);
        }
        return __returnedSamplesPosition.get(studyConfiguration.getStudyId());
    }

    public static LinkedHashMap<String, Integer> getReturnedSamplesPosition(
            StudyConfiguration studyConfiguration,
            LinkedHashSet<String> returnedSamples) {
        return StudyConfiguration.getReturnedSamplesPosition(studyConfiguration, returnedSamples, StudyConfiguration::getIndexedSamples);
    }



    public static String genotypeToDataModelType(String genotype) {
        return genotype.replace("-1", ".");
    }

    public static String genotypeToStorageType(String genotype) {
        return genotype.replace(".", "-1");
    }

    public List<String> getFormat(int studyId) {
        StudyConfiguration studyConfiguration = studyConfigurations.get(studyId);
        List<String> extraFields = studyConfiguration.getAttributes().getAsStringList(Options.EXTRA_GENOTYPE_FIELDS
                .key());
        if (extraFields.isEmpty()) {
            return Collections.singletonList("GT");
        } else {
            List<String> format = new ArrayList<>(1 + extraFields.size());
            format.addAll(extraFields);
            return format;
        }
    }
}
