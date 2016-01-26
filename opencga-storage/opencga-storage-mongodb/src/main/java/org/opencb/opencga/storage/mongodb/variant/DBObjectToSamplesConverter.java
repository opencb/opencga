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

package org.opencb.opencga.storage.mongodb.variant;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.commons.lang3.math.NumberUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.mongodb.variant.DBObjectToStudyVariantEntryConverter.*;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToSamplesConverter /*implements ComplexTypeConverter<VariantSourceEntry, DBObject>*/ {

    public static final String UNKNOWN_GENOTYPE = "?/?";
    public static final Object UNKNOWN_FIELD = -1;

    private final Map<Integer, StudyConfiguration> studyConfigurations;
    private final Map<Integer, BiMap<String, Integer>> __studySamplesId; //Inverse map from "sampleIds". Do not use directly, can be null. Use "getIndexedIdSamplesMap()"
    private final Map<Integer, LinkedHashMap<String, Integer>> __returnedSamplesPosition;
    private final Map<Integer, Set<String>> studyDefaultGenotypeSet;
    private LinkedHashSet<String> returnedSamples;
    private VariantSourceDBAdaptor sourceDbAdaptor;
    private StudyConfigurationManager studyConfigurationManager;
    private String returnedUnknownGenotype;

    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(DBObjectToSamplesConverter.class.getName());

    /**
     * Create a converter from a Map of samples to DBObject entities.
     **/
    DBObjectToSamplesConverter() {
        studyConfigurations = new HashMap<>();
        __studySamplesId = new HashMap<>();
        __returnedSamplesPosition = new HashMap<>();
        studyDefaultGenotypeSet = new HashMap<>();
        returnedSamples = new LinkedHashSet<>();
        studyConfigurationManager = null;
        returnedUnknownGenotype = null;
    }

    /**
     * Create a converter from DBObject to a Map of samples, providing the list
     * of sample names.
     *
     * @param samples The list of samples, if any
     * @param defaultGenotype
     */
    public DBObjectToSamplesConverter(int studyId, List<String> samples, String defaultGenotype) {
        this(studyId, null, samples, defaultGenotype);
    }

    /**
     * Create a converter from DBObject to a Map of samples, providing the list
     * of sample names.
     *
     * @param fileId
     * @param samples The list of samples, if any
     * @param defaultGenotype
     */
    public DBObjectToSamplesConverter(int studyId, Integer fileId, List<String> samples, String defaultGenotype) {
        this();
        setSamples(studyId, fileId, samples);
        studyConfigurations.get(studyId).getAttributes().put(MongoDBVariantStorageManager.DEFAULT_GENOTYPE, Collections.singleton(defaultGenotype));
        studyDefaultGenotypeSet.put(studyId, Collections.singleton(defaultGenotype));
    }

    /**
     *
     */
    public DBObjectToSamplesConverter(StudyConfigurationManager studyConfigurationManager, VariantSourceDBAdaptor variantSourceDBAdaptor) {
        this();
        this.sourceDbAdaptor = variantSourceDBAdaptor;
        this.studyConfigurationManager = studyConfigurationManager;
    }

    /**
     *
     */
    public DBObjectToSamplesConverter(StudyConfiguration studyConfiguration) {
        this(Collections.singletonList(studyConfiguration));
    }

    /**
     *
     */
    public DBObjectToSamplesConverter(List<StudyConfiguration> studyConfigurations) {
        this();
        studyConfigurations.forEach(this::addStudyConfiguration);
    }

    public List<List<String>> convertToDataModelType(DBObject object, int studyId) {
        return convertToDataModelType(object, null, studyId);
    }

    /**
     *
     * @param object Mongo object
     * @param study  If not null, will be filled with Format, SamplesData and SamplesPosition
     * @param studyId StudyId
     * @return Samples Data
     */
    public List<List<String>> convertToDataModelType(DBObject object, StudyEntry study, int studyId) {

        if (!studyConfigurations.containsKey(studyId) && studyConfigurationManager != null) { // Samples not set as constructor argument, need to query
            QueryResult<StudyConfiguration> queryResult = studyConfigurationManager.getStudyConfiguration(studyId, null);
            if(queryResult.first() == null) {
                logger.warn("DBObjectToSamplesConverter.convertToDataModelType StudyConfiguration {studyId: {}} not found! Looking for VariantSource", studyId);

                if (sourceDbAdaptor != null) {
                    QueryResult samplesBySource = sourceDbAdaptor.getSamplesBySource(object.get(FILEID_FIELD).toString(), null);
                    if(samplesBySource.getResult().isEmpty()) {
                        logger.warn("DBObjectToSamplesConverter.convertToDataModelType VariantSource not found! Can't read sample names");
                    } else {
                        setSamples(studyId, null, (List<String>) samplesBySource.getResult().get(0));
                    }
                }
            } else {
                addStudyConfiguration(queryResult.first());
            }
        }

        if (!studyConfigurations.containsKey(studyId)) {
            return Collections.emptyList();
        }

        StudyConfiguration studyConfiguration = studyConfigurations.get(studyId);
        Map<String, Integer> sampleIds = getIndexedSamplesIdMap(studyId);
        final BiMap<String, Integer> samplesPosition = StudyConfiguration.getIndexedSamplesPosition(studyConfiguration);
        final LinkedHashMap<String, Integer> samplesPositionToReturn = getReturnedSamplesPosition(studyConfiguration);
        List<String> extraFields = studyConfiguration.getAttributes().getAsStringList(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key());
        if (sampleIds == null || sampleIds.isEmpty()) {
            fillStudyEntryFields(study, samplesPositionToReturn, extraFields, Collections.emptyList());
            return Collections.emptyList();
        }

        BasicDBObject mongoGenotypes = (BasicDBObject) object.get(GENOTYPES_FIELD);

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
            String[] values = new String[1 + extraFields.size()];
            values[0] = mostCommonGtString;
            samplesData.add(Arrays.asList(values));
        }


        // Loop through the non-most commmon genotypes, and set their defaultValue
        // in the position specified in the array, such as:
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ]
        // genotypes[41], genotypes[311], etc, will be set to "0|1"
        Map<Integer, String> idSamples = getIndexedSamplesIdMap(studyId).inverse();
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

        int extraFieldPosition = 1; //Skip GT
        for (String extraField : extraFields) {
            if (object.containsField(extraField.toLowerCase())) {
                List values = (List) object.get(extraField.toLowerCase());

                for (int i = 0; i < values.size(); i++) {
                    Object value = values.get(i);
                    String sampleName = samplesPosition.inverse().get(i);
                    samplesData.get(samplesPositionToReturn.get(sampleName)).set(extraFieldPosition, value.toString());
                }
            }
            extraFieldPosition++;
        }

        fillStudyEntryFields(study, samplesPositionToReturn, extraFields, samplesData);
        return samplesData;
    }

    private void fillStudyEntryFields(StudyEntry study, LinkedHashMap<String, Integer> samplesPositionToReturn, List<String> extraFields, List<List<String>> samplesData) {
        //If != null, just return samplesData
        if (study != null) {
            //Set FORMAT
            if (extraFields.isEmpty()) {
                study.setFormat(Collections.singletonList("GT"));
            } else {
                List<String> format = new ArrayList<>(1 + extraFields.size());
                format.add("GT");
                format.addAll(extraFields);
                study.setFormat(format);
            }

            //Set Samples Position
            study.setSamplesPosition(samplesPositionToReturn);
            //Set Samples Data
            study.setSamplesData(samplesData);
        }
    }

    public DBObject convertToStorageType(StudyEntry studyEntry, int studyId, int fileId) {
        Map<String, List<Integer>> genotypeCodes = new HashMap<>();

        StudyConfiguration studyConfiguration = studyConfigurations.get(studyId);

        HashBiMap<String, Integer> sampleIds = HashBiMap.create(studyConfiguration.getSampleIds());
        // Classify samples by genotype
        int sampleIdx = 0;
        Integer gtIdx = studyEntry.getFormatPositions().get("GT");
        List<String> studyEntryOrderedSamplesName = studyEntry.getOrderedSamplesName();
        for (List<String> data : studyEntry.getSamplesData()) {
            String genotype = data.get(gtIdx);
            String sampleName = studyEntryOrderedSamplesName.get(sampleIdx);
            if (genotype != null) {
//                Genotype g = new Genotype(genotype);
                List<Integer> samplesWithGenotype = genotypeCodes.get(genotype);
                if (samplesWithGenotype == null) {
                    samplesWithGenotype = new ArrayList<>();
                    genotypeCodes.put(genotype, samplesWithGenotype);
                }
                samplesWithGenotype.add(sampleIds.get(sampleName));
            }
            sampleIdx++;
        }


        Set<String> defaultGenotype = studyDefaultGenotypeSet.get(studyId).stream().collect(Collectors.toSet());

        // In Mongo, samples are stored in a map, classified by their genotype.
        // The most common genotype will be marked as "default" and the specific
        // positions where it is shown will not be stored. Example from 1000G:
        // "def" : 0|0,
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ],
        // "1|0" : [ 262, 290, 300, 331, 343, 369, 374, 391, 879, 918, 930 ]
        BasicDBObject mongoSamples = new BasicDBObject();
        BasicDBObject mongoGenotypes = new BasicDBObject();
        for (Map.Entry<String, List<Integer>> entry : genotypeCodes.entrySet()) {
            String genotypeStr = genotypeToStorageType(entry.getKey());
            if (!defaultGenotype.contains(entry.getKey())) {
                mongoGenotypes.append(genotypeStr, entry.getValue());
            }
        }
        mongoSamples.append(GENOTYPES_FIELD, mongoGenotypes);


        //Position for samples in this file
        HashBiMap<String, Integer> samplesPosition = HashBiMap.create();
        int position = 0;
        for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fileId)) {
            samplesPosition.put(studyConfiguration.getSampleIds().inverse().get(sampleId), position++);
        }


        List<String> extraFields = studyConfiguration.getAttributes().getAsStringList(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key());
        for (String extraField : extraFields) {
            List<Object> values = new ArrayList<>(samplesPosition.size());
            for (int size = samplesPosition.size(); size > 0; size--) {
                values.add(UNKNOWN_FIELD);
            }
            sampleIdx = 0;
            if (studyEntry.getFormatPositions().containsKey(extraField)) {
                Integer fieldIdx = studyEntry.getFormatPositions().get(extraField);
                for (List<String> sampleData : studyEntry.getSamplesData()) {
                    String sampleName = studyEntryOrderedSamplesName.get(sampleIdx);
                    Integer index = samplesPosition.get(sampleName);
                    Object value;
                    String stringValue = sampleData.get(fieldIdx);
                    if (NumberUtils.isNumber(stringValue)) {
                        try {
                            value = Integer.parseInt(stringValue);
                        } catch (NumberFormatException e) {
                            try {
                                value = Double.parseDouble(stringValue);
                            } catch (NumberFormatException e2) {
                                value = stringValue;
                            }
                        }
                    } else {
                        value = stringValue;
                    }
                    values.set(index, value);
                    sampleIdx++;
                }
            }
            mongoSamples.append(extraField.toLowerCase(), values);
        }

        return mongoSamples;
    }


    public void setSamples(int studyId, Integer fileId, List<String> samples) {
        int i = 0;
        int size = samples == null? 0 : samples.size();
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

        Set defGenotypeSet = studyConfiguration.getAttributes().get(MongoDBVariantStorageManager.DEFAULT_GENOTYPE, Set.class);
        if (defGenotypeSet == null) {
            List<String> defGenotype = studyConfiguration.getAttributes().getAsStringList(MongoDBVariantStorageManager.DEFAULT_GENOTYPE);
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


    private StudyEntry getLegacyNoncompressedSamples(BasicDBObject object) {
        StudyEntry studyEntry = new StudyEntry(object.get(FILEID_FIELD).toString(),
                object.get(STUDYID_FIELD).toString());

        BasicDBObject mongoGenotypes = (BasicDBObject) object.get(GENOTYPES_FIELD);
        for (Object entry : mongoGenotypes.toMap().entrySet()) {
            Map.Entry sample = (Map.Entry) entry;
            studyEntry.addSampleData(sample.getKey().toString(), ((DBObject) sample.getValue()).toMap());
        }
//            for (String sample : samples) {
//                Map<String, String> sampleData = ((DBObject) mongoGenotypes.get(sample)).toMap();
//                fileWithSamples.addSampleData(sample, sampleData);
//                System.out.println("Sample processed: " + sample);
//            }
        return studyEntry;
    }

    private DBObject getLegacyNoncompressedSamples(StudyEntry object) {
        BasicDBObject mongoSamples = new BasicDBObject();
        for (Map.Entry<String, Map<String, String>> entry : object.getSamplesDataAsMap().entrySet()) {
            BasicDBObject sampleData = new BasicDBObject();
            for (Map.Entry<String, String> sampleEntry : entry.getValue().entrySet()) {
                sampleData.put(sampleEntry.getKey(), sampleEntry.getValue());
            }
            mongoSamples.put(entry.getKey(), sampleData);
        }
        return mongoSamples;
    }

    public static String genotypeToDataModelType(String genotype) {
        return genotype.replace("-1", ".");
    }

    public static String genotypeToStorageType(String genotype) {
        return genotype.replace(".", "-1");
    }

    public List<String> getFormat(int studyId) {
        StudyConfiguration studyConfiguration = studyConfigurations.get(studyId);
        List<String> extraFields = studyConfiguration.getAttributes().getAsStringList(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key());
        if (extraFields.isEmpty()) {
            return Collections.singletonList("GT");
        } else {
            List<String> format = new ArrayList<>(1 + extraFields.size());
            format.addAll(extraFields);
            return format;
        }
    }
}
