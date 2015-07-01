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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.ComplexTypeConverter;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.slf4j.LoggerFactory;

import static org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceEntryConverter.FILEID_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceEntryConverter.GENOTYPES_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceEntryConverter.STUDYID_FIELD;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToSamplesConverter /*implements ComplexTypeConverter<VariantSourceEntry, DBObject>*/ {

    public static final String UNKNOWN_GENOTYPE = "?";

    private Map<Integer, Map<String, Integer>> studySampleIds;
    private Map<Integer, Map<Integer, String>> __studyIdSamples; //Inverse map from "sampleIds". Do not use directly, can be null. Use "getIdSamplesMap()"
    private VariantSourceDBAdaptor sourceDbAdaptor;
    private StudyConfigurationManager studyConfigurationManager;
    @Deprecated private boolean compressDefaultGenotype;
    private Genotype defaultGenotype;

    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(DBObjectToSamplesConverter.class.getName());

    /**
     * Create a converter from a Map of samples to DBObject entities.
     *
     * @param compressDefaultGenotype Whether to compress samples default genotype or not
     */
    DBObjectToSamplesConverter(boolean compressDefaultGenotype) {
        this.__studyIdSamples = null;
        this.studySampleIds = null;
        this.sourceDbAdaptor = null;
        this.compressDefaultGenotype = compressDefaultGenotype;
        this.studyConfigurationManager = null;
    }

    /**
     * Create a converter from DBObject to a Map of samples, providing the list
     * of sample names.
     *
     * @param samples The list of samples, if any
     */
    public DBObjectToSamplesConverter(int studyId, List<String> samples) {
        this(true);
        setSamples(studyId, samples);
    }

    /**
     * Create a converter from DBObject to a Map of samples, providing a map
     * of sample names to the corresponding sample id.
     *
     * @param sampleIds Map of samples to sampleId
     */
    public DBObjectToSamplesConverter(int studyId, boolean compressDefaultGenotype, Map<String, Integer> sampleIds) {
        this(compressDefaultGenotype);
        setSampleIds(studyId, sampleIds);
    }

    /**
     *
     */
    public DBObjectToSamplesConverter(StudyConfigurationManager studyConfigurationManager, VariantSourceDBAdaptor variantSourceDBAdaptor) {
        this(true);
        this.sourceDbAdaptor = variantSourceDBAdaptor;
        this.studyConfigurationManager = studyConfigurationManager;
    }

    /**
     *
     */
    public DBObjectToSamplesConverter(Genotype defaultGenotype, StudyConfiguration studyConfiguration) {
        this(defaultGenotype != null, Collections.singletonList(studyConfiguration));
        this.defaultGenotype = defaultGenotype;
    }

    /**
     *
     */
    public DBObjectToSamplesConverter(boolean compressDefaultGenotype, List<StudyConfiguration> studyConfigurations) {
        this(compressDefaultGenotype);
        for (StudyConfiguration studyConfiguration : studyConfigurations) {
            setSampleIds(studyConfiguration.getStudyId(), studyConfiguration.getSampleIds());
        }
    }

//    @Override
    public Map<String, Map<String, String>> convertToDataModelType(DBObject object, String studyIdStr) {
//        Integer studyId = Integer.parseInt(object.get(STUDYID_FIELD).toString());
        Integer studyId = Integer.parseInt(studyIdStr);
        if (sourceDbAdaptor != null) { // Samples not set as constructor argument, need to query
            QueryResult<StudyConfiguration> queryResult = studyConfigurationManager.getStudyConfiguration(studyId, new QueryOptions());
            if(queryResult.first() == null) {
                logger.warn("DBObjectToSamplesConverter.convertToDataModelType StudyConfiguration {studyId: {}} not found! Looking for VariantSource", studyId);

                QueryResult samplesBySource = sourceDbAdaptor.getSamplesBySource(
                        object.get(FILEID_FIELD).toString(), null);
                if(samplesBySource.getResult().isEmpty()) {
                    logger.warn("DBObjectToSamplesConverter.convertToDataModelType VariantSource not found! Can't read sample names");
                } else {
                    setSamples(studyId, (List<String>) samplesBySource.getResult().get(0));
                }
            } else {
                setSampleIds(queryResult.first().getStudyId(), queryResult.first().getSampleIds());
            }
//            QueryResult samplesBySource = sourceDbAdaptor.getSamplesBySource(
//                    object.get(FILEID_FIELD).toString(), null);
//            if(samplesBySource.getResult().isEmpty()) {
//                System.out.println("DBObjectToSamplesConverter.convertToDataModelType " + samplesBySource);
//                sampleIds = null;
//                idSamples = null;
//            } else {
//                setSamples((List<String>) samplesBySource.getResult().get(0));
//            }
        }

        Map<String, Integer> sampleIds = studySampleIds.get(studyId);
        if (sampleIds == null || sampleIds.isEmpty()) {
            return Collections.emptyMap();
        }

        BasicDBObject mongoGenotypes = (BasicDBObject) object.get(GENOTYPES_FIELD);
//        int numSamples = idSamples.size();

        // Temporary file, just to store the samples
//        VariantSourceEntry fileWithSamples = new VariantSourceEntry(fileId.toString(), studyId.toString());
        Map<String, Map<String, String>> samplesData = new LinkedHashMap<>();//new VariantSourceEntry(fileId.toString(), studyId.toString());

        // Add the samples to the file
        for (Map.Entry<String, Integer> entry : sampleIds.entrySet()) {
            samplesData.put(entry.getKey(), new HashMap<>(1)); //size == 1, only will contain GT field
        }

        // An array of genotypes is initialized with the most common one
//        String mostCommonGtString = mongoGenotypes.getString("def");
        if (defaultGenotype != null) {
            String mostCommonGtString = defaultGenotype.toString();
            if (mostCommonGtString != null) {
                for (Map.Entry<String, Map<String, String>> entry : samplesData.entrySet()) {
                    entry.getValue().put("GT", mostCommonGtString);
                }
            }
        }

        // Loop through the non-most commmon genotypes, and set their defaultValue
        // in the position specified in the array, such as:
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ]
        // genotypes[41], genotypes[311], etc, will be set to "0|1"
        Map idSamples = getIdSamplesMap(studyId);
        for (Map.Entry<String, Object> dbo : mongoGenotypes.entrySet()) {
            if (!dbo.getKey().equals(UNKNOWN_GENOTYPE)) {
                String genotype = genotypeToDataModelType(dbo.getKey());
                for (Integer sampleId : (List<Integer>) dbo.getValue()) {
                    if (idSamples.containsKey(sampleId)) {
                        samplesData.get(idSamples.get(sampleId)).put("GT", genotype);
                    }
                }
            }
        }

        return samplesData;
    }

//    @Override
    public DBObject convertToStorageType(Map<String, Map<String, String>> object, String studyIdStr) {
        Map<Genotype, List<Integer>> genotypeCodes = new HashMap<>();

//        Integer studyId = Integer.parseInt(object.getStudyId());
        Integer studyId = Integer.parseInt(studyIdStr);
        Map<String, Integer> sampleIds = studySampleIds.get(studyId);
        // Classify samples by genotype
        for (Map.Entry<String, Map<String, String>> sample : object.entrySet()) {
            String genotype = sample.getValue().get("GT");
            if (genotype != null) {
                Genotype g = new Genotype(genotype);
                List<Integer> samplesWithGenotype = genotypeCodes.get(g);
                if (samplesWithGenotype == null) {
                    samplesWithGenotype = new ArrayList<>();
                    genotypeCodes.put(g, samplesWithGenotype);
                }
                samplesWithGenotype.add(sampleIds.get(sample.getKey()));
            }
        }

        // Get the most common genotype
        Map.Entry<Genotype, List<Integer>> longestList = null;
        if (compressDefaultGenotype) {
            for (Map.Entry<Genotype, List<Integer>> entry : genotypeCodes.entrySet()) {
                if (defaultGenotype != null) {
                    if (defaultGenotype.equals(entry.getKey())) {
                        longestList = entry;
                    }
                } else {
                    List<Integer> genotypeList = entry.getValue();
                    if (longestList == null || genotypeList.size() > longestList.getValue().size()) {
                        longestList = entry;
                    }
                }
            }
        }

        // In Mongo, samples are stored in a map, classified by their genotype.
        // The most common genotype will be marked as "default" and the specific
        // positions where it is shown will not be stored. Example from 1000G:
        // "def" : 0|0,
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ],
        // "1|0" : [ 262, 290, 300, 331, 343, 369, 374, 391, 879, 918, 930 ]
        BasicDBObject mongoSamples = new BasicDBObject();
        for (Map.Entry<Genotype, List<Integer>> entry : genotypeCodes.entrySet()) {
            String genotypeStr = genotypeToStorageType(entry.getKey().toString());
            if (longestList != null && entry.getKey().equals(longestList.getKey())) {
//                mongoSamples.append("def", genotypeStr);
            } else {
                mongoSamples.append(genotypeStr, entry.getValue());
            }
        }

        return mongoSamples;
    }


    public void setSamples(int studyId, List<String> samples) {
        int i = 0;
        int size = samples == null? 0 : samples.size();
        Map<String, Integer> sampleIds = new HashMap<>(size);
        if (samples != null) {
            for (String sample : samples) {
                sampleIds.put(sample, i);
                i++;
            }
        }
        setSampleIds(studyId, sampleIds);
    }

    public void setSampleIds(int studyId, Map<String, Integer> sampleIds) {
        if (studySampleIds == null) {
            studySampleIds = new HashMap<>();
            __studyIdSamples = new HashMap<>();
        }
        this.studySampleIds.put(studyId, sampleIds);
        this.__studyIdSamples.put(studyId, null);
    }

    /**
     * Lazy usage of idSamplesMap. Only inverts map if required
     *
     * @return  Inverts map "sampleIds". From Map<SampleName, SampleId> to Map<SampleId, SampleName>
     */
    private Map getIdSamplesMap(int studyId) {
        Map<String, Integer> sampleIds = studySampleIds.get(studyId);
        if (this.__studyIdSamples.get(studyId) == null) {
            HashMap idSamples = new HashMap<>(sampleIds.size());
            for (Map.Entry<String, Integer> entry : sampleIds.entrySet()) {
                idSamples.put(entry.getValue(), entry.getKey());
            }
            this.__studyIdSamples.put(studyId, idSamples);
        }

        Map<Integer, String> idSamples = this.__studyIdSamples.get(studyId);
        if(sampleIds.size() != idSamples.size()) {
            throw new IllegalStateException("Invalid sample ids map. SampleIDs must be unique.");
        }
        return idSamples;
    }

    private VariantSourceEntry getLegacyNoncompressedSamples(BasicDBObject object) {
        VariantSourceEntry variantSourceEntry = new VariantSourceEntry(object.get(FILEID_FIELD).toString(),
                object.get(STUDYID_FIELD).toString());

        BasicDBObject mongoGenotypes = (BasicDBObject) object.get(GENOTYPES_FIELD);
        for (Object entry : mongoGenotypes.toMap().entrySet()) {
            Map.Entry sample = (Map.Entry) entry;
            variantSourceEntry.addSampleData(sample.getKey().toString(), ((DBObject) sample.getValue()).toMap());
        }
//            for (String sample : samples) {
//                Map<String, String> sampleData = ((DBObject) mongoGenotypes.get(sample)).toMap();
//                fileWithSamples.addSampleData(sample, sampleData);
//                System.out.println("Sample processed: " + sample);
//            }
        return variantSourceEntry;
    }

    private DBObject getLegacyNoncompressedSamples(VariantSourceEntry object) {
        BasicDBObject mongoSamples = new BasicDBObject();
        for (Map.Entry<String, Map<String, String>> entry : object.getSamplesData().entrySet()) {
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

    public Genotype getDefaultGenotype() {
        return defaultGenotype;
    }

    public void setDefaultGenotype(Genotype defaultGenotype) {
        this.defaultGenotype = defaultGenotype;
    }

    public boolean isCompressDefaultGenotype() {
        return compressDefaultGenotype;
    }

    public void setCompressDefaultGenotype(boolean compressDefaultGenotype) {
        this.compressDefaultGenotype = compressDefaultGenotype;
    }
}
