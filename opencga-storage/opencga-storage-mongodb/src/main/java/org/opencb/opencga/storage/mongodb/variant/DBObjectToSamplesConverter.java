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

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;

import static org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceEntryConverter.FILEID_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceEntryConverter.SAMPLES_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceEntryConverter.STUDYID_FIELD;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToSamplesConverter implements ComplexTypeConverter<VariantSourceEntry, DBObject> {

//    private boolean compressSamples;
//    private List<String> samples;
    private Map<String, Integer> sampleIds;
    private Map<Integer, String> idSamples;
    private VariantSourceDBAdaptor sourceDbAdaptor;
    private boolean compressDefaultGenotype;

    /**
     * Create a converter from a Map of samples to DBObject entities.
     * 
     * @param compressDefaultGenotype Whether to compress samples default genotype or not
     */
    public DBObjectToSamplesConverter(boolean compressDefaultGenotype) {
        this.idSamples = null;
        this.sampleIds = null;
        this.sourceDbAdaptor = null;
        this.compressDefaultGenotype = compressDefaultGenotype;
    }

    /**
     * Create a converter from DBObject to a Map of samples, providing the list 
     * of sample names.
     * 
     * @param samples The list of samples, if any
     */
    public DBObjectToSamplesConverter(List<String> samples) {
        this(true);
        setSamples(samples);
    }

    /**
     * Create a converter from DBObject to a Map of samples, providing a map
     * of sample names to the corresponding sample id.
     *
     * @param sampleIds Map of samples to sampleId
     */
    public DBObjectToSamplesConverter(boolean compressDefaultGenotype, Map<String, Integer> sampleIds) {
        this(compressDefaultGenotype);
        setSampleIds(sampleIds);
    }

    /**
     * Create a converter from DBObject to a Map of samples, providing the
     * connection details to the database where the samples are stored.
     *
     * @param credentials Parameters for connecting to the database
     * @param collectionName Collection that stores the variant sources
     */
    public DBObjectToSamplesConverter(MongoCredentials credentials, String collectionName) {
        this(true);
        try {
            this.sourceDbAdaptor = new VariantSourceMongoDBAdaptor(credentials, collectionName);
        } catch (UnknownHostException ex) {
            Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Create a converter from DBObject to a Map of samples, providing the
     * VariantSourceDBAdaptor where the samples are stored.
     *
     * @param variantSourceDBAdaptor  VariantSourceDBAdaptor where the samples are stored.
     */
    public DBObjectToSamplesConverter(VariantSourceDBAdaptor variantSourceDBAdaptor) {
        this(true);
        this.sourceDbAdaptor = variantSourceDBAdaptor;
    }


    @Override
    public VariantSourceEntry convertToDataModelType(DBObject object) {
        if (sourceDbAdaptor != null) { // Samples not set as constructor argument, need to query
            QueryResult samplesBySource = sourceDbAdaptor.getSamplesBySource(
                    object.get(FILEID_FIELD).toString(), null);
            if(samplesBySource.getResult().isEmpty()) {
                System.out.println("DBObjectToSamplesConverter.convertToDataModelType " + samplesBySource);
                sampleIds = null;
                idSamples = null;
            } else {
                setSamples((List<String>) samplesBySource.getResult().get(0));
            }
        }
        
        if (sampleIds == null || sampleIds.isEmpty()) {
            return new VariantSourceEntry(object.get(FILEID_FIELD).toString(), object.get(STUDYID_FIELD).toString());
        }
        
        BasicDBObject mongoGenotypes = (BasicDBObject) object.get(SAMPLES_FIELD);
//        int numSamples = idSamples.size();
        
        // Temporary file, just to store the samples
        VariantSourceEntry fileWithSamples = new VariantSourceEntry(object.get(FILEID_FIELD).toString(), 
                object.get(STUDYID_FIELD).toString());

        // Add the samples to the file
        for (Map.Entry<String, Integer> entry : sampleIds.entrySet()) {
            Map<String, String> sampleData = new HashMap<>(1);  //size == 1, only will contain GT field
            fileWithSamples.addSampleData(entry.getKey(), sampleData);
        }

        // An array of genotypes is initialized with the most common one
        String mostCommonGtString = mongoGenotypes.getString("def");
        if(mostCommonGtString != null) {
            for (Map.Entry<String, Map<String, String>> entry : fileWithSamples.getSamplesData().entrySet()) {
                entry.getValue().put("GT", mostCommonGtString);
            }
        }

        // Loop through the non-most commmon genotypes, and set their value
        // in the position specified in the array, such as:
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ]
        // genotypes[41], genotypes[311], etc, will be set to "0|1"
        for (Map.Entry<String, Object> dbo : mongoGenotypes.entrySet()) {
            if (!dbo.getKey().equals("def")) {
                String genotype = dbo.getKey().replace("-1", ".");
                for (Integer sampleId : (List<Integer>) dbo.getValue()) {
                    if (idSamples.containsKey(sampleId)) {
                        fileWithSamples.getSamplesData().get(idSamples.get(sampleId)).put("GT", genotype);
                    }
                }
            }
        }


        
        return fileWithSamples;
    }

    @Override
    public DBObject convertToStorageType(VariantSourceEntry object) {
        Map<Genotype, List<Integer>> genotypeCodes = new HashMap<>();

        // Classify samples by genotype
        for (Map.Entry<String, Map<String, String>> sample : object.getSamplesData().entrySet()) {
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
                List<Integer> genotypeList = entry.getValue();
                if (longestList == null || genotypeList.size() > longestList.getValue().size()) {
                    longestList = entry;
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
            String genotypeStr = entry.getKey().toString().replace(".", "-1");
            if (longestList != null && entry.getKey().equals(longestList.getKey())) {
                mongoSamples.append("def", genotypeStr);
            } else {
                mongoSamples.append(genotypeStr, entry.getValue());
            }
        }

        return mongoSamples;
    }


    public void setSamples(List<String> samples) {
        int i = 0;
        int size = samples == null? 0 : samples.size();
        sampleIds = new HashMap<>(size);
        idSamples = new HashMap<>(size);
        if (samples != null) {
            for (String sample : samples) {
                sampleIds.put(sample, i);
                idSamples.put(i, sample);
                i++;
            }
        }
    }

    public void setSampleIds(Map<String, Integer> sampleIds) {
        this.sampleIds = sampleIds;
        this.idSamples = new HashMap<>(sampleIds.size());
        for (Map.Entry<String, Integer> entry : sampleIds.entrySet()) {
            idSamples.put(entry.getValue(), entry.getKey());
        }
        assert sampleIds.size() == idSamples.size();
    }

    private VariantSourceEntry getLegacyNoncompressedSamples(BasicDBObject object) {
        VariantSourceEntry variantSourceEntry = new VariantSourceEntry(object.get(FILEID_FIELD).toString(),
                object.get(STUDYID_FIELD).toString());

        BasicDBObject mongoGenotypes = (BasicDBObject) object.get(SAMPLES_FIELD);
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
}
