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

    private boolean compressSamples;
    private List<String> samples;
    private VariantSourceDBAdaptor sourceDbAdaptor;
    private boolean defValue;

    /**
     * Create a converter from a Map of samples to DBObject entities.
     * 
     * @param compressSamples Whether to compress samples or not
     */
    public DBObjectToSamplesConverter(boolean compressSamples, boolean defValue) {
        this.compressSamples = compressSamples;
        this.samples = null;
        this.sourceDbAdaptor = null;
        this.defValue = defValue;
    }

    /**
     * Create a converter from DBObject to a Map of samples, providing the list 
     * of sample names.
     * 
     * @param samples The list of samples, if any
     */
    public DBObjectToSamplesConverter(List<String> samples) {
        this.samples = samples;
        this.sourceDbAdaptor = null;
    }
    
    /**
     * Create a converter from DBObject to a Map of samples, providing the 
     * connection details to the database where the samples are stored.
     * 
     * @param credentials Parameters for connecting to the database
     * @param collectionName Collection that stores the variant sources
     */
    public DBObjectToSamplesConverter(MongoCredentials credentials, String collectionName) {
        try {
            this.samples = null;
            this.sourceDbAdaptor = new VariantSourceMongoDBAdaptor(credentials, collectionName);
        } catch (UnknownHostException ex) {
            Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    @Override
    public VariantSourceEntry convertToDataModelType(DBObject object) {
        if (sourceDbAdaptor != null) { // Samples not set as constructor argument, need to query
            QueryResult samplesBySource = sourceDbAdaptor.getSamplesBySource(
                    object.get(FILEID_FIELD).toString(), null);
            if(samplesBySource.getResult().isEmpty()) {
                System.out.println("DBObjectToSamplesConverter.convertToDataModelType " + samplesBySource);
                samples = null;
            } else {
                samples = (List<String>) samplesBySource.getResult().get(0);
            }
        }
        
        if (samples == null) {
            return new VariantSourceEntry(object.get(FILEID_FIELD).toString(), object.get(STUDYID_FIELD).toString());
        }
        
        BasicDBObject mongoGenotypes = (BasicDBObject) object.get(SAMPLES_FIELD);
        int numSamples = samples.size();
        
        // Temporary file, just to store the samples
        VariantSourceEntry fileWithSamples = new VariantSourceEntry(object.get(FILEID_FIELD).toString(), 
                object.get(STUDYID_FIELD).toString());

        if (mongoGenotypes.containsField("def")) { // Compressed genotypes mode
            // An array of genotypes is initialized with the most common one
            Genotype[] genotypes = new Genotype[numSamples];
            String mostCommonGtString = mongoGenotypes.getString("def");
            if(mostCommonGtString != null) {
                Genotype mostCommongGt = new Genotype(mostCommonGtString);
                for (int i = 0; i < numSamples; i++) {
                    genotypes[i] = mostCommongGt;
                }
            }

            // Loop through the non-most commmon genotypes, and set their value
            // in the position specified in the array, such as:
            // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ]
            // genotypes[41], genotypes[311], etc, will be set to "0|1"
            for (Map.Entry<String, Object> dbo : mongoGenotypes.entrySet()) {
                if (!dbo.getKey().equals("def")) {
                    Genotype gt = new Genotype(dbo.getKey().replace("-1", "."));
                    for (int position : (List<Integer>) dbo.getValue()) {
                        genotypes[position] = gt;
                    }
                }
            }

            // Add the samples to the file, combining the data structures
            // containing the samples' names and the genotypes
            int i = 0;
            for (String sample : samples) {
                Map<String, String> sampleData = new HashMap<>();
                sampleData.put("GT", genotypes[i].toString());
                fileWithSamples.addSampleData(sample, sampleData);
                i++;
            }

        } else { // Non-compressed genotypes mode
            for (Object entry : mongoGenotypes.toMap().entrySet()) {
                Map.Entry sample = (Map.Entry) entry;
                fileWithSamples.addSampleData(sample.getKey().toString(), ((DBObject) sample.getValue()).toMap());
            }
            
//            for (String sample : samples) {
//                Map<String, String> sampleData = ((DBObject) mongoGenotypes.get(sample)).toMap();
//                fileWithSamples.addSampleData(sample, sampleData);
//                System.out.println("Sample processed: " + sample);
//            }
        }
        
        return fileWithSamples;
    }

    @Override
    public DBObject convertToStorageType(VariantSourceEntry object) {
        if (compressSamples) {
            return getCompressedSamples(object);
        } else {
            return getDecompressedSamples(object);
        }
    }
    
    private DBObject getCompressedSamples(VariantSourceEntry object) {
        Map<Genotype, List<Integer>> genotypeCodes = new HashMap<>();
        int i = 0;

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
                samplesWithGenotype.add(i);
            }
            i++;
        }

        // Get the most common genotype
        Map.Entry<Genotype, List<Integer>> longestList = null;
        if (defValue) {
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
        BasicDBObject mongoSamples = new BasicDBObject("def", null);
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
    
    private DBObject getDecompressedSamples(VariantSourceEntry object) {
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
