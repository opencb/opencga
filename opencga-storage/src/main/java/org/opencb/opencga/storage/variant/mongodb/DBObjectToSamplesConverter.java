package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.storage.variant.VariantSourceDBAdaptor;
import static org.opencb.opencga.storage.variant.mongodb.DBObjectToArchivedVariantFileConverter.FILEID_FIELD;
import static org.opencb.opencga.storage.variant.mongodb.DBObjectToArchivedVariantFileConverter.SAMPLES_FIELD;
import static org.opencb.opencga.storage.variant.mongodb.DBObjectToArchivedVariantFileConverter.STUDYID_FIELD;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToSamplesConverter implements ComplexTypeConverter<ArchivedVariantFile, DBObject> {

    private boolean compressSamples;
    private List<String> samples;
    private VariantSourceDBAdaptor sourceDbAdaptor;

    /**
     * Create a converter from a Map of samples to DBObject entities.
     * 
     * @param compressSamples Whether to compress samples or not
     */
    public DBObjectToSamplesConverter(boolean compressSamples) {
        this.compressSamples = compressSamples;
        this.samples = null;
        this.sourceDbAdaptor = null;
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
     */
    public DBObjectToSamplesConverter(MongoCredentials credentials) {
        try {
            this.samples = null;
            this.sourceDbAdaptor = new VariantSourceMongoDBAdaptor(credentials);
        } catch (UnknownHostException ex) {
            Logger.getLogger(DBObjectToArchivedVariantFileConverter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    @Override
    public ArchivedVariantFile convertToDataModelType(DBObject object) {
        if (samples == null && sourceDbAdaptor != null) { // Samples not set as constructor argument, need to query
            samples = (List<String>) sourceDbAdaptor.getSamplesBySource(object.get(FILEID_FIELD).toString(), 
                    object.get(STUDYID_FIELD).toString(), null).getResult().get(0);
        }
        
        if (samples == null) {
            return new ArchivedVariantFile(object.get(FILEID_FIELD).toString(), object.get(STUDYID_FIELD).toString());
        }
        
        BasicDBObject mongoGenotypes = (BasicDBObject) object.get(SAMPLES_FIELD);
        int numSamples = samples.size();
            
        // An array of genotypes is initialized with the most common one
        Genotype[] genotypes = new Genotype[numSamples];
        String mostCommonGtString = mongoGenotypes.getString("def");
        Genotype mostCommongGt = new Genotype(mostCommonGtString);
        for (int i = 0; i < numSamples; i++) {
            genotypes[i] = mostCommongGt;
        }

        // Loop through the non-most commmon genotypes, and set their value
        // in the position specified in the array, such as:
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ]
        // genotypes[41], genotypes[311], etc, will be set to "0|1"
        for (Map.Entry<String, Object> dbo : mongoGenotypes.entrySet()) {
            if (!dbo.getKey().equals("def")) {
                Genotype gt = new Genotype(dbo.getKey());
                for (int position : (List<Integer>) dbo.getValue()) {
                    genotypes[position] = gt;
                }
            }
        }

        // Add the samples to the file, combining the data structures
        // containing the samples' names and the genotypes
        ArchivedVariantFile fileWithSamples = new ArchivedVariantFile(object.get(FILEID_FIELD).toString(), 
                object.get(STUDYID_FIELD).toString());
        
        int i = 0;
        for (String sample : samples) {
            Map<String, String> sampleData = new HashMap<>();
            sampleData.put("GT", genotypes[i].toString());
            fileWithSamples.addSampleData(sample, sampleData);
            i++;
        }
        
        return fileWithSamples;
    }

    @Override
    public DBObject convertToStorageType(ArchivedVariantFile object) {
        if (compressSamples) {
            return getCompressedSamples(object);
        } else {
            return getDecompressedSamples(object);
        }
    }
    
    private DBObject getCompressedSamples(ArchivedVariantFile object) {
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
        for (Map.Entry<Genotype, List<Integer>> entry : genotypeCodes.entrySet()) {
            List<Integer> genotypeList = entry.getValue();
            if (longestList == null || genotypeList.size() > longestList.getValue().size()) {
                longestList = entry;
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
            if (longestList != null && entry.getKey().equals(longestList.getKey())) {
                mongoSamples.append("def", entry.getKey().toString());
            } else {
                mongoSamples.append(entry.getKey().toString(), entry.getValue());
            }
        }
        
        return mongoSamples;
    }
    
    private DBObject getDecompressedSamples(ArchivedVariantFile object) {
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
