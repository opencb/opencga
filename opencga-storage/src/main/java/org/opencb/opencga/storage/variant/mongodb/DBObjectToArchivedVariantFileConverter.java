package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.storage.variant.VariantSourceDBAdaptor;

/**
 * 
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToArchivedVariantFileConverter implements ComplexTypeConverter<ArchivedVariantFile, DBObject> {

    public final static String FILEID_FIELD = "fid";
    public final static String STUDYID_FIELD = "sid";
    public final static String ATTRIBUTES_FIELD = "attrs";
    public final static String FORMAT_FIELD = "fm";
    public final static String SAMPLES_FIELD = "samp";
    public final static String STATS_FIELD = "st";
    
    
    private boolean includeSamples;
    
    private List<String> samples;
    
    private DBObjectToVariantStatsConverter statsConverter;
    private VariantSourceDBAdaptor sourceDbAdaptor;

    /**
     * Create a converter between ArchivedVariantFile and DBObject entities when 
     * there is no need to provide a list of samples nor statistics.
     */
    public DBObjectToArchivedVariantFileConverter() {
        this.includeSamples = false;
        this.samples = null;
        this.statsConverter = null;
    }
    
    /**
     * Create a converter from ArchivedVariantFile to DBObject entities. A 
     * list of samples and a statistics converter may be provided in case those 
     * should be processed during the conversion.
     * 
     * @param samples The list of samples, if any
     * @param statsConverter The object used to convert the file statistics
     */
    public DBObjectToArchivedVariantFileConverter(List<String> samples, DBObjectToVariantStatsConverter statsConverter) {
        this.samples = samples;
        this.statsConverter = statsConverter;
    }
    
    /**
     * Create a converter from DBObject to ArchivedVariantFile entities. A 
     * list of samples and a statistics converter may be provided in case those 
     * should be processed during the conversion.
     * 
     * If samples are to be included, their names must have been previously 
     * stored in the database and the connection parameters must be provided.
     * 
     * @param includeSamples Whether to include samples or not
     * @param statsConverter The object used to convert the file statistics
     * @param credentials Parameters for connecting to the database
     */
    public DBObjectToArchivedVariantFileConverter(boolean includeSamples, DBObjectToVariantStatsConverter statsConverter, MongoCredentials credentials) {
        this.includeSamples = includeSamples;
        if (this.includeSamples) {
            try {
                this.sourceDbAdaptor = new VariantSourceMongoDBAdaptor(credentials);
            } catch (UnknownHostException ex) {
                Logger.getLogger(DBObjectToArchivedVariantFileConverter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        this.statsConverter = statsConverter;
    }
    
    
    @Override
    public ArchivedVariantFile convertToDataModelType(DBObject object) {
        String fileId = (String) object.get(FILEID_FIELD);
        String studyId = (String) object.get(STUDYID_FIELD);
        ArchivedVariantFile file = new ArchivedVariantFile(fileId, studyId);
        
        // Attributes
        if (object.containsField(ATTRIBUTES_FIELD)) {
            file.setAttributes(((DBObject) object.get(ATTRIBUTES_FIELD)).toMap());
        }
        if (object.containsField(FORMAT_FIELD)) {
            file.setFormat((String) object.get(FORMAT_FIELD));
        }
        
        // Samples
        if (includeSamples && object.containsField(SAMPLES_FIELD)) {
            BasicDBList genotypes = (BasicDBList) object.get(SAMPLES_FIELD);
            samples = (List<String>) sourceDbAdaptor.getSamplesBySource(fileId, studyId, null).getResult().get(0);
            Iterator<String> samplesIterator = samples.iterator();
            Iterator<Object> genotypesIterator = genotypes.iterator();
            
            while (samplesIterator.hasNext() && genotypesIterator.hasNext()) {
                String sampleName = samplesIterator.next();
                Genotype gt = Genotype.decode((int) genotypesIterator.next());
                Map<String, String> sampleData = new HashMap<>();
                sampleData.put("GT", gt.toString());
                file.addSampleData(sampleName, sampleData);
            }
        }
        
        // Statistics
        if (statsConverter != null && object.containsField(STATS_FIELD)) {
            file.setStats(statsConverter.convertToDataModelType((DBObject) object.get(STATS_FIELD)));
        }
        return file;
    }

    @Override
    public DBObject convertToStorageType(ArchivedVariantFile object) {
        BasicDBObject mongoFile = new BasicDBObject(FILEID_FIELD, object.getFileId()).append(STUDYID_FIELD, object.getStudyId());

        // Attributes
        if (object.getAttributes().size() > 0) {
            BasicDBObject attrs = null;
            for (Map.Entry<String, String> entry : object.getAttributes().entrySet()) {
                if (attrs == null) {
                    attrs = new BasicDBObject(entry.getKey(), entry.getValue());
                } else {
                    attrs.append(entry.getKey(), entry.getValue());
                }
            }

            if (attrs != null) {
                mongoFile.put(ATTRIBUTES_FIELD, attrs);
            }
        }

        // Samples are stored in a map, classified by their genotype
        // The most common genotype will be marked as "default" and the specific
        // positions where it is shown will not be stored. Example from 1000G:
        // "def" : 100,       
        // "101" : [ 41, 311, 342, 358, 881, 898, 903 ],
        // "110" : [ 262, 290, 300, 331, 343, 369, 374, 391, 879, 918, 930 ]
        if (samples != null && !samples.isEmpty()) {
            mongoFile.append(FORMAT_FIELD, object.getFormat()); // Useless field if genotypeCodes are not stored

            Map<Integer, List<Integer>> genotypeCodes = new HashMap<>();
            int i = 0;
            
            // Classify samples by genotype
            for (String sampleName : samples) {
                String genotype = object.getSampleData(sampleName, "GT");
                if (genotype != null) {
                    Genotype g = new Genotype(genotype);
                    int encoding = g.encode();
                    List<Integer> samplesWithGenotype = genotypeCodes.get(encoding);
                    if (samplesWithGenotype == null) {
                        samplesWithGenotype = new ArrayList<>();
                        genotypeCodes.put(encoding, samplesWithGenotype);
                    }
                    samplesWithGenotype.add(i);
                }
                i++;
            }
            
            // Get the most common genotype
            Map.Entry<Integer, List<Integer>> longestList = null;
            for (Map.Entry<Integer, List<Integer>> entry : genotypeCodes.entrySet()) {
                List<Integer> genotypeList = entry.getValue();
                if (longestList == null || genotypeList.size() > longestList.getValue().size()) {
                    longestList = entry;
                }
            }
            
            // Create the map to store in Mongo
            BasicDBObject mongoGenotypeCodes = new BasicDBObject();
            for (Map.Entry<Integer, List<Integer>> entry : genotypeCodes.entrySet()) {
                if (entry.getKey().equals(longestList.getKey())) {
                    mongoGenotypeCodes.append("def", entry.getKey());
                } else {
                    mongoGenotypeCodes.append(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
            
            mongoFile.put(SAMPLES_FIELD, mongoGenotypeCodes);
        }
        
        // Statistics
        if (statsConverter != null && object.getStats() != null) {
            mongoFile.put(STATS_FIELD, statsConverter.convertToStorageType(object.getStats()));
        }
        
        return mongoFile;
    }
    
}
