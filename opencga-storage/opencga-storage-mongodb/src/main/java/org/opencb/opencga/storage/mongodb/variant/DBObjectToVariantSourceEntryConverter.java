package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

/**
 * 
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantSourceEntryConverter implements ComplexTypeConverter<VariantSourceEntry, DBObject> {

    public final static String FILEID_FIELD = "fid";
    public final static String STUDYID_FIELD = "sid";
    public final static String ALTERNATES_FIELD = "alts";
    public final static String ATTRIBUTES_FIELD = "attrs";
    public final static String FORMAT_FIELD = "fm";
    public final static String SAMPLES_FIELD = "samp";
    public final static String STATS_FIELD = "st";
    
    private boolean includeSrc;

    private DBObjectToSamplesConverter samplesConverter;
    private DBObjectToVariantStatsConverter statsConverter;

    /**
     * Create a converter between VariantSourceEntry and DBObject entities when 
     * there is no need to provide a list of samples or statistics.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the DBObject
     */
    public DBObjectToVariantSourceEntryConverter(boolean includeSrc) {
        this.includeSrc = includeSrc;
        this.samplesConverter = null;
        this.statsConverter = null;
    }


    /**
     * Create a converter from VariantSourceEntry to DBObject entities. A
     * samples converter and a statistics converter may be provided in case those
     * should be processed during the conversion.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the DBObject
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     * @param statsConverter   The object used to convert the file statistics. If null, won't convert
     *
     */
    public DBObjectToVariantSourceEntryConverter(boolean includeSrc,
                                                 DBObjectToSamplesConverter samplesConverter,
                                                 DBObjectToVariantStatsConverter statsConverter) {
        this(includeSrc);
        this.samplesConverter = samplesConverter;
        this.statsConverter = statsConverter;
    }
    
    @Override
    public VariantSourceEntry convertToDataModelType(DBObject object) {
        String fileId = (String) object.get(FILEID_FIELD);
        String studyId = (String) object.get(STUDYID_FIELD);
        VariantSourceEntry file = new VariantSourceEntry(fileId, studyId);
        
        // Alternate alleles
        if (object.containsField(ALTERNATES_FIELD)) {
            List list = (List) object.get(ALTERNATES_FIELD);
            String[] alternatives = new String[list.size()];
            int i = 0;
            for (Object o : list) {
                alternatives[i] = o.toString();
                i++;
            }
            file.setSecondaryAlternates(alternatives);
        }
        
        // Attributes
        if (object.containsField(ATTRIBUTES_FIELD)) {
            file.setAttributes(((DBObject) object.get(ATTRIBUTES_FIELD)).toMap());
            // Unzip the "src" field, if available
            if (((DBObject) object.get(ATTRIBUTES_FIELD)).containsField("src")) {
                byte[] o = (byte[]) ((DBObject) object.get(ATTRIBUTES_FIELD)).get("src");
                try {
                    file.addAttribute("src", org.opencb.commons.utils.StringUtils.gunzip(o));
                } catch (IOException ex) {
                    Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        if (object.containsField(FORMAT_FIELD)) {
            file.setFormat((String) object.get(FORMAT_FIELD));
        }
        
        // Samples
        if (samplesConverter != null && object.containsField(SAMPLES_FIELD)) {
            VariantSourceEntry fileWithSamplesData = samplesConverter.convertToDataModelType(object);
            
            // Add the samples to the Java object, combining the data structures
            // with the samples' names and the genotypes
            for (Map.Entry<String, Map<String, String>> sampleData : fileWithSamplesData.getSamplesData().entrySet()) {
                file.addSampleData(sampleData.getKey(), sampleData.getValue());
            }
        }
        
        // Statistics
        if (statsConverter != null && object.containsField(STATS_FIELD)) {
            file.setStats(statsConverter.convertToDataModelType((DBObject) object.get(STATS_FIELD)));
        }
        return file;
    }

    @Override
    public DBObject convertToStorageType(VariantSourceEntry object) {
        BasicDBObject mongoFile = new BasicDBObject(FILEID_FIELD, object.getFileId()).append(STUDYID_FIELD, object.getStudyId());

        // Alternate alleles
        if (object.getSecondaryAlternates().length > 1) {
            mongoFile.append(ALTERNATES_FIELD, object.getSecondaryAlternates());
        }
        
        // Attributes
        if (object.getAttributes().size() > 0) {
            BasicDBObject attrs = null;
            for (Map.Entry<String, String> entry : object.getAttributes().entrySet()) {
                Object value = entry.getValue();
                if (entry.getKey().equals("src")) {
                    if (includeSrc) {
                        try {
                            value = org.opencb.commons.utils.StringUtils.gzip(entry.getValue());
                        } catch (IOException ex) {
                            Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        continue;
                    }
                }

                if (attrs == null) {
                    attrs = new BasicDBObject(entry.getKey(), value);
                } else {
                    attrs.append(entry.getKey(), value);
                }
            }

            if (attrs != null) {
                mongoFile.put(ATTRIBUTES_FIELD, attrs);
            }
        }

//        if (samples != null && !samples.isEmpty()) {
        if (samplesConverter != null) {
            mongoFile.append(FORMAT_FIELD, object.getFormat()); // Useless field if genotypeCodes are not stored
            mongoFile.put(SAMPLES_FIELD, samplesConverter.convertToStorageType(object));
        }
        
        // Statistics
        if (statsConverter != null && object.getStats() != null) {
            mongoFile.put(STATS_FIELD, statsConverter.convertToStorageType(object.getStats()));
        }
        
        return mongoFile;
    }


    public void setIncludeSrc(boolean includeSrc) {
        this.includeSrc = includeSrc;
    }
}
