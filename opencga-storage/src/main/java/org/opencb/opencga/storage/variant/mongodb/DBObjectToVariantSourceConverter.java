package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.Calendar;
import java.util.Map;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantSourceConverter implements ComplexTypeConverter<VariantSource, DBObject> {

    public final static String FILEID_FIELD = "fid";
    public final static String FILENAME_FIELD = "fname";
    public final static String STUDYID_FIELD = "sid";
    public final static String STUDYNAME_FIELD = "sname";
    public final static String DATE_FIELD = "date";
    public final static String SAMPLES_FIELD = "samp";
    
    public final static String STATS_FIELD = "st";
    public final static String NUMSAMPLES_FIELD = "nSamp";
    public final static String NUMVARIANTS_FIELD = "nVar";
    public final static String NUMSNPS_FIELD = "nSnp";
    public final static String NUMINDELS_FIELD = "nIndel";
    public final static String NUMPASSFILTERS_FIELD = "nPass";
    public final static String NUMTRANSITIONS_FIELD = "nTi";
    public final static String NUMTRANSVERSIONS_FIELD = "nTv";
    public final static String MEANQUALITY_FIELD = "meanQ";
    
    public final static String METADATA_FIELD = "meta";
    public final static String HEADER_FIELD = "header";
    
    
    @Override
    public VariantSource convertToDataModelType(DBObject object) {
        VariantSource source = new VariantSource((String) object.get(FILENAME_FIELD), (String) object.get(FILEID_FIELD),
                (String) object.get(STUDYID_FIELD), (String) object.get(STUDYNAME_FIELD));
        
        // Samples
        source.setSamplesPosition((Map) object.get(SAMPLES_FIELD));
        
        // Statistics
        DBObject statsObject = (DBObject) object.get(STATS_FIELD);
        VariantGlobalStats stats = new VariantGlobalStats();
        stats.setSamplesCount((int) statsObject.get(NUMSAMPLES_FIELD));
        stats.setVariantsCount((int) statsObject.get(NUMVARIANTS_FIELD));
        stats.setSnpsCount((int) statsObject.get(NUMSNPS_FIELD));
        stats.setIndelsCount((int) statsObject.get(NUMINDELS_FIELD));
        stats.setPassCount((int) statsObject.get(NUMPASSFILTERS_FIELD));
        stats.setTransitionsCount((int) statsObject.get(NUMTRANSITIONS_FIELD));
        stats.setTransversionsCount((int) statsObject.get(NUMTRANSVERSIONS_FIELD));
        stats.setMeanQuality(((Double) statsObject.get(MEANQUALITY_FIELD)).floatValue());
        source.setStats(stats);
        
        // Metadata
        BasicDBObject metadata = (BasicDBObject) object.get(METADATA_FIELD);
        for (Map.Entry<String, Object> o : metadata.entrySet()) {
            source.addMetadata(o.getKey(), o.getValue().toString());
        }
        
        return source;
    }

    @Override
    public DBObject convertToStorageType(VariantSource object) {
        BasicDBObject studyMongo = new BasicDBObject(FILENAME_FIELD, object.getFileName())
                .append(FILEID_FIELD, object.getFileId())
                .append(STUDYNAME_FIELD, object.getStudyName())
                .append(STUDYID_FIELD, object.getStudyId())
                .append(DATE_FIELD, Calendar.getInstance().getTime())
                .append(SAMPLES_FIELD, object.getSamplesPosition());

        // TODO Pending how to manage the consequence type ranking (calculate during reading?)
//        BasicDBObject cts = new BasicDBObject();
//        for (Map.Entry<String, Integer> entry : conseqTypes.entrySet()) {
//            cts.append(entry.getKey(), entry.getValue());
//        }

        // Statistics
        VariantGlobalStats global = object.getStats();
        if (global != null) {
            DBObject globalStats = new BasicDBObject(NUMSAMPLES_FIELD, global.getSamplesCount())
                    .append(NUMVARIANTS_FIELD, global.getVariantsCount())
                    .append(NUMSNPS_FIELD, global.getSnpsCount())
                    .append(NUMINDELS_FIELD, global.getIndelsCount())
                    .append(NUMPASSFILTERS_FIELD, global.getPassCount())
                    .append(NUMTRANSITIONS_FIELD, global.getTransitionsCount())
                    .append(NUMTRANSVERSIONS_FIELD, global.getTransversionsCount())
                    .append(MEANQUALITY_FIELD, (double) global.getMeanQuality());

            studyMongo = studyMongo.append(STATS_FIELD, globalStats);
        }

        // TODO Save pedigree information
        
        // Metadata
        Map<String, String> meta = object.getMetadata();
        DBObject metadataMongo = new BasicDBObject(HEADER_FIELD, meta.get("variantFileHeader"));
        studyMongo = studyMongo.append(METADATA_FIELD, metadataMongo);
        
        return studyMongo;
    }
    
}
