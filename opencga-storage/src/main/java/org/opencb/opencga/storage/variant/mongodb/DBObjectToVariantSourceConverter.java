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

    @Override
    public VariantSource convertToDataModelType(DBObject object) {
        VariantSource source = new VariantSource((String) object.get("fileName"), (String) object.get("fileId"),
                (String) object.get("studyName"), (String) object.get("studyId"));
        
        // Samples
        BasicDBObject samplesPosition = (BasicDBObject) object.get("samples");
        source.setSamplesPosition(samplesPosition.toMap());
        
        // Statistics
        DBObject statsObject = (DBObject) object.get("globalStats");
        VariantGlobalStats stats = new VariantGlobalStats();
        stats.setSamplesCount((int) statsObject.get("samplesCount"));
        stats.setVariantsCount((int) statsObject.get("variantsCount"));
        stats.setSnpsCount((int) statsObject.get("snpCount"));
        stats.setIndelsCount((int) statsObject.get("indelCount"));
        stats.setPassCount((int) statsObject.get("passCount"));
        stats.setTransitionsCount((int) statsObject.get("transitionsCount"));
        stats.setTransversionsCount((int) statsObject.get("transversionsCount"));
        stats.setMeanQuality(((Double) statsObject.get("meanQuality")).floatValue());
        source.setStats(stats);
        
        // Metadata
        BasicDBObject metadata = (BasicDBObject) object.get("metadata");
        for (Map.Entry<String, Object> o : metadata.entrySet()) {
            source.addMetadata(o.getKey(), o.getValue().toString());
        }
        
        return source;
    }

    @Override
    public DBObject convertToStorageType(VariantSource object) {
        BasicDBObject studyMongo = new BasicDBObject("fileName", object.getFileName())
                .append("fileId", object.getFileId())
                .append("studyName", object.getStudyName())
                .append("studyId", object.getStudyId())
                .append("date", Calendar.getInstance().getTime())
                .append("samples", object.getSamplesPosition());

        // TODO Pending how to manage the consequence type ranking (calculate during reading?)
//        BasicDBObject cts = new BasicDBObject();
//        for (Map.Entry<String, Integer> entry : conseqTypes.entrySet()) {
//            cts.append(entry.getKey(), entry.getValue());
//        }

        // Statistics
        VariantGlobalStats global = object.getStats();
        if (global != null) {
            DBObject globalStats = new BasicDBObject("samplesCount", global.getSamplesCount())
                    .append("variantsCount", global.getVariantsCount())
                    .append("snpCount", global.getSnpsCount())
                    .append("indelCount", global.getIndelsCount())
                    .append("passCount", global.getPassCount())
                    .append("transitionsCount", global.getTransitionsCount())
                    .append("transversionsCount", global.getTransversionsCount())
//                    .append("accumulatedQuality", stats.getAccumulatedQuality())
                    .append("meanQuality", (float) global.getMeanQuality());
//                    globalStats.append("consequenceTypes", cts);

            studyMongo = studyMongo.append("globalStats", globalStats);
//        } else {
//            studyMongo.append("globalStats", new BasicDBObject("consequenceTypes", cts));
        }

        // TODO Save pedigree information
        
        // Metadata
        Map<String, String> meta = object.getMetadata();
        DBObject metadataMongo = new BasicDBObject("header", meta.get("variantFileHeader"));
        studyMongo = studyMongo.append("metadata", metadataMongo);
        
        return studyMongo;
    }
    
}
