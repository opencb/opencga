package org.opencb.opencga.storage.variant.hbase;

import java.util.Calendar;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantSourceToHbaseConverter implements ComplexTypeConverter<VariantSource, Put> {

    public final static byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    public final static byte[] FILENAME_COLUMN = Bytes.toBytes("filename");
    public final static byte[] FILEID_COLUMN = Bytes.toBytes("fileid");
    public final static byte[] STUDYNAME_COLUMN = Bytes.toBytes("studyname");
    public final static byte[] STUDYID_COLUMN = Bytes.toBytes("studyid");
    public final static byte[] DATE_COLUMN = Bytes.toBytes("date");
    
    @Override
    public VariantSource convertToDataModelType(Put object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Put convertToStorageType(VariantSource object) {
        // TODO Implementation pending
        Put put = new Put();
        // Column names are not optimized to static variables because not many sources will be transformed at once
        put.add(COLUMN_FAMILY, FILENAME_COLUMN, Bytes.toBytes(object.getFileName()));
        put.add(COLUMN_FAMILY, FILEID_COLUMN, Bytes.toBytes(object.getFileId()));
        put.add(COLUMN_FAMILY, STUDYNAME_COLUMN, Bytes.toBytes(object.getStudyName()));
        put.add(COLUMN_FAMILY, STUDYID_COLUMN, Bytes.toBytes(object.getStudyId()));
        put.add(COLUMN_FAMILY, DATE_COLUMN, Bytes.toBytes(Calendar.getInstance().getTimeInMillis()));
        
        
        /*
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
        */
        return put;
    }
    
}
