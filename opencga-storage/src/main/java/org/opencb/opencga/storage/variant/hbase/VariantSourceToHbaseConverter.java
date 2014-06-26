package org.opencb.opencga.storage.variant.hbase;

import java.util.Calendar;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
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
    
    public final static byte[] NUMSAPLES_COLUMN = Bytes.toBytes("numsamples");
    public final static byte[] NUMVARIANTS_COLUMN = Bytes.toBytes("numvariants");
    public final static byte[] NUMSNPS_COLUMN = Bytes.toBytes("numsnps");
    public final static byte[] NUMINDELS_COLUMN = Bytes.toBytes("numindels");
    public final static byte[] NUMPASS_COLUMN = Bytes.toBytes("numpass");
    public final static byte[] NUMTRANSITIONS_COLUMN = Bytes.toBytes("numtransitions");
    public final static byte[] NUMTRANSVERSIONS_COLUMN = Bytes.toBytes("numtransversions");
    public final static byte[] MEANQUALITY_COLUMN = Bytes.toBytes("meanquality");
    
    public final static byte[] METADATA_COLUMN = Bytes.toBytes("metadata");
    
    /**
     * Not-going-to-be-used row key, just necessary to satisfy HBase API.
     */
    private static byte[] rowkey = Bytes.toBytes("ArchivedVariantFileToHbaseConverter");
    
    
    @Override
    public VariantSource convertToDataModelType(Put object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Put convertToStorageType(VariantSource object) {
        // TODO Implementation pending
        Put put = new Put(rowkey);
        put.add(COLUMN_FAMILY, FILENAME_COLUMN, Bytes.toBytes(object.getFileName()));
        put.add(COLUMN_FAMILY, FILEID_COLUMN, Bytes.toBytes(object.getFileId()));
        put.add(COLUMN_FAMILY, STUDYNAME_COLUMN, Bytes.toBytes(object.getStudyName()));
        put.add(COLUMN_FAMILY, STUDYID_COLUMN, Bytes.toBytes(object.getStudyId()));
        put.add(COLUMN_FAMILY, DATE_COLUMN, Bytes.toBytes(Calendar.getInstance().getTimeInMillis()));
        
        // TODO Pending how to manage the consequence type ranking (calculate during reading?)
//        BasicDBObject cts = new BasicDBObject();
//        for (Map.Entry<String, Integer> entry : conseqTypes.entrySet()) {
//            cts.append(entry.getKey(), entry.getValue());
//        }
        
        // Statistics
        VariantGlobalStats global = object.getStats();
        if (global != null) {
            put.add(COLUMN_FAMILY, NUMSAPLES_COLUMN, Bytes.toBytes(global.getSamplesCount()));
            put.add(COLUMN_FAMILY, NUMVARIANTS_COLUMN, Bytes.toBytes(global.getVariantsCount()));
            put.add(COLUMN_FAMILY, NUMSNPS_COLUMN, Bytes.toBytes(global.getSnpsCount()));
            put.add(COLUMN_FAMILY, NUMINDELS_COLUMN, Bytes.toBytes(global.getIndelsCount()));
            put.add(COLUMN_FAMILY, NUMPASS_COLUMN, Bytes.toBytes(global.getPassCount()));
            put.add(COLUMN_FAMILY, NUMTRANSITIONS_COLUMN, Bytes.toBytes(global.getTransitionsCount()));
            put.add(COLUMN_FAMILY, NUMTRANSVERSIONS_COLUMN, Bytes.toBytes(global.getTransversionsCount()));
            put.add(COLUMN_FAMILY, MEANQUALITY_COLUMN, Bytes.toBytes(global.getMeanQuality()));
        }

        // TODO Save pedigree information

        // TODO Metadata as Protocol Buffers? It contains a variable number of fields, and it is not going to be queried a lot
//        Map<String, String> meta = object.getMetadata();
//        DBObject metadataMongo = new BasicDBObject("header", meta.get("variantFileHeader"));
        
        return put;
    }
    
}
