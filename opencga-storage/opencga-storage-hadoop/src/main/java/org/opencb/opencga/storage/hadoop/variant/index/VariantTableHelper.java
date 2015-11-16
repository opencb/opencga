/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.models.protobuf.VariantCallProtos.VariantCallMetaProt;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableHelper extends GenomeHelper {

    public static final String OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT = "opencga.storage.hadoop.vcf.transform.table.output";
    public static final String OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT = "opencga.storage.hadoop.vcf.transform.table.input";
    private final AtomicReference<byte[]> outtable = new AtomicReference<>();
    private final AtomicReference<byte[]> intable = new AtomicReference<>();

    /**
     * 
     * @param conf
     */
    public VariantTableHelper (Configuration conf) {
        super(conf);
        String outTable = conf.get(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT, StringUtils.EMPTY);
        String intable = conf.get(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT, StringUtils.EMPTY);
        if(StringUtils.isEmpty(outTable)){
            throw new IllegalArgumentException("Property for Output Table name missing or empty!!!");
        }
        if(StringUtils.isEmpty(intable)){
            throw new IllegalArgumentException("Property for Input Table name missing or empty!!!");
        }
        setOutputTable(outTable);
        setInputTable(intable);
    }

    public VariantCallMetaProt loadMeta() throws IOException{
        final Get get = new Get(getMetaRowKey());
        get.addColumn(getColumnFamily(), getMetaColumnKey());
        HBaseManager.HBaseTableFunction<Result> func = (Table table)-> table.get(new Get(getMetaRowKey()));
        Result res = hBaseManager.act(getOutputTable(), func);
        if(res.isEmpty() || !res.containsColumn(getColumnFamily(), getMetaRowKey())) {
            return null;
        }
        byte[] val = res.getValue(getColumnFamily(), getMetaRowKey());
        return VariantCallMetaProt.parseFrom(val);
    }
    // TODO for the future: 
    // Table locking
    // http://grokbase.com/t/hbase/user/1169nsvfcx/does-put-support-dont-put-if-row-exists
    public void storeMeta(VariantCallMetaProt meta) throws IOException{
        final Put put = wrapMetaAsPut(meta);
        hBaseManager.act(getOutputTable(), (Table t) -> t.put(put));
    }

    public Put wrapMetaAsPut(VariantCallMetaProt meta){
        return wrapMetaAsPut(getMetaColumnKey(), meta);
    }
    
    public byte[] getMetaColumnKey(){
        return getMetaRowKey();
    }
    
    public byte[] getOutputTable() {
        return outtable.get();
    }
    
    public byte[] getIntputTable(){
        return intable.get();
    }

    public void act(HBaseManager.HBaseTableConsumer<Table> func) throws IOException {
        hBaseManager.act(getOutputTable(), func);
    }

    public <T> T actOnTable(HBaseManager.HBaseTableAdminFunction<T> func) throws IOException {
        return hBaseManager.act(getOutputTableAsString(), func);
    }

    public String getOutputTableAsString() {
        return Bytes.toString(getOutputTable());
    }

    private void setOutputTable(String table_name){
        this.outtable.set(Bytes.toBytes(table_name));
    }

    private void setInputTable(String table_name){
        this.intable.set(Bytes.toBytes(table_name));
    }

    public static void setOutputTableName(Configuration conf, String out_table) {
        conf.set(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT, out_table);
    }
    public static void setInputTableName(Configuration conf, String in_table) {
        conf.set(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT, in_table);
    }

}
