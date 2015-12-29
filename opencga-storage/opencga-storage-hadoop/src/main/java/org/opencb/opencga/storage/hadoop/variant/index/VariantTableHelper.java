/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HBaseStudyConfigurationManager;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableHelper extends GenomeHelper {

    public static final String OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT = "opencga.storage.hadoop.vcf.transform.table.output";
    public static final String OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT = "opencga.storage.hadoop.vcf.transform.table.input";
    private final AtomicReference<byte[]> outtable = new AtomicReference<>();
    private final AtomicReference<byte[]> intable = new AtomicReference<>();


    public VariantTableHelper(Configuration conf) {
        this(conf, conf.get(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT, StringUtils.EMPTY),
                conf.get(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT, StringUtils.EMPTY));
    }


    public VariantTableHelper(Configuration conf, String intable, String outTable) {
        super(conf);
        if (StringUtils.isEmpty(outTable)) {
            throw new IllegalArgumentException("Property for Output Table name missing or empty!!!");
        }
        if (StringUtils.isEmpty(intable)) {
            throw new IllegalArgumentException("Property for Input Table name missing or empty!!!");
        }
        setOutputTable(outTable);
        setInputTable(intable);
    }

    public StudyConfiguration loadMeta() throws IOException {
        HBaseStudyConfigurationManager scm =
                new HBaseStudyConfigurationManager(Bytes.toString(outtable.get()), this.hBaseManager.getConf(), null);
        QueryResult<StudyConfiguration> query = scm.getStudyConfiguration(getStudyId(), new QueryOptions());
        if (query.getResult().size() != 1) {
            throw new NotSupportedException("Only one study configuration expected for study");
        }
        return query.first();
    }

    // TODO for the future:
    // Table locking
    // http://grokbase.com/t/hbase/user/1169nsvfcx/does-put-support-dont-put-if-row-exists
    public void storeMeta(StudyConfiguration studyConf) throws IOException {
        HBaseStudyConfigurationManager scm =
                new HBaseStudyConfigurationManager(Bytes.toString(outtable.get()), this.hBaseManager.getConf(), null);
        scm.updateStudyConfiguration(studyConf, new QueryOptions());
    }

    public byte[] getOutputTable() {
        return outtable.get();
    }

    public byte[] getIntputTable() {
        return intable.get();
    }

    public void act(Connection con, HBaseManager.HBaseTableConsumer func) throws IOException {
        hBaseManager.act(con, getOutputTable(), func);
    }

    public <T> T actOnTable(Connection con, HBaseManager.HBaseTableAdminFunction<T> func) throws IOException {
        return hBaseManager.act(con, getOutputTableAsString(), func);
    }

    public String getOutputTableAsString() {
        return Bytes.toString(getOutputTable());
    }

    private void setOutputTable(String tableName) {
        this.outtable.set(Bytes.toBytes(tableName));
    }

    private void setInputTable(String tableName) {
        this.intable.set(Bytes.toBytes(tableName));
    }

    public static void setOutputTableName(Configuration conf, String outTable) {
        conf.set(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT, outTable);
    }

    public static void setInputTableName(Configuration conf, String inTable) {
        conf.set(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT, inTable);
    }

}
