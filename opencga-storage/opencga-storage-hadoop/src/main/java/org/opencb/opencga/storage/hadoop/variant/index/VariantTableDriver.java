/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableDriver extends AbstractVariantTableDriver {
    protected static final Logger LOG = LoggerFactory.getLogger(VariantTableDriver.class);

    public static final String JOB_OPERATION_NAME = "Load";

    public VariantTableDriver() { /* nothing */ }

    public VariantTableDriver(Configuration conf) {
        super(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        int fixedSizeArgs = 5;
        getConf().set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, args[1]);
        getConf().set(CONFIG_VARIANT_TABLE_NAME, args[2]);
        getConf().set(GenomeHelper.CONFIG_STUDY_ID, args[3]);
        getConf().setStrings(CONFIG_VARIANT_FILE_IDS, args[4].split(","));
        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            getConf().set(args[i], args[i + 1]);
        }
        return super.run(args);
    }

    @SuppressWarnings ("rawtypes")
    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return VariantTableMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return JOB_OPERATION_NAME;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new VariantTableDriver()));
        } catch (Exception e) {
            LOG.error("Problems", e);
            System.err.println("");
            e.printStackTrace();
        }
    }

    public static int privateMain(String[] args, Configuration conf, VariantTableDriver driver) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        driver.setConf(conf);
        int exitCode = ToolRunner.run(driver, args);
        return exitCode;
    }

}
