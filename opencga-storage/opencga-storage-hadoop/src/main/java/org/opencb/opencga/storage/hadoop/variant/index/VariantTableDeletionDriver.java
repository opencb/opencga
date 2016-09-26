package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableDeletionDriver extends AbstractVariantTableDriver {

    public static final String JOB_OPERATION_NAME = "Delete";

    public VariantTableDeletionDriver() { /* nothing */}

    public VariantTableDeletionDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return VariantTableDeletionMapReduce.class;
    }

    @Override
    protected String getJobOperationName() {
        return JOB_OPERATION_NAME;
    }

    public static void main(String[] args) throws Exception {
        System.exit(privateMain(args, null));
    }

    public static int privateMain(String[] args, Configuration conf) throws Exception {
        if (conf == null) {
            conf = new Configuration();
        }
        VariantTableDeletionDriver driver = new VariantTableDeletionDriver();
        driver.setConf(conf);
        String[] toolArgs = configure(args, driver);
        if (null == toolArgs) {
            return -1;
        }

        /* Alternative to using tool runner */
//      int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);
        int exitCode = driver.run(toolArgs);
        return exitCode;
    }
}
