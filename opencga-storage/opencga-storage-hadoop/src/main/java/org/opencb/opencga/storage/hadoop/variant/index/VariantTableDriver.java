/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableDriver extends AbstractVariantTableDriver {


    public VariantTableDriver() { /* nothing */ }

    public VariantTableDriver(Configuration conf) {
        super(conf);
    }

    @SuppressWarnings ("rawtypes")
    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return VariantTableMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return "Load";
    }

    public static void main(String[] args) throws Exception {
        System.exit(privateMain(args, null, new VariantTableDriver()));
    }

    public static int privateMain(String[] args, Configuration conf, VariantTableDriver driver) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            conf = new Configuration();
        }
        String[] toolArgs = configure(args, conf);
        if (null == toolArgs) {
            return -1;
        }

        //set the configuration back, so that Tool can configure itself
        driver.setConf(conf);

        /* Alternative to using tool runner */
//      int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);
        int exitCode = driver.run(toolArgs);
        return exitCode;
    }

}
