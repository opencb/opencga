package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.HBaseStudyConfigurationManager;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableDeletionDriver extends AbstractVariantTableDriver {

    public VariantTableDeletionDriver() { /* nothing */}

    public VariantTableDeletionDriver(Configuration conf) {
        super(conf);
    }

    @SuppressWarnings ("rawtypes")
    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return VariantTableDeletionMapReduce.class;
    }

    @Override
    public int run(String[] args) throws Exception {
        // Execute MR job
        int exitValue = super.run(args);
        if (exitValue != 0) {
            return exitValue;
        }
        // If everything went fine, remove file column from Archive table and from studyconfig
        HBaseStudyConfigurationManager scm = getStudyConfigurationManager();
        StudyConfiguration sc = loadStudyConfiguration();
        String[] fileArr = argFileArray();
        for (String file : fileArr) {
            getLog().info(String.format("Remove `{0}` from Study Configuration", file));
            Integer fId = Integer.parseInt(file);
            sc.getIndexedFiles().remove(fId);
        }
        getLog().info("Store updated StudyConfiguration ... ");
        scm.updateStudyConfiguration(sc, new QueryOptions());
        return exitValue;
    }

    @Override
    protected String getJobOperationName() {
        return "Delete";
    }

    public static void main(String[] args) throws Exception {
        System.exit(privateMain(args, null));
    }

    public static int privateMain(String[] args, Configuration conf) throws Exception {
        if (conf == null) {
            conf = new Configuration();
        }
        VariantTableDeletionDriver driver = new VariantTableDeletionDriver();
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
