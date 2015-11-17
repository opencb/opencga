/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.NotSupportedException;

import org.apache.commons.lang3.StringUtils;
import org.apache.directory.api.util.exception.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.hpg.bigdata.tools.utils.HBaseUtils;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HBaseStudyConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableDriver extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(VariantTableDriver.class);

    public static final String OPENCGA_VARIANT_TRANSFORM_FILE_ARR = "opencga.variant.transform.file_arr";
    public static final String OPENCGA_VARIANT_TRANSFORM_STUDY = "opencga.variant.transform.study";
    public static final String OPENCGA_VARIANT_TRANSFORM_OUTPUT = "opencga.variant.transform.output";
    public static final String OPENCGA_VARIANT_TRANSFORM_INPUT = "opencga.variant.transform.input";
    public static final String HBASE_MASTER = "hbase.master";

    public VariantTableDriver () { /* nothing */}

    public VariantTableDriver(Configuration conf){
        super(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inTablePrefix = conf.get(OPENCGA_VARIANT_TRANSFORM_INPUT, StringUtils.EMPTY);
        String out_table = conf.get(OPENCGA_VARIANT_TRANSFORM_OUTPUT, StringUtils.EMPTY);
        String[] file_arr = conf.getStrings(OPENCGA_VARIANT_TRANSFORM_FILE_ARR, new String[0]);
        Integer studyId = conf.getInt(OPENCGA_VARIANT_TRANSFORM_STUDY, -1);

        /* -------------------------------*/
        // Validate parameters CHECK
        if (StringUtils.isEmpty(inTablePrefix)) {
            throw new IllegalArgumentException("No input hbase table basename specified!!!");
        }
        if (StringUtils.isEmpty(out_table)) {
            throw new IllegalArgumentException("No output hbase table specified!!!");
        }
        if (inTablePrefix.equals(out_table)) {
            throw new IllegalArgumentException("Input and Output tables must be different");
        }
        if (studyId < 0)  {
            throw new IllegalArgumentException("No Study id specified!!!");
        }
        int fileCnt = file_arr.length;
        if (fileCnt == 0) {
            throw new IllegalArgumentException("No files specified");
        }

        String inTable = GenomeHelper.getArchiveTableName(inTablePrefix, studyId);
        LOG.info(String.format("Use table %s as input", inTable));

        GenomeHelper.setStudyId(conf,studyId);
        VariantTableHelper.setOutputTableName(conf, out_table);
        VariantTableHelper.setInputTableName(conf, inTable);
        
        VariantTableHelper gh = new VariantTableHelper(conf);


        /* -------------------------------*/
        // Validate input CHECK
        HBaseManager.HBaseTableAdminFunction<Boolean> func = ((Table table, Admin admin) -> HBaseUtils.exist(table.getName(),admin));
        if(!gh.getHBaseManager().act(inTable, func)) {
            throw new IllegalArgumentException(String.format("Input table %s does not exist!!!",inTable));
        }

        /* -------------------------------*/
        // INIT META Data
        StudyConfiguration sconf = loadStudyConfiguration(conf, gh, studyId); // needed for query (column names)
        // TODO check if this needs further work

        /* -------------------------------*/
        // JOB setup
        Job job = Job.getInstance(conf, "Genome Variant Transform from & to HBase");
        job.setJarByClass(VariantTableMapper.class);    // class that contains mapper
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");

        // QUERY design
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        // specify return columns (file IDs)
        for(String fname : file_arr){
            Integer id = sconf.getFileIds().get(fname);
            scan.addColumn(gh.getColumnFamily(), Bytes.toBytes(id));
        }

        // set other scan attrs
        TableMapReduceUtil.initTableMapperJob(
            inTable,      // input table
            scan,             // Scan instance to control CF and attribute selection
            VariantTableMapper.class,   // mapper class
            null,             // mapper output key
            null,             // mapper output value
            job);
        TableMapReduceUtil.initTableReducerJob(
            out_table,      // output table
            null,             // reducer class
            job);
        job.setNumReduceTasks(0);
        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            LOG.error("error with job!");
        }
        return b?0:1;
    }

    private StudyConfiguration loadStudyConfiguration(Configuration conf, VariantTableHelper gh, int studyId) throws IOException {
        HBaseStudyConfigurationManager scm = new HBaseStudyConfigurationManager(null, conf, null);
        QueryResult<StudyConfiguration> res = scm.getStudyConfiguration(studyId, new QueryOptions());
        if(res.getResult().size() != 1)
            throw new NotSupportedException();
        return res.first();
    }

    public static void addHBaseSettings(Configuration conf, String hostPortString) throws URISyntaxException{
        String[] hostPort = hostPortString.split(":");
        String server = hostPort[0];
        String port = hostPort.length > 0 ? hostPort[1] : "60000";
        String master = String.join(":", server, port);
        conf.set(HConstants.ZOOKEEPER_QUORUM, server);
        conf.set(HBASE_MASTER, master);
    }


    /**
     * @param args
     * @throws IOException 
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        Configuration conf = new Configuration();
        VariantTableDriver driver = new VariantTableDriver();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        
        //get the args w/o generic hadoop args
        String[] toolArgs = parser.getRemainingArgs();
        
        if (toolArgs.length != 4) {
            System.err.printf("Usage: %s [generic options] <server> <input-table> <output-table> <column>\n", 
                    VariantTableDriver.class.getSimpleName());
            System.err.println("Found " + Arrays.toString(toolArgs));
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        String[] cols = toolArgs[3].split(",");

        addHBaseSettings(conf, toolArgs[0]);
        conf.set(OPENCGA_VARIANT_TRANSFORM_INPUT, toolArgs[1]);
        conf.set(OPENCGA_VARIANT_TRANSFORM_OUTPUT, toolArgs[2]);
        conf.setInt(OPENCGA_VARIANT_TRANSFORM_STUDY, 1);
        conf.setStrings(OPENCGA_VARIANT_TRANSFORM_FILE_ARR, cols);

        //set the configuration back, so that Tool can configure itself
        driver.setConf(conf);
        
        /* Alternative to using tool runner */
//      int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);
        int exitCode = driver.run(toolArgs);

        System.exit(exitCode);
    }

}
