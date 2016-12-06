package org.opencb.opencga.storage.hadoop.variant.exporters;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseStudyConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver.CONFIG_VARIANT_FILE_IDS;
import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver.CONFIG_VARIANT_TABLE_NAME;

/**
 * Created by mh719 on 21/11/2016.
 */
public class VariantTableExportDriver extends Configured implements Tool {
    public static final String CONFIG_VARIANT_TABLE_EXPORT_AVRO_PATH = "opencga.variant.table.export.avro.path";
    protected static final Logger LOG = LoggerFactory.getLogger(VariantTableExportDriver.class);
    private VariantTableHelper variantTablehelper;
    private HBaseStudyConfigurationManager scm;

    public VariantTableExportDriver() { /* nothing */ }

    public VariantTableExportDriver(Configuration conf) {
        super(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        int fixedSizeArgs = 5;
        configFromArgs(args, fixedSizeArgs);

        Configuration conf = getConf();

        String archiveTable = conf.get(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, StringUtils.EMPTY);
        String variantTable = conf.get(CONFIG_VARIANT_TABLE_NAME, StringUtils.EMPTY);
        Integer studyId = conf.getInt(GenomeHelper.CONFIG_STUDY_ID, -1);

        String outFile = conf.get(CONFIG_VARIANT_TABLE_EXPORT_AVRO_PATH, StringUtils.EMPTY);

        /* -------------------------------*/
        // Validate parameters CHECK
        if (StringUtils.isEmpty(outFile)) {
            throw new IllegalArgumentException("No AVRO output file specified!!!");
        }
        if (StringUtils.isEmpty(archiveTable)) {
            throw new IllegalArgumentException("No archive hbase table basename specified!!!");
        }
        if (StringUtils.isEmpty(variantTable)) {
            throw new IllegalArgumentException("No variant hbase table specified!!!");
        }
        if (archiveTable.equals(variantTable)) {
            throw new IllegalArgumentException("archive and variant tables must be different");
        }
        if (studyId < 0) {
            throw new IllegalArgumentException("No Study id specified!!!");
        }


        getLog().info(String.format("Use table %s as input", variantTable));
        GenomeHelper.setStudyId(conf, studyId);
        VariantTableHelper.setOutputTableName(conf, variantTable);
        VariantTableHelper.setInputTableName(conf, archiveTable);

        VariantTableHelper gh = getHelper();

        /* -------------------------------*/
        // Validate input CHECK
        checkTablesExist(gh, archiveTable, variantTable);

        // Check File(s) or Study is specified
        List<Integer> fileIds = getFilesToUse();

        /* -------------------------------*/
        // JOB setup
        setConf(conf);
        Job job = createJob(variantTable, outFile, fileIds);

        // QUERY design
        Scan scan = createScan();

        // set other scan attrs
        boolean addDependencyJar = conf.getBoolean(GenomeHelper.CONFIG_HBASE_ADD_DEPENDENCY_JARS, true);
        initMapReduceJob(variantTable, outFile, job, scan, addDependencyJar);

        boolean succeed = executeJob(job);
        if (!succeed) {
            getLog().error("error with job!");
        }

        getStudyConfigurationManager().close();

        return succeed ? 0 : 1;
    }

    protected boolean executeJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        Thread hook = new Thread(() -> {
            try {
                if (!job.isComplete()) {
                    job.killJob();
                }
//                onError();
            } catch (IOException e) {
                getLog().error("Error", e);
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        boolean succeed = job.waitForCompletion(true);
        Runtime.getRuntime().removeShutdownHook(hook);
        return succeed;
    }

    private Class<? extends TableMapper> getMapperClass() {
        return AnalysisToAvroMapper.class;
    }

    protected void initMapReduceJob(String inTable, String outputPath, Job job, Scan scan, boolean addDependencyJar)
            throws IOException {
        TableMapReduceUtil.initTableMapperJob(
                inTable,      // input table
                scan,             // Scan instance to control CF and attribute selection
                getMapperClass(),   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job,
                addDependencyJar);

        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroKeyOutputFormat.setOutputPath(job, new Path(outputPath)); // set Path
        AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema()); // Set schema
        AvroKeyOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression
        job.setNumReduceTasks(0);
    }

    protected Scan createScan() {
        Scan scan = new Scan();
//        int caching = getConf().getInt(AbstractVariantTableDriver.HBASE_SCAN_CACHING, 50);
//        getLog().info("Scan set Caching to " + caching);
//        scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        return scan;
    }

    protected Job createJob(String variantTable, String outFile, List<Integer> files) throws IOException {
        Job job = Job.getInstance(getConf(), "opencga: Export files " + files
                + " from VariantTable '" + variantTable + " to " + outFile + "'");
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.setJarByClass(getMapperClass());    // class that contains mapper
        return job;
    }

    protected List<Integer> getFilesToUse() throws IOException {
        StudyConfiguration studyConfiguration = loadStudyConfiguration();
        LinkedHashSet<Integer> indexedFiles = studyConfiguration.getIndexedFiles();
        String[] fileArr = getConf().getStrings(CONFIG_VARIANT_FILE_IDS, new String[0]);
        List<Integer> files = Arrays.stream(fileArr).map(s -> Integer.parseInt(s)).collect(Collectors.toList());
        if (files.isEmpty()) { // no files specified - use all indexed files for study
            files = new ArrayList<>(indexedFiles);
        } else { // Validate that they exist
            List<Integer> notIndexed = files.stream().filter(fid -> !indexedFiles.contains(fid))
                    .collect(Collectors.toList());
            if (!notIndexed.isEmpty()) {
                throw new IllegalStateException("Provided File ID(s) not indexed!!!" + notIndexed);
            }
        }
        if (files.isEmpty()) { // if still empty (no files provided and / or found in study
            throw new IllegalArgumentException("No files specified / available for study "
                    + getHelper().getStudyId());
        }
        return files;
    }

    protected StudyConfiguration loadStudyConfiguration() throws IOException {
        HBaseStudyConfigurationManager scm = getStudyConfigurationManager();
        int studyId = getHelper().getStudyId();
        QueryResult<StudyConfiguration> res = scm.getStudyConfiguration(studyId, new QueryOptions());
        if (res.getResult().size() != 1) {
            throw new IllegalStateException("StudyConfiguration " + studyId + " not found! " + res.getResult().size());
        }
        return res.first();
    }

    protected HBaseStudyConfigurationManager getStudyConfigurationManager() throws IOException {
        if (scm == null) {
            byte[] outTable = getHelper().getOutputTable();
            scm = new HBaseStudyConfigurationManager(Bytes.toString(outTable), getConf(), null);
        }
        return scm;
    }

    private void checkTablesExist(GenomeHelper genomeHelper, String... tables) {
        final HBaseManager hBaseManager = genomeHelper.getHBaseManager();
        Arrays.stream(tables).forEach(table -> {
            try {
                if (!hBaseManager.tableExists(table)) {
                    throw new IllegalArgumentException(String.format("Table %s does not exist!!!", table));
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    protected VariantTableHelper getHelper() {
        if (null == variantTablehelper) {
            variantTablehelper = new VariantTableHelper(getConf());
        }
        return variantTablehelper;
    }

    protected void configFromArgs(String[] args, int fixedSizeArgs) {
        getConf().set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, args[1]);
        getConf().set(CONFIG_VARIANT_TABLE_NAME, args[2]);
        getConf().set(GenomeHelper.CONFIG_STUDY_ID, args[3]);
        getConf().setStrings(CONFIG_VARIANT_FILE_IDS, args[4].split(","));
        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            getConf().set(args[i], args[i + 1]);
        }
    }

    public static Logger getLog() {
        return LOG;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new VariantTableExportDriver()));
        } catch (Exception e) {
            LOG.error("Problems", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static int privateMain(String[] args, Configuration conf, VariantTableExportDriver driver) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        driver.setConf(conf);
        int exitCode = ToolRunner.run(driver, args);
        return exitCode;
    }

}
