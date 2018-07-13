package org.opencb.opencga.storage.hadoop.variant.mr;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMapReduceUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(VariantMapReduceUtil.class);


    public static void initTableMapperJob(Job job, String inTable, String outTable, Scan scan, Class<? extends TableMapper> mapperClass)
            throws IOException {
        initTableMapperJob(job, inTable, scan, mapperClass);
        setOutputHBaseTable(job, outTable);
        setNoneReduce(job);
    }

    public static void initTableMapperJob(Job job, String inTable, String outTable, List<Scan> scans,
                                          Class<? extends TableMapper> mapperClass)
            throws IOException {
        initTableMapperJob(job, inTable, scans, mapperClass);
        setOutputHBaseTable(job, outTable);
        setNoneReduce(job);
    }

    public static void initTableMapperMultiOutputJob(Job job, String inTable, List<Scan> scans, Class<? extends TableMapper> mapperClass)
            throws IOException {
        initTableMapperJob(job, inTable, scans, mapperClass);
        setMultiTableOutput(job);
        setNoneReduce(job);
    }

    public static void initTableMapperJob(Job job, String inTable, Scan scan, Class<? extends TableMapper> mapperClass)
            throws IOException {
        boolean addDependencyJar = job.getConfiguration().getBoolean(HadoopVariantStorageEngine.MAPREDUCE_ADD_DEPENDENCY_JARS, true);
        LOGGER.info("Use table {} as input", inTable);
        TableMapReduceUtil.initTableMapperJob(
                inTable,      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapperClass,   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job,
                addDependencyJar);
    }

    public static void initTableMapperJob(Job job, String inTable, List<Scan> scans, Class<? extends TableMapper> mapperClass)
            throws IOException {
        if (scans.isEmpty()) {
            throw new IllegalArgumentException("There must be at least one scan! Error creating TableMapperJob '" + job.getJobName() + "'");
        } else if (scans.size() == 1) {
            initTableMapperJob(job, inTable, scans.get(0), mapperClass);
        } else {
            boolean addDependencyJar = job.getConfiguration().getBoolean(HadoopVariantStorageEngine.MAPREDUCE_ADD_DEPENDENCY_JARS, true);
            LOGGER.info("Use table {} as input", inTable);
            for (Scan scan : scans) {
                scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(inTable));
            }
            TableMapReduceUtil.initTableMapperJob(
                    scans,            // Scan instance to control CF and attribute selection
                    mapperClass,      // mapper class
                    null,             // mapper output key
                    null,             // mapper output value
                    job,
                    addDependencyJar);
        }
    }

    public static void initVariantMapperJobFromHBase(Job job, String variantTableName, Scan scan,
                                                     Class<? extends VariantMapper> variantMapperClass)
            throws IOException {
        initTableMapperJob(job, variantTableName, scan, TableMapper.class);

        job.setMapperClass(variantMapperClass);

//        job.getConfiguration().set(TableInputFormat.INPUT_TABLE, variantTableName);
        job.setInputFormatClass(HBaseVariantTableInputFormat.class);
    }

    public static void initVariantMapperJobFromPhoenix(Job job, VariantHadoopDBAdaptor dbAdaptor,
                                                       Class<? extends VariantMapper> variantMapperClass)
            throws IOException {
        initVariantMapperJobFromPhoenix(job, dbAdaptor, new Query(), new QueryOptions(), variantMapperClass);
    }

    public static void initVariantMapperJobFromPhoenix(Job job, VariantHadoopDBAdaptor dbAdaptor, Query query, QueryOptions queryOptions,
                                                       Class<? extends VariantMapper> variantMapperClass)
            throws IOException {
        GenomeHelper genomeHelper = dbAdaptor.getGenomeHelper();
        String variantTableName = dbAdaptor.getVariantTable();
        StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();
        VariantSqlQueryParser variantSqlQueryParser = new VariantSqlQueryParser(genomeHelper, variantTableName, scm, null, false);

        String sql = variantSqlQueryParser.parse(query, queryOptions).getSql();

        initVariantMapperJobFromPhoenix(job, variantTableName, sql, variantMapperClass);
    }

    public static void initVariantMapperJobFromPhoenix(Job job, String variantTableName, String selectQuery,
                                                       Class<? extends VariantMapper> variantMapperClass)
            throws IOException {
        // VariantDBWritable is the DBWritable class that enables us to process the Result of the query
        PhoenixMapReduceUtil.setInput(job, PhoenixVariantTableInputFormat.VariantDBWritable.class, variantTableName,  selectQuery);

        job.setMapperClass(variantMapperClass);
        job.setOutputFormatClass(PhoenixOutputFormat.class);

//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(DoubleWritable.class);
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(StockWritable.class);

        job.setInputFormatClass(PhoenixVariantTableInputFormat.class);
    }

    public static void setNoneReduce(Job job) throws IOException {
        job.setNumReduceTasks(0);
    }

    public static void setOutputHBaseTable(Job job, String outTable) throws IOException {
        boolean addDependencyJar = job.getConfiguration().getBoolean(HadoopVariantStorageEngine.MAPREDUCE_ADD_DEPENDENCY_JARS, true);
        LOGGER.info("Use table {} as output", outTable);
        TableMapReduceUtil.initTableReducerJob(
                outTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
    }

    public static void setMultiTableOutput(Job job) throws IOException {
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        boolean addDependencyJars = job.getConfiguration().getBoolean(HadoopVariantStorageEngine.MAPREDUCE_ADD_DEPENDENCY_JARS, true);

        if (addDependencyJars) {
            TableMapReduceUtil.addDependencyJars(job);
        }
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Mutation.class);

        TableMapReduceUtil.initCredentials(job);
    }

    public static void configureVariantConverter(Configuration configuration,
                                                 boolean mutableSamplesPosition,
                                                 boolean studyNameAsStudyId,
                                                 boolean simpleGenotypes,
                                                 String unknownGenotype) {
        configuration.setBoolean(HBaseToVariantConverter.MUTABLE_SAMPLES_POSITION, mutableSamplesPosition);
        configuration.setBoolean(HBaseToVariantConverter.STUDY_NAME_AS_STUDY_ID, studyNameAsStudyId);
        configuration.setBoolean(HBaseToVariantConverter.SIMPLE_GENOTYPES, simpleGenotypes);
        configuration.set(VariantQueryParam.UNKNOWN_GENOTYPE.key(), unknownGenotype);
    }

    public static void configureVCores(Configuration conf) {
        // Set parallel pool size
        String fjpKey = "java.util.concurrent.ForkJoinPool.common.parallelism";
        boolean hasForkJoinPool = false;
        Integer vCores = conf.getInt("mapreduce.map.cpu.vcores", 1);
        Collection<String> opts = conf.getStringCollection("opencga.variant.table.mapreduce.map.java.opts");
        String optString = StringUtils.EMPTY;
        if (!opts.isEmpty()) {
            for (String opt : opts) {
                if (opt.contains(fjpKey)) {
                    hasForkJoinPool = true;
                }
                optString += opt + " ";
            }
        }
        if (!hasForkJoinPool && vCores > 1) {
            optString += " -D" + fjpKey + "=" + vCores;
            LOGGER.warn("Force ForkJoinPool to {}", vCores);
        }
        if (StringUtils.isNotBlank(optString)) {
            LOGGER.info("Set mapreduce java opts: {}", optString);
            conf.set("mapreduce.map.java.opts", optString);
        }
    }
}
