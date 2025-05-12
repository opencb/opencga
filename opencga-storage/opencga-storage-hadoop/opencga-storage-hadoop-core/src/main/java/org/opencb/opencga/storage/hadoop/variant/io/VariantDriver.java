package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.adaptors.VariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.metadata.local.LocalVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.io.HDFSIOConnector;
import org.opencb.opencga.storage.hadoop.utils.MapReduceOutputFile;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryFromConfig;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryOptionsFromConfig;

/**
 * Created on 14/06/18.
 *
 * export HADOOP_USER_CLASSPATH_FIRST=true
 * hbase_conf=$(hbase classpath | tr ":" "\n" | grep "/conf" | tr "\n" ":")
 * export HADOOP_CLASSPATH=${hbase_conf}:$PWD/libs/avro-1.7.7.jar:$PWD/libs/jackson-databind-2.6.6.jar:$PWD/libs/jackson-core-2.6.6.jar
 * export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$PWD/libs/jackson-annotations-2.6.6.jar
 * yarn jar opencga-storage-hadoop-core-1.4.0-jar-with-dependencies.jar \
 *      org.opencb.opencga.storage.hadoop.variant.io.VariantExporterDriver \
 *      opencga_variants study myStudy --of avro --output my.variants.avro --region 22
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantDriver extends AbstractVariantsTableDriver {

    protected MapReduceOutputFile output;
    private final Query query = new Query();
    private final QueryOptions options = new QueryOptions();
    private static Logger logger = LoggerFactory.getLogger(VariantDriver.class);
    protected boolean useReduceStep;
    private LocalVariantStorageMetadataDBAdaptorFactory recordMetadataDBAdaptor;
    private List<URI> recordMetadataFiles;

    @Override
    protected void parseAndValidateParameters() throws IOException {
        setStudyId(-1);
        super.parseAndValidateParameters();

//        useReduceStep = Boolean.valueOf(getParam(CONCAT_OUTPUT_PARAM));
        output = initMapReduceOutputFile();

        getQueryFromConfig(query, getConf());
        getQueryOptionsFromConfig(options, getConf());
        if (!options.getBoolean(QueryOptions.SORT)) {
            // Unsorted results might break the file generation.
            // Results from HBase are always sorted, but when reading from Phoenix, some results might be out of order.
            options.put(QueryOptions.SORT, true);
        }

        logger.info(" * Query:");
        for (String key : query.keySet()) {
            String value = query.getString(key);
            if (value.length() > 100) {
                List<String> valuesList = query.getAsStringList(key);
                if (valuesList.size() > 10) {
                    value = "(" + (valuesList.size()) + " elements) " + StringUtils.join(valuesList.subList(0, 10), ",") + "...";
                }
            }
            logger.info("   - {}: {}", key, value);
        }
    }

    @Override
    protected VariantStorageMetadataDBAdaptorFactory newMetadataDbAdaptorFactory() {
        VariantStorageMetadataDBAdaptorFactory dbAdaptorFactory = super.newMetadataDbAdaptorFactory();
        recordMetadataDBAdaptor = new LocalVariantStorageMetadataDBAdaptorFactory(dbAdaptorFactory);
        return recordMetadataDBAdaptor;
    }

    @Override
    protected abstract Class<? extends VariantMapper> getMapperClass();

    protected abstract Class<? extends Reducer> getReducerClass();

    protected Class<? extends Partitioner> getPartitioner() {
        return null;
    }

    protected abstract Class<? extends OutputFormat> getOutputFormatClass();

    protected abstract void setupJob(Job job) throws IOException;

    @Override
    protected final Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        setupJob(job);
        Class<? extends VariantMapper> mapperClass = getMapperClass();
        Class<? extends Reducer> reducerClass = getReducerClass();
        if (mapperClass == null) {
            throw new IllegalArgumentException("Mapper class not provided!");
        }
        if (useReduceStep) {
            if (reducerClass == null) {
                throw new IllegalArgumentException("Reducer class not provided!");
            }
        }
        Class<? extends OutputFormat> outputFormatClass = getOutputFormatClass();
        if (outputFormatClass == null) {
            throw new IllegalArgumentException("Output format class not provided!");
        }
        job.setOutputFormatClass(outputFormatClass);

        if (useReduceStep) {
            setupReducer(job, variantTable);
        } else {
            VariantMapReduceUtil.setNoneReduce(job);
        }

        VariantMapReduceUtil.initVariantMapperJob(job, mapperClass, variantTable, getMetadataManager(), query, options, false);

        setNoneTimestamp(job);

        FileOutputFormat.setOutputPath(job, output.getOutdir()); // set Path

        VariantMapReduceUtil.configureVariantConverter(job.getConfiguration(), false, true, true,
                query.getString(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."));

        URI recordMetadataOutput;
        if (output.getLocalOutput() != null) {
            recordMetadataOutput = output.getLocalOutput().getParent().toUri().resolve("recordMetadata");
        } else {
            recordMetadataOutput = Paths.get(System.getProperty("java.io.tmpdir")).resolve(TimeUtils.getTime() + "_recordMetadata").toUri();
        }
        FileSystem.get(recordMetadataOutput, job.getConfiguration()).mkdirs(new org.apache.hadoop.fs.Path(recordMetadataOutput));
        recordMetadataFiles = recordMetadataDBAdaptor.writeToFile(recordMetadataOutput, new HDFSIOConnector(job.getConfiguration()));
        VariantMapReduceUtil.configureLocalMetadataManager(job, recordMetadataFiles);

        return job;
    }

    @Override
    protected void postSubmit(Job job) throws IOException {
        super.postSubmit(job);

        FileSystem fs = FileSystem.get(recordMetadataFiles.get(0), job.getConfiguration());
        for (URI recordMetadataFile : recordMetadataFiles) {
            fs.delete(new Path(recordMetadataFile), false);
        }
    }

    protected void setupReducer(Job job, String variantTable) throws IOException {
        Class<? extends Partitioner> partitionerClass = getPartitioner();
        if (partitionerClass == null) {
            logger.info("Use one Reduce task to produce a single file");
            job.setReducerClass(getReducerClass());
            job.setNumReduceTasks(1);
        } else {
            String numReducersKey = getClass().getSimpleName() + "." + JobContext.NUM_REDUCES;
            String numReducersStr = getParam(numReducersKey);
            int reduceTasks;
            if (StringUtils.isNotEmpty(numReducersStr)) {
                reduceTasks = Integer.parseInt(numReducersStr);
                logger.info("Set reduce tasks to " + reduceTasks + " (derived from input parameter '" + numReducersKey + "')");
            } else {
                int serversSize = getHBaseManager().act(variantTable, (table, admin) -> admin.getClusterStatus().getServersSize());
                // Set the number of reduce tasks to 2x times the number of servers
                reduceTasks = serversSize * 2;
                logger.info("Set reduce tasks to " + reduceTasks + " (derived from 'number_of_servers * 2')");
            }
            job.setReducerClass(getReducerClass());
            job.setPartitionerClass(partitionerClass);
            job.setNumReduceTasks(reduceTasks);
        }
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        output.postExecute(succeed);
    }

}
