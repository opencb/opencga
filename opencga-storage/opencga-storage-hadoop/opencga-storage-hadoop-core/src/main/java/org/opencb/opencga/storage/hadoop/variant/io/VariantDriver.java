package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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

    public static final String OUTPUT_PARAM = "output";
    public static final String CONCAT_OUTPUT_PARAM = "concat-output";
    private Path outdir;
    private Path localOutput;
    private Query query = new Query();
    private QueryOptions options = new QueryOptions();
    private static Logger logger = LoggerFactory.getLogger(VariantDriver.class);
    protected boolean useReduceStep;

    @Override
    protected void parseAndValidateParameters() throws IOException {
        setStudyId(-1);
        super.parseAndValidateParameters();
        String outdirStr = getParam(OUTPUT_PARAM);
        if (StringUtils.isEmpty(outdirStr)) {
            throw new IllegalArgumentException("Missing argument " + OUTPUT_PARAM);
        }

        useReduceStep = Boolean.valueOf(getParam(CONCAT_OUTPUT_PARAM));
        outdir = new Path(outdirStr);
        if (isLocal(outdir)) {
            localOutput = getLocalOutput(outdir);
            outdir = getTempOutdir("opencga_export", localOutput.getName());
            outdir.getFileSystem(getConf()).deleteOnExit(outdir);
        }
        if (localOutput != null) {
            useReduceStep = true;
            logger.info(" * Outdir file: " + localOutput.toUri());
            logger.info(" * Temporary outdir file: " + outdir.toUri());
        } else {
            logger.info(" * Outdir file: " + outdir.toUri());
        }

        getQueryFromConfig(query, getConf());
        getQueryOptionsFromConfig(options, getConf());

        logger.info(" * Query:");
        for (Map.Entry<String, Object> entry : query.entrySet()) {
            logger.info("   * " + entry.getKey() + " : " + entry.getValue());
        }
    }

    @Override
    protected abstract Class<? extends VariantMapper> getMapperClass();

    protected abstract Class<? extends Reducer> getReducerClass();

    protected abstract Class<? extends FileOutputFormat> getOutputFormatClass();

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
        Class<? extends FileOutputFormat> outputFormatClass = getOutputFormatClass();
        if (outputFormatClass == null) {
            throw new IllegalArgumentException("Output format class not provided!");
        }
        job.setOutputFormatClass(outputFormatClass);

        if (useReduceStep) {
            logger.info("Use one Reduce task to produce a single file");
            job.setReducerClass(reducerClass);
            job.setNumReduceTasks(1);
        } else {
            VariantMapReduceUtil.setNoneReduce(job);
        }

        VariantQueryParser variantQueryParser = new HadoopVariantQueryParser(null, getMetadataManager());
        ParsedVariantQuery variantQuery = variantQueryParser.parseQuery(query, options);
        Query query = variantQuery.getQuery();
        if (VariantHBaseQueryParser.isSupportedQuery(query)) {
            logger.info("Init MapReduce job reading from HBase");
            boolean useSampleIndex = !getConf().getBoolean("skipSampleIndex", false) && SampleIndexQueryParser.validSampleIndexQuery(query);
            if (useSampleIndex) {
                // Remove extra fields from the query
                new SampleIndexDBAdaptor(getHBaseManager(), getTableNameGenerator(), getMetadataManager()).parseSampleIndexQuery(query);

                logger.info("Use sample index to read from HBase");
            }

            VariantHBaseQueryParser parser = new VariantHBaseQueryParser(getMetadataManager());
            List<Scan> scans = parser.parseQueryMultiRegion(variantQuery, options);
            VariantMapReduceUtil.configureMapReduceScans(scans, getConf());

            VariantMapReduceUtil.initVariantMapperJobFromHBase(job, variantTable, scans, mapperClass, useSampleIndex);
        } else {
            logger.info("Init MapReduce job reading from Phoenix");
            String sql = new VariantSqlQueryParser(variantTable, getMetadataManager(), getHelper().getConf())
                    .parse(variantQuery, options);

            VariantMapReduceUtil.initVariantMapperJobFromPhoenix(job, variantTable, sql, mapperClass);
        }

        setNoneTimestamp(job);

        FileOutputFormat.setOutputPath(job, outdir); // set Path

        VariantMapReduceUtil.configureVariantConverter(job.getConfiguration(), false, true, true,
                query.getString(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."));


        return job;
    }


    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed) {
            if (localOutput != null) {
                concatMrOutputToLocal(outdir, localOutput);
            }
        }
        if (localOutput != null) {
            deleteTemporaryFile(outdir);
        }
    }

}
