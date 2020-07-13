package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantRowMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.STUDY_ID;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryFromConfig;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryOptionsFromConfig;

public abstract class VariantTableAggregationDriver extends AbstractVariantsTableDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String OUTPUT = "output";

    protected Path outdir;
    protected Path localOutput;


    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("--" + VariantStorageOptions.STUDY.key(), "<study>*");
        params.put("--" + OUTPUT, "<output>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        int studyId = getStudyId();
        if (studyId < 0) {
            throw new IllegalArgumentException("Missing study");
        }

        String outdirStr = getParam(OUTPUT);
        if (StringUtils.isNotEmpty(outdirStr)) {
            outdir = new Path(outdirStr);

            if (isLocal(outdir)) {
                localOutput = getLocalOutput(outdir, this::generateOutputFileName);
                outdir = getTempOutdir("opencga_sample_variant_stats", localOutput.getName());
                outdir.getFileSystem(getConf()).deleteOnExit(outdir);
            }
        }
    }


    protected Query getQuery() {
        return getQueryFromConfig(getConf());
    }

    protected QueryOptions getQueryOptions() {
        return getQueryOptionsFromConfig(getConf());
    }

    @Override
    protected abstract Class<? extends VariantRowMapper> getMapperClass();

    protected abstract Class<?> getMapOutputKeyClass();

    protected abstract Class<?> getMapOutputValueClass();

    protected abstract Class<? extends Reducer> getCombinerClass();

    protected abstract Class<? extends Reducer> getReducerClass();

    protected abstract Class<?> getOutputKeyClass();

    protected abstract Class<?> getOutputValueClass();

    protected abstract int getNumReduceTasks();

    public abstract boolean isOutputWithHeaders();

    protected String generateOutputFileName() {
        return null;
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();

        boolean skipSampleIndex = true;
        Query query = getQuery();
        QueryOptions queryOptions = getQueryOptions();
        VariantMapReduceUtil.initVariantRowMapperJob(job, getMapperClass(),
                variantTable, metadataManager, query, queryOptions, skipSampleIndex);

        job.getConfiguration().setInt(STUDY_ID, getStudyId());

        job.setReducerClass(getReducerClass());
        if (getCombinerClass() != null) {
            job.setCombinerClass(getCombinerClass());
        }

        job.setMapOutputKeyClass(getMapOutputKeyClass());
        job.setMapOutputValueClass(getMapOutputValueClass());

        job.setOutputKeyClass(getOutputKeyClass());
        job.setOutputValueClass(getOutputValueClass());

        if (outdir == null) {
            job.setOutputFormatClass(NullOutputFormat.class);
        } else {
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, outdir);
            if (localOutput == null) {
                LOGGER.info("Output directory : " + outdir);
            } else {
                LOGGER.info("Temporary output directory : " + outdir);
                LOGGER.info("Local output file : " + localOutput);
            }
        }

        int numReduceTasks = getNumReduceTasks();
        LOGGER.info("Using " + numReduceTasks + " reduce tasks");
        job.setNumReduceTasks(numReduceTasks);
        VariantMapReduceUtil.setNoneTimestamp(job);

        return job;
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed) {
            if (localOutput != null) {
                concatMrOutputToLocal(outdir, localOutput, isOutputWithHeaders());
            }
        }
        if (localOutput != null) {
            deleteTemporaryFile(outdir);
        }
    }

//    @SuppressWarnings("unchecked")
//    public static void main(String[] args) {
//        main(args, (Class<? extends AbstractVariantsTableDriver>) MethodHandles.lookup().lookupClass());
//    }

}
