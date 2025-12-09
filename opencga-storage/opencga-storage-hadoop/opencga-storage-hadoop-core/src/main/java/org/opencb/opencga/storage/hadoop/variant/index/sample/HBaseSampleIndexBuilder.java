package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexBuilder;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.OVERWRITE;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.SAMPLE_INDEX_BUILD_MAX_SAMPLES_PER_MR;

/**
 * Created by jacobo on 04/01/19.
 */
public class HBaseSampleIndexBuilder extends SampleIndexBuilder {

    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final MRExecutor mrExecutor;
    private final HBaseSampleIndexDBAdaptor sampleIndexDBAdaptor;
    private Logger logger = LoggerFactory.getLogger(HBaseSampleIndexBuilder.class);

    public HBaseSampleIndexBuilder(HBaseSampleIndexDBAdaptor sampleIndexDBAdaptor, String study, MRExecutor mrExecutor) {
        super(sampleIndexDBAdaptor, study);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.mrExecutor = mrExecutor;
        this.tableNameGenerator = sampleIndexDBAdaptor.getTableNameGenerator();
    }

    public void buildSampleIndex(List<String> samples, ObjectMap options)
            throws StorageEngineException {
        buildSampleIndex(samples, options, options.getBoolean(OVERWRITE, false));
    }

    protected void buildSampleIndexBatch(int studyId, List<Integer> sampleIds, ObjectMap options)
            throws StorageEngineException {

        sampleIndexDBAdaptor.createTableIfNeeded(studyId, schema.getVersion(), options);
        sampleIndexDBAdaptor.expandTableIfNeeded(studyId, schema.getVersion(), sampleIds, options);

        int batchSize = options.getInt(
                SAMPLE_INDEX_BUILD_MAX_SAMPLES_PER_MR.key(),
                SAMPLE_INDEX_BUILD_MAX_SAMPLES_PER_MR.defaultValue());
//        if (finalSamplesList.size() < 10) {
//            buildSampleIndexBatchMultiThread(studyId, finalSamplesList);
//        }
        List<List<Integer>> batches = BatchUtils.splitBatches(sampleIds, batchSize, true);
        if (batches.size() > 1) {
            batchSize = batches.get(0).size();
            logger.warn("Unable to run sample index build in one single MapReduce operation.");
            logger.info("Split in {} jobs of {} samples each.", batches, batchSize);
            for (int i = 0; i < batches.size(); i++) {
                List<Integer> subSet = batches.get(i);
                logger.info("Running MapReduce {}/{} over {} samples", i + 1, batches, subSet.size());
                buildSampleIndexBatchMapreduce(studyId, subSet, options);
            }
        } else {
            buildSampleIndexBatchMapreduce(studyId, sampleIds, options);
        }
    }

    private void buildSampleIndexBatchMapreduce(int studyId, List<Integer> samples, ObjectMap options)
            throws StorageEngineException {
        options = new ObjectMap(options);
        options.put(SampleIndexDriver.SAMPLE_IDS, samples);
        options.put(SampleIndexDriver.OUTPUT, sampleIndexDBAdaptor.getSampleIndexTableName(studyId, schema.getVersion()));
        options.put(SampleIndexDriver.SAMPLE_INDEX_VERSION, schema.getVersion());

        mrExecutor.run(SampleIndexDriver.class,
                SampleIndexDriver.buildArgs(
                        tableNameGenerator.getArchiveTableName(studyId),
                        tableNameGenerator.getVariantTableName(),
                        studyId,
                        null,
                        options),
                "Build sample index for " + (samples.size() < 10 ? "samples " + samples : samples.size() + " samples"));
        postSampleIndexBuild(studyId, samples);
    }

}
