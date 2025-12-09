package org.opencb.opencga.storage.hadoop.variant.index.sample.annotation;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleIndexAnnotationConstructor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.HBaseSampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.SAMPLE_INDEX_ANNOTATION_MAX_SAMPLES_PER_MR;

/**
 * Created by jacobo on 04/01/19.
 */
public class HBaseSampleIndexAnnotationConstructor extends SampleIndexAnnotationConstructor {

    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final MRExecutor mrExecutor;
    private final HBaseSampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final Logger logger = LoggerFactory.getLogger(HBaseSampleIndexAnnotationConstructor.class);

    protected HBaseSampleIndexAnnotationConstructor(HBaseSampleIndexDBAdaptor sampleIndexDBAdaptor, MRExecutor mrExecutor) {
        super(sampleIndexDBAdaptor);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.mrExecutor = mrExecutor;
        this.tableNameGenerator = sampleIndexDBAdaptor.getTableNameGenerator();
    }

    @Override
    protected void run(int studyId, List<Integer> samples, int sampleIndexVersion, ObjectMap options)
            throws StorageEngineException {
        sampleIndexDBAdaptor.createTableIfNeeded(studyId, sampleIndexVersion, options);

        int batchSize = options.getInt(
                SAMPLE_INDEX_ANNOTATION_MAX_SAMPLES_PER_MR.key(),
                SAMPLE_INDEX_ANNOTATION_MAX_SAMPLES_PER_MR.defaultValue());
//        if (finalSamplesList.size() < 10) {
//            updateSampleAnnotationBatchMultiThread(studyId, samples);
//        }
        if (samples.size() > batchSize) {
            List<List<Integer>> batches = BatchUtils.splitBatches(samples, batchSize, true);
            logger.warn("Unable to run sample index annotation in one single MapReduce operation.");
            logger.info("Split in {} jobs of {} samples each.", batches, batchSize);
            for (int i = 0; i < batches.size(); i++) {
                List<Integer> batch = batches.get(i);
                logger.info("Running MapReduce {}/{} over {} samples", i + 1, batches.size(), batch.size());
                runBatch(studyId, batch, sampleIndexVersion, options);
                postRunBatch(studyId, batch, sampleIndexVersion);
            }
        } else {
            runBatch(studyId, samples, sampleIndexVersion, options);
            postRunBatch(studyId, samples, sampleIndexVersion);
        }
    }

    @Override
    protected void runBatch(int studyId, List<Integer> samples, int sampleIndexVersion, ObjectMap options)
            throws StorageEngineException {
        options.put(SampleIndexAnnotationLoaderDriver.OUTPUT,
                sampleIndexDBAdaptor.getSampleIndexTableNameLatest(studyId));
        options.put(SampleIndexAnnotationLoaderDriver.SAMPLE_INDEX_VERSION, sampleIndexVersion);
        mrExecutor.run(SampleIndexAnnotationLoaderDriver.class, SampleIndexAnnotationLoaderDriver.buildArgs(
                tableNameGenerator.getArchiveTableName(studyId),
                tableNameGenerator.getVariantTableName(), studyId, samples, options),
                "Annotate sample index for "
                        + (samples.size() < 10 ? "samples " + samples : samples.size() + " samples"));

    }

}
