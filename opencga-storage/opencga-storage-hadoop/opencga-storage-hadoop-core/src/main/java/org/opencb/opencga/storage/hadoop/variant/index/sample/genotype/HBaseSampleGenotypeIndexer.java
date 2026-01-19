package org.opencb.opencga.storage.hadoop.variant.index.sample.genotype;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleGenotypeIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.HBaseSampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.SAMPLE_INDEX_BUILD_MAX_SAMPLES_PER_MR;

/**
 * Created by jacobo on 04/01/19.
 */
public class HBaseSampleGenotypeIndexer extends SampleGenotypeIndexer {

    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final MRExecutor mrExecutor;
    private final HBaseSampleIndexDBAdaptor sampleIndexDBAdaptor;
    private Logger logger = LoggerFactory.getLogger(HBaseSampleGenotypeIndexer.class);

    public HBaseSampleGenotypeIndexer(HBaseSampleIndexDBAdaptor sampleIndexDBAdaptor, MRExecutor mrExecutor) {
        super(sampleIndexDBAdaptor);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.mrExecutor = mrExecutor;
        this.tableNameGenerator = sampleIndexDBAdaptor.getTableNameGenerator();
    }

    @Override
    protected void run(int studyId, SampleIndexSchema schema, List<Integer> sampleIds, ObjectMap options)
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
                runBatch(studyId, schema, subSet, options);
            }
        } else {
            runBatch(studyId, schema, sampleIds, options);
        }
    }

    @Override
    protected void indexBatch(int studyId, SampleIndexSchema schema, List<Integer> samples, ObjectMap options)
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
    }

}
