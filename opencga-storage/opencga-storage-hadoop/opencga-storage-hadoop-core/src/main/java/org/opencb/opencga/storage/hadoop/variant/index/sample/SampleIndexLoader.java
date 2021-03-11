package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.OVERWRITE;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.SAMPLE_INDEX_BUILD_MAX_SAMPLES_PER_MR;

/**
 * Created by jacobo on 04/01/19.
 */
public class SampleIndexLoader {

    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final MRExecutor mrExecutor;
    private final VariantStorageMetadataManager metadataManager;
    private final SampleIndexSchema schema;
    private Logger logger = LoggerFactory.getLogger(SampleIndexLoader.class);

    public SampleIndexLoader(HBaseVariantTableNameGenerator tableNameGenerator,
                             VariantStorageMetadataManager metadataManager, MRExecutor mrExecutor, SampleIndexSchema schema) {
        this.tableNameGenerator = tableNameGenerator;
        this.mrExecutor = mrExecutor;
        this.metadataManager = metadataManager;
        this.schema = schema;
    }

    public void buildSampleIndex(String study, List<String> samples, ObjectMap options)
            throws StorageEngineException {
        buildSampleIndex(study, samples, options, options.getBoolean(OVERWRITE, false));
    }

    public void buildSampleIndex(String study, List<String> samples, ObjectMap options, boolean overwrite)
            throws StorageEngineException {
        int studyId = metadataManager.getStudyId(study);
        List<Integer> sampleIds;
        if (samples.size() == 1 && samples.get(0).equals(VariantQueryUtils.ALL)) {
            sampleIds = metadataManager.getIndexedSamples(studyId);
        } else {
            sampleIds = new ArrayList<>(samples.size());
            for (String sample : samples) {
                Integer sampleId = metadataManager.getSampleId(studyId, sample, true);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sample, study);
                }
                sampleIds.add(sampleId);
            }
        }

        List<Integer> finalSamplesList = new ArrayList<>(samples.size());
        List<String> alreadyIndexed = new LinkedList<>();
        for (Integer sampleId : sampleIds) {
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
            if (overwrite || SampleIndexDBAdaptor.getSampleIndexStatus(sampleMetadata) != TaskMetadata.Status.READY) {
                finalSamplesList.add(sampleId);
            } else {
                // SamplesIndex already annotated
                alreadyIndexed.add(sampleMetadata.getName());
            }
        }
        if (!alreadyIndexed.isEmpty()) {
            logger.info("Skip sample index for " + alreadyIndexed.size() + " samples."
                    + " Add " + OVERWRITE + "=true to overwrite existing sample index on all samples");
        }

        if (finalSamplesList.isEmpty()) {
            logger.info("Skip sample index build. Nothing to do!");
            return;
        }
        if (finalSamplesList.size() < 20) {
            logger.info("Run sample index build on samples " + finalSamplesList);
        } else {
            logger.info("Run sample index build on " + finalSamplesList.size() + " samples");
        }

        int batchSize = options.getInt(
                SAMPLE_INDEX_BUILD_MAX_SAMPLES_PER_MR.key(),
                SAMPLE_INDEX_BUILD_MAX_SAMPLES_PER_MR.defaultValue());
//        if (finalSamplesList.size() < 10) {
//            buildSampleIndexBatchMultiThread(studyId, finalSamplesList);
//        }
        if (finalSamplesList.size() > batchSize) {
            int batches = (int) Math.round(Math.ceil(finalSamplesList.size() / ((float) batchSize)));
            batchSize = (finalSamplesList.size() / batches) + 1;
            logger.warn("Unable to run sample index build in one single MapReduce operation.");
            logger.info("Split in {} jobs of {} samples each.", batches, batchSize);
            for (int i = 0; i < batches; i++) {
                List<Integer> subSet = finalSamplesList.subList(i * batchSize, Math.min((i + 1) * batchSize, finalSamplesList.size()));
                logger.info("Running MapReduce {}/{} over {} samples", i + 1, batches, subSet.size());
                buildSampleIndexBatchMapreduce(studyId, subSet, options);
            }
        } else {
            buildSampleIndexBatchMapreduce(studyId, finalSamplesList, options);
        }
    }

    private void buildSampleIndexBatchMapreduce(int studyId, List<Integer> samples, ObjectMap options)
            throws StorageEngineException {
        options = new ObjectMap(options);
        options.put(SampleIndexDriver.SAMPLE_IDS, samples);
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

    private void postSampleIndexBuild(int studyId, List<Integer> samples) throws StorageEngineException {
        postSampleIndexBuild(studyId, samples, metadataManager);
    }

    public static void postSampleIndexBuild(int studyId, List<Integer> samples, VariantStorageMetadataManager metadataManager)
            throws StorageEngineException {
        for (Integer sampleId : samples) {
            metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                return SampleIndexDBAdaptor.setSampleIndexStatus(sampleMetadata, TaskMetadata.Status.READY);
            });
        }
    }
}
