package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.mr.SampleIndexAnnotationLoaderDriver;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.OVERWRITE;
import static org.opencb.opencga.storage.core.metadata.models.TaskMetadata.Status;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.SAMPLE_INDEX_ANNOTATION_MAX_SAMPLES_PER_MR;

/**
 * Created by jacobo on 04/01/19.
 */
public class SampleIndexAnnotationLoader {

    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final MRExecutor mrExecutor;
    private final SampleIndexDBAdaptor sampleDBAdaptor;
    private final VariantStorageMetadataManager metadataManager;
    private final Logger logger = LoggerFactory.getLogger(SampleIndexAnnotationLoader.class);

    public SampleIndexAnnotationLoader(HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                       VariantStorageMetadataManager metadataManager, MRExecutor mrExecutor) {
        this.tableNameGenerator = tableNameGenerator;
        this.mrExecutor = mrExecutor;
        this.metadataManager = metadataManager;
        this.sampleDBAdaptor = new SampleIndexDBAdaptor(hBaseManager, tableNameGenerator, this.metadataManager);
    }

    public SampleIndexAnnotationLoader(SampleIndexDBAdaptor sampleDBAdaptor, MRExecutor mrExecutor) {
        this.mrExecutor = mrExecutor;
        this.sampleDBAdaptor = sampleDBAdaptor;
        this.metadataManager = sampleDBAdaptor.getMetadataManager();
        this.tableNameGenerator = sampleDBAdaptor.getTableNameGenerator();
    }

    public void updateSampleAnnotation(String study, List<String> samples, ObjectMap options)
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

        updateSampleAnnotation(studyId, sampleIds, options, options.getBoolean(OVERWRITE, false));
    }

    public void updateSampleAnnotation(int studyId, List<Integer> samples, ObjectMap options)
            throws StorageEngineException {
        updateSampleAnnotation(studyId, samples, options, options.getBoolean(OVERWRITE, false));
    }

    public void updateSampleAnnotation(int studyId, List<Integer> samples, ObjectMap options, boolean overwrite)
            throws StorageEngineException {
        int sampleIndexVersion = sampleDBAdaptor.getSchemaLatest(studyId).getVersion();
        List<Integer> finalSamplesList = new ArrayList<>(samples.size());
        List<String> nonAnnotated = new LinkedList<>();
        List<String> alreadyAnnotated = new LinkedList<>();
        for (Integer sampleId : samples) {
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
            if (sampleMetadata.isAnnotated()) {
                if (sampleMetadata.getSampleIndexAnnotationStatus(sampleIndexVersion).equals(Status.READY)
                        && !overwrite) {
                    // SamplesIndex already annotated
                    alreadyAnnotated.add(sampleMetadata.getName());
                } else {
                    finalSamplesList.add(sampleId);
                }
            } else {
                // Discard non-annotated samples
                nonAnnotated.add(sampleMetadata.getName());
            }
        }
        if (!nonAnnotated.isEmpty()) {
            if (nonAnnotated.size() < 20) {
                logger.warn("Unable to update sample index from samples " + nonAnnotated + ". Samples not fully annotated.");
            } else {
                logger.warn("Unable to update sample index from " + nonAnnotated.size() + " samples. Samples not fully annotated.");
            }
        }
        if (!alreadyAnnotated.isEmpty()) {
            logger.info("Skip sample index annotation for " + alreadyAnnotated.size() + " samples."
                    + " Add " + OVERWRITE + "=true to overwrite existing sample index annotation on all samples");
        }

        if (finalSamplesList.isEmpty()) {
            logger.info("Skip sample index annotation. Nothing to do!");
            return;
        }

        sampleDBAdaptor.createTableIfNeeded(studyId, sampleIndexVersion, options);

        if (finalSamplesList.size() < 20) {
            logger.info("Run sample index annotation on samples " + finalSamplesList);
        } else {
            logger.info("Run sample index annotation on " + finalSamplesList.size() + " samples");
        }

        int batchSize = options.getInt(
                SAMPLE_INDEX_ANNOTATION_MAX_SAMPLES_PER_MR.key(),
                SAMPLE_INDEX_ANNOTATION_MAX_SAMPLES_PER_MR.defaultValue());
//        if (finalSamplesList.size() < 10) {
//            updateSampleAnnotationBatchMultiThread(studyId, finalSamplesList);
//        }
        if (finalSamplesList.size() > batchSize) {
            int batches = (int) Math.round(Math.ceil(finalSamplesList.size() / ((float) batchSize)));
            batchSize = (finalSamplesList.size() / batches) + 1;
            logger.warn("Unable to run sample index annotation in one single MapReduce operation.");
            logger.info("Split in {} jobs of {} samples each.", batches, batchSize);
            for (int i = 0; i < batches; i++) {
                List<Integer> subSet = finalSamplesList.subList(i * batchSize, Math.min((i + 1) * batchSize, finalSamplesList.size()));
                logger.info("Running MapReduce {}/{} over {} samples", i + 1, batches, subSet.size());
                updateSampleAnnotationBatchMapreduce(studyId, subSet, sampleIndexVersion, options);
            }
        } else {
            updateSampleAnnotationBatchMapreduce(studyId, finalSamplesList, sampleIndexVersion, options);
        }

        postAnnotationLoad(studyId, sampleIndexVersion);
    }

    private void updateSampleAnnotationBatchMapreduce(int studyId, List<Integer> samples, int sampleIndexVersion, ObjectMap options)
            throws StorageEngineException {
        options.put(SampleIndexAnnotationLoaderDriver.OUTPUT, sampleDBAdaptor.getSampleIndexTableNameLatest(studyId));
        options.put(SampleIndexAnnotationLoaderDriver.SAMPLE_INDEX_VERSION, sampleIndexVersion);
        mrExecutor.run(SampleIndexAnnotationLoaderDriver.class, SampleIndexAnnotationLoaderDriver.buildArgs(
                tableNameGenerator.getArchiveTableName(studyId),
                tableNameGenerator.getVariantTableName(), studyId, samples, options),
                "Annotate sample index for " + (samples.size() < 10 ? "samples " + samples : samples.size() + " samples"));

        postAnnotationBatchLoad(studyId, samples, sampleIndexVersion);
    }

    public void postAnnotationBatchLoad(int studyId, List<Integer> samples, int version)
            throws StorageEngineException {
        for (Integer sampleId : samples) {
            metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                sampleMetadata.setSampleIndexAnnotationStatus(Status.READY, version);
            });
        }
    }

    public void postAnnotationLoad(int studyId, int version)
            throws StorageEngineException {
        sampleDBAdaptor.updateSampleIndexSchemaStatus(studyId, version);
    }
}
