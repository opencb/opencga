package org.opencb.opencga.storage.core.variant.index.sample;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.OVERWRITE;
import static org.opencb.opencga.storage.core.metadata.models.TaskMetadata.Status;

/**
 * Created by jacobo on 04/01/19.
 */
public abstract class SampleIndexAnnotationLoader {

    protected final VariantStorageMetadataManager metadataManager;
    protected final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final Logger logger = LoggerFactory.getLogger(SampleIndexAnnotationLoader.class);

    public SampleIndexAnnotationLoader(SampleIndexDBAdaptor sampleIndexDBAdaptor) {
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.metadataManager = sampleIndexDBAdaptor.getMetadataManager();
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
        int sampleIndexVersion = sampleIndexDBAdaptor.getSchemaLatest(studyId).getVersion();
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

        if (finalSamplesList.size() < 20) {
            logger.info("Run sample index annotation on samples " + finalSamplesList);
        } else {
            logger.info("Run sample index annotation on " + finalSamplesList.size() + " samples");
        }

        run(studyId, finalSamplesList, sampleIndexVersion, options);

        postAnnotationLoad(studyId, sampleIndexVersion);
    }

    protected void run(int studyId, List<Integer> samples, int sampleIndexVersion, ObjectMap options)
            throws StorageEngineException {
        // By default, run all in a single batch
        runBatch(studyId, samples, sampleIndexVersion, options);
        postRunBatch(studyId, samples, sampleIndexVersion);
    }

    protected abstract void runBatch(int studyId, List<Integer> samples, int sampleIndexVersion, ObjectMap options)
            throws StorageEngineException;

    public void postRunBatch(int studyId, List<Integer> samples, int version)
            throws StorageEngineException {
        for (Integer sampleId : samples) {
            metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                sampleMetadata.setSampleIndexAnnotationStatus(Status.READY, version);
            });
        }
    }

    public void postAnnotationLoad(int studyId, int version)
            throws StorageEngineException {
        sampleIndexDBAdaptor.updateSampleIndexSchemaStatus(studyId, version);
    }

}
