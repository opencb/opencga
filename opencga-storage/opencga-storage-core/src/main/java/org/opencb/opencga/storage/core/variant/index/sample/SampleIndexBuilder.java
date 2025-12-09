package org.opencb.opencga.storage.core.variant.index.sample;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.OVERWRITE;

public abstract class SampleIndexBuilder {

    protected final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    protected final String study;
    protected final VariantStorageMetadataManager metadataManager;
    protected final SampleIndexSchema schema;
    private Logger logger = LoggerFactory.getLogger(SampleIndexBuilder.class);

    public SampleIndexBuilder(SampleIndexDBAdaptor sampleIndexDBAdaptor, String study) {
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.study = study;
        metadataManager = sampleIndexDBAdaptor.getMetadataManager();
        schema = sampleIndexDBAdaptor.getSchemaLatest(study);
    }


    public void buildSampleIndex(List<String> samples, ObjectMap options, boolean overwrite)
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
        int version = schema.getVersion();
        List<Integer> finalSamplesList = new ArrayList<>(samples.size());
        List<String> alreadyIndexed = new LinkedList<>();
        for (Integer sampleId : sampleIds) {
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
            if (overwrite || sampleMetadata.getSampleIndexStatus(version) != TaskMetadata.Status.READY) {
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

        buildSampleIndexBatch(studyId, finalSamplesList, options);
    }

    protected abstract void buildSampleIndexBatch(int studyId, List<Integer> sampleIds, ObjectMap options) throws StorageEngineException;

    protected void postSampleIndexBuild(int studyId, List<Integer> samples) throws StorageEngineException {
        for (Integer sampleId : samples) {
            metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                sampleMetadata.setSampleIndexStatus(TaskMetadata.Status.READY, schema.getVersion());
            });
        }
    }

}
