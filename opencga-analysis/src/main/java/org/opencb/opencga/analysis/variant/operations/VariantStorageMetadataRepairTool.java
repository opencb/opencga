package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.operations.variant.VariantStorageMetadataRepairToolParams;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.*;

@Tool(id = VariantStorageMetadataRepairTool.ID, description = VariantStorageMetadataRepairTool.DESCRIPTION,
        type = Tool.Type.OPERATION, scope = Tool.Scope.GLOBAL, resource = Enums.Resource.VARIANT)
public class VariantStorageMetadataRepairTool extends OperationTool {
    public static final String ID = "variant-storage-metadata-repair";
    public static final String DESCRIPTION = "Execute some repairs on Variant Storage Metadata. Advanced users only.";

    @ToolParams
    protected VariantStorageMetadataRepairToolParams toolParams;

    @Override
    protected void check() throws Exception {
        super.check();

        String userId = getCatalogManager().getUserManager().getUserId(getToken());
        if (!userId.equals(ParamConstants.OPENCGA_USER_ID)) {
            throw new CatalogAuthenticationException("Only user '" + ParamConstants.OPENCGA_USER_ID + "' can run this operation!");
        }
    }

    @Override
    protected void run() throws Exception {
        if (CollectionUtils.isEmpty(toolParams.getStudies())) {
            // Get all studies
            Query query = new Query();
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    StudyDBAdaptor.QueryParams.UID.key(),
                    StudyDBAdaptor.QueryParams.ID.key(),
                    StudyDBAdaptor.QueryParams.FQN.key()));
            OpenCGAResult<Study> studyDataResult = catalogManager.getStudyManager().get(query, options, token);

            for (Study study : studyDataResult.getResults()) {
                fixStudy(study.getFqn(), true);
            }
        } else {
            for (String study : toolParams.getStudies()) {
                String studyFqn = getStudyFqn(study);
                fixStudy(studyFqn, false);
            }
        }
    }

    private void fixStudy(String study, boolean skipMissing) throws StorageEngineException, CatalogException, ToolException {
        logger.info("Process study " + study);

        DataStore dataStore = getVariantStorageManager().getDataStore(study, getToken());
        VariantStorageMetadataManager metadataManager = getVariantStorageEngine(dataStore).getMetadataManager();
        int studyId;
        try {
            studyId = metadataManager.getStudyId(study);
        } catch (Exception e) {
            if (skipMissing) {
                logger.info("Skip study '" + study + "'. Not present in VariantStorage");
                return;
            } else {
                throw e;
            }
        }

        if (CollectionUtils.isEmpty(toolParams.getWhat())
                || toolParams.getWhat().contains(VariantStorageMetadataRepairToolParams.What.SAMPLE_FILE_ID)) {
            rebuildSampleFileIds(metadataManager, study, studyId);
        }
    }

    private void rebuildSampleFileIds(VariantStorageMetadataManager metadataManager, String study, Integer studyId)
            throws StorageEngineException, ToolException {
        List<Integer> samples = new LinkedList<>();
        metadataManager.sampleMetadataIterator(studyId).forEachRemaining(sampleMetadata -> samples.add(sampleMetadata.getId()));

        int batchSize = toolParams.getSamplesBatchSize();
        int batches = (samples.size() / batchSize) + 1;
        logger.info("Repairing {} sample file ids in {} batches", samples.size(), batches);
        int fixedSamples = 0;
        for (int batchIdx = 0; batchIdx < batches; batchIdx++) {
            logger.info("Batch {}/{}", batchIdx + 1, batches);
            Map<Integer, List<Integer>> batch = new HashMap<>();
            for (Integer sampleId : samples.subList(batchIdx * batchSize, Math.min(samples.size(), (batchIdx + 1) * batchSize))) {
                batch.put(sampleId, new LinkedList<>());
            }

            metadataManager.fileMetadataIterator(studyId).forEachRemaining(fileMetadata -> {
                for (Integer sample : fileMetadata.getSamples()) {
                    batch.computeIfPresent(sample, (key, fileIds) -> {
                        fileIds.add(fileMetadata.getId());
                        return fileIds;
                    });
                }
            });

            for (Map.Entry<Integer, List<Integer>> entry : batch.entrySet()) {
                Integer sampleId = entry.getKey();
                List<Integer> fileIds = entry.getValue();

                List<Integer> actualFiles = metadataManager.getSampleMetadata(studyId, sampleId).getFiles();
                if (actualFiles.size() != fileIds.size() || !actualFiles.containsAll(fileIds)) {
                    fixedSamples++;
                    metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                        logger.info("Repair sample {}. Had {} fileIds instead of {}.",
                                sampleMetadata.getName(), sampleMetadata.getFiles().size(), fileIds.size());
                        return sampleMetadata.setFiles(fileIds);
                    });
                }
            }
        }
        if (fixedSamples == 0) {
            logger.info("Nothing to repair!");
        } else {
            logger.info("Repaired {} samples", fixedSamples);
        }
        addAttribute(study + ".numSamples", samples.size());
        addAttribute(study + ".sampleFileIdRepairs", fixedSamples);
    }

    protected VariantStorageEngine getVariantStorageEngine(DataStore dataStore) throws StorageEngineException {
        VariantStorageEngine variantStorageEngine = StorageEngineFactory.get(getVariantStorageManager().getStorageConfiguration())
                .getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
        if (dataStore.getConfiguration() != null) {
            variantStorageEngine.getOptions().putAll(dataStore.getConfiguration());
        }
        return variantStorageEngine;
    }
}
