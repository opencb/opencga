package org.opencb.opencga.analysis.variant.manager.operations;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.VariantSetupResult;
import org.opencb.opencga.core.models.variant.VariantSetupParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VariantSetupOperationManager extends OperationManager {


    public static final String ID = "variant-setup";
    private static Logger logger = LoggerFactory.getLogger(VariantSetupOperationManager.class);

    public VariantSetupOperationManager(VariantStorageManager variantStorageManager, VariantStorageEngine variantStorageEngine) {
        super(variantStorageManager, variantStorageEngine);
    }

    public VariantSetupResult setup(String studyFqn, VariantSetupParams params, String token)
            throws CatalogException, StorageEngineException {
        // Copy params to avoid modifying input object
        params = new VariantSetupParams(params);
        check(studyFqn, params, token);

        VariantSetupResult result = new VariantSetupResult();
        result.setDate(TimeUtils.getTime());
        result.setUserId(catalogManager.getUserManager().getUserIdContextStudy(studyFqn, token));
        result.setParams(params.toObjectMap());
        result.setStatus(VariantSetupResult.Status.READY);

        inferParams(params);

        ObjectMap options = variantStorageEngine.inferConfigurationParams(params);
        result.setOptions(options);

        catalogManager.getStudyManager().setVariantEngineSetupOptions(studyFqn, result, token);

        return result;
    }

    /**
     * Infer some parameters from others.
     *  - averageFileSize inferred from fileType
     *  - samplesPerFile inferred from dataDistribution or expectedSamplesNumber and expectedFilesNumber
     *  - numberOfVariantsPerSample inferred from fileType
     * @param params params to infer
     */
    private void inferParams(VariantSetupParams params) {
        if (params.getFileType() != null) {
            switch (params.getFileType()) {
                case GENOME_gVCF:
                    if (params.getAverageFileSize() == null) {
                        params.setAverageFileSize("1GiB");
                    }
                    if (params.getVariantsPerSample() == null) {
                        params.setVariantsPerSample(5000000);
                    }
                    break;
                case GENOME_VCF:
                    if (params.getAverageFileSize() == null) {
                        params.setAverageFileSize("500MiB");
                    }
                    if (params.getVariantsPerSample() == null) {
                        params.setVariantsPerSample(5000000);
                    }
                    break;
                case EXOME:
                    if (params.getAverageFileSize() == null) {
                        params.setAverageFileSize("100MiB");
                    }
                    if (params.getVariantsPerSample() == null) {
                        params.setVariantsPerSample(100000);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown fileType " + params.getFileType());
            }
        }
        // Unable to tell. Use a default value for numberOfVariantsPerSample
        if (params.getVariantsPerSample() == null) {
            params.setVariantsPerSample(5000000);
        }

        if (params.getAverageSamplesPerFile() == null) {
            if (params.getDataDistribution() == null) {
                params.setAverageSamplesPerFile(params.getExpectedSamples().floatValue() / params.getExpectedFiles().floatValue());
            } else {
                switch (params.getDataDistribution()) {
                    case SINGLE_SAMPLE_PER_FILE:
                        params.setAverageSamplesPerFile(1f);
                        break;
                    case MULTIPLE_SAMPLES_PER_FILE:
                        params.setAverageSamplesPerFile(params.getExpectedSamples().floatValue() / params.getExpectedFiles().floatValue());
                        break;
                    case MULTIPLE_FILES_PER_SAMPLE:
                        // Hard to tell. Let's assume 2 samples per file
                        params.setAverageSamplesPerFile(2f);
                        break;
                    case FILES_SPLIT_BY_CHROMOSOME:
                    case FILES_SPLIT_BY_REGION:
                        params.setAverageSamplesPerFile(params.getExpectedSamples().floatValue());
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown dataDistribution " + params.getDataDistribution());
                }
            }
        }
    }

    private void check(String studyStr, VariantSetupParams params, String token) throws CatalogException, StorageEngineException {
        Study study = catalogManager.getStudyManager().get(studyStr,
                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION_VARIANT_ENGINE.key()), token)
                .first();

        VariantStorageMetadataManager metadataManager = variantStorageEngine.getMetadataManager();
        if (metadataManager.studyExists(studyStr)) {
            int studyId = metadataManager.getStudyId(studyStr);
            if (!metadataManager.getIndexedFiles(studyId).isEmpty()) {
                throw new IllegalArgumentException("Unable to execute variant-setup on study '" + studyStr + "'. "
                        + "It already has indexed files.");
            }
        }
        if (hasVariantSetup(study)) {
            logger.info("Study {} was already setup. Re executing variant-setup", studyStr);
        }

        if (params.getExpectedFiles() == null || params.getExpectedFiles() <= 0) {
            throw new IllegalArgumentException("Missing expectedFiles");
        }
        if (params.getExpectedSamples() == null || params.getExpectedSamples() <= 0) {
            throw new IllegalArgumentException("Missing expectedSamples");
        }

        if (params.getAverageFileSize() == null && params.getFileType() == null) {
            throw new IllegalArgumentException("Missing averageFileSize or fileType");
        }
    }

    public static boolean hasVariantSetup(Study study) {
        boolean hasSetup = false;
        VariantSetupResult setup = getVariantSetupResult(study);
        if (setup != null && setup.getStatus() == VariantSetupResult.Status.READY) {
            hasSetup = true;
        }
        return hasSetup;
    }

    private static VariantSetupResult getVariantSetupResult(Study study) {
        if (study.getInternal() != null
                && study.getInternal().getConfiguration() != null
                && study.getInternal().getConfiguration().getVariantEngine() != null) {
            return study.getInternal().getConfiguration().getVariantEngine().getSetup();
        }
        return null;
    }

}
