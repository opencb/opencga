package org.opencb.opencga.storage.core.variant.score;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class VariantScoreRemover {

    private static Logger logger = LoggerFactory.getLogger(VariantScoreRemover.class);
    protected final VariantStorageMetadataManager metadataManager;

    protected VariantScoreRemover(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public final void remove(String study, String scoreName, ObjectMap options) throws StorageEngineException {

        VariantScoreMetadata scoreMetadata = preRemove(study, scoreName, options);

        try {
            remove(scoreMetadata, options);
            postRemove(scoreMetadata, true);
        } catch (Exception e) {
            postRemove(scoreMetadata, false);
            throw new StorageEngineException("Error removing VariantScore " + scoreName, e);
        }
    }

    protected abstract void remove(VariantScoreMetadata scoreMetadata, ObjectMap options) throws StorageEngineException;

    protected VariantScoreMetadata preRemove(String study, String scoreName, ObjectMap options) throws StorageEngineException {
        int studyId = metadataManager.getStudyId(study);
        VariantScoreMetadata scoreMetadata = metadataManager.getVariantScoreMetadata(studyId, scoreName);
        if (scoreMetadata == null) {
            throw new StorageEngineException("Score '" + scoreName + "' not found in study '" + study + "'");
        }
        int scoreId = scoreMetadata.getId();

        boolean resume = options.getBoolean(VariantStorageOptions.RESUME.key(), false);
        boolean force = options.getBoolean(VariantStorageOptions.FORCE.key(), false);

        return metadataManager.updateVariantScoreMetadata(studyId, scoreId, variantScoreMetadata -> {
            switch (variantScoreMetadata.getRemoveStatus()) {
                case RUNNING:
                    // Unexpected status. Resume to continue
                    if (!resume) {
                        throw new StorageEngineException("Score '" + scoreName + "' already being removed from study '" + study + "'. "
                                + "To resume a failed remove, rerun with " + VariantStorageOptions.RESUME.key() + "=true");
                    }
                    // Do not break
                case ERROR:
                    logger.info("Resume remove score from " + variantScoreMetadata.getRemoveStatus());
                    break;
                case NONE:
                    break;
                default:
                    throw new StorageEngineException("Unexpected remove status " + variantScoreMetadata.getRemoveStatus());
            }
            variantScoreMetadata.setRemoveStatus(TaskMetadata.Status.RUNNING);

            switch (variantScoreMetadata.getIndexStatus()) {
                case NONE:
                    throw new StorageEngineException("Score '" + scoreName + "' not indexed in study '" + study + "'");
                case ERROR:
                case READY:
                    // Expected status. Continue
                    break;
                case RUNNING:
                case DONE:
                default:
                    // Unexpected status. Force to continue
                    if (!force) {
                        throw new StorageEngineException("Score '" + scoreName + "' is in status "
                                + "'" + variantScoreMetadata.getIndexStatus() + "' right now. "
                                + "To force remove, rerun with " + VariantStorageOptions.FORCE.key() + "=true"
                        );
                    }
                    break;
            }
            variantScoreMetadata.setIndexStatus(TaskMetadata.Status.ERROR);
        });
    }

    protected void postRemove(VariantScoreMetadata scoreMetadata, boolean success) throws StorageEngineException {
        if (success) {
            metadataManager.removeVariantScoreMetadata(scoreMetadata);
        } else {
            metadataManager.updateVariantScoreMetadata(scoreMetadata.getStudyId(), scoreMetadata.getId(), variantScoreMetadata -> {
                variantScoreMetadata.setRemoveStatus(TaskMetadata.Status.ERROR);
                variantScoreMetadata.setIndexStatus(TaskMetadata.Status.ERROR);
            });
        }
    }

}
