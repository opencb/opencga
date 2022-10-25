package org.opencb.opencga.storage.core.variant.score;

import org.apache.solr.common.StringUtils;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnector;
import org.opencb.opencga.storage.core.io.plain.StringDataReader;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

public abstract class VariantScoreLoader {

    protected final VariantStorageMetadataManager metadataManager;
    protected final IOConnector ioConnector;

    protected VariantScoreLoader(VariantStorageMetadataManager metadataManager, IOConnector ioConnector) {
        this.metadataManager = metadataManager;
        this.ioConnector = ioConnector;
    }

    public final VariantScoreMetadata loadVariantScore(URI scoreFile, String study, String scoreName,
                                                 String cohort1, String cohort2, VariantScoreFormatDescriptor descriptor, ObjectMap options)
            throws StorageEngineException {
        descriptor.checkValid();

        VariantScoreMetadata scoreMetadata = preLoad(study, scoreName, cohort1, cohort2, options);

        boolean success = false;
        try {
            load(scoreFile, scoreMetadata, descriptor, options);
            success = true;
        } catch (Exception e) {
            success = false;
            throw new StorageEngineException("Error loading VariantScore " + scoreName + " from file " + scoreFile, e);
        } finally {
            scoreMetadata = postLoad(scoreMetadata, success);
        }

        return scoreMetadata;
    }

    protected abstract void load(URI scoreFile, VariantScoreMetadata scoreMetadata,
                                 VariantScoreFormatDescriptor descriptor, ObjectMap options)
            throws ExecutionException, IOException;

    protected VariantScoreMetadata preLoad(String study, String scoreName, String cohort1, String cohort2, ObjectMap options)
            throws StorageEngineException {

        int studyId = metadataManager.getStudyId(study);
        Integer cohortId1 = metadataManager.getCohortId(studyId, cohort1);
        if (cohortId1 == null) {
            throw VariantQueryException.cohortNotFound(cohort1, studyId, metadataManager);
        }
        Integer cohortId2;
        if (StringUtils.isEmpty(cohort2)) {
            cohortId2 = null;
        } else {
            cohortId2 = metadataManager.getCohortId(studyId, cohort2);
            if (cohortId2 == null) {
                throw VariantQueryException.cohortNotFound(cohort2, studyId, metadataManager);
            }
        }

        boolean resume = options.getBoolean(VariantStorageOptions.RESUME.key(), false);

        VariantScoreMetadata variantScoreMetadata =
                metadataManager.getOrCreateVariantScoreMetadata(studyId, scoreName, cohortId1, cohortId2);

        return metadataManager.updateVariantScoreMetadata(variantScoreMetadata.getStudyId(), variantScoreMetadata.getId(), vsm -> {
            if (resume || vsm.getIndexStatus().equals(TaskMetadata.Status.NONE) || vsm.getIndexStatus().equals(TaskMetadata.Status.ERROR)) {
                vsm.setIndexStatus(TaskMetadata.Status.RUNNING);
            } else {
                throw new StorageEngineException("Variant score '" + scoreName + "' is in status '" + vsm.getIndexStatus() + "'.");
            }
        });
    }

    protected VariantScoreMetadata postLoad(VariantScoreMetadata variantScoreMetadata, boolean success) throws StorageEngineException {
        return metadataManager.updateVariantScoreMetadata(variantScoreMetadata.getStudyId(), variantScoreMetadata.getId(), vsm -> {
            if (success) {
                vsm.setIndexStatus(TaskMetadata.Status.READY);
            } else {
                vsm.setIndexStatus(TaskMetadata.Status.ERROR);
            }
        });
    }

    protected StringDataReader getDataReader(URI scoreFile) throws IOException {
        StringDataReader stringReader = new StringDataReader(scoreFile, ioConnector);
        ProgressLogger progressLogger = new ProgressLogger("Loading variant score:", ioConnector.size(scoreFile), 200);
        stringReader.setReadBytesListener((totalRead, delta) -> progressLogger.increment(delta, "Bytes"));
        return stringReader;
    }

    protected VariantScoreParser newParser(VariantScoreMetadata scoreMetadata,
                                           VariantScoreFormatDescriptor descriptor) {

        final String cohortName = metadataManager.getCohortName(scoreMetadata.getStudyId(), scoreMetadata.getCohortId1());
        final String secondCohortName;
        if (scoreMetadata.getCohortId2() != null) {
            secondCohortName = metadataManager.getCohortName(scoreMetadata.getStudyId(), scoreMetadata.getCohortId1());
        } else {
            secondCohortName = null;
        }
        return new VariantScoreParser(descriptor, scoreMetadata, cohortName, secondCohortName);
    }

}
