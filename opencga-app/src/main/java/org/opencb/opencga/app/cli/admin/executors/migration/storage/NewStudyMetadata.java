package org.opencb.opencga.app.cli.admin.executors.migration.storage;

import org.apache.solr.common.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Replace StudyConfiguration with StudyMetadata
 *
 * Created on 14/03/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class NewStudyMetadata extends AbstractStorageMigrator {

    private final Logger logger = LoggerFactory.getLogger(NewStudyMetadata.class);
    private final AddFilePathToStudyConfigurationMigration filePathMigrator;


    public NewStudyMetadata(StorageConfiguration storageConfiguration, CatalogManager catalogManager) {
        super(storageConfiguration, catalogManager);
        filePathMigrator = new AddFilePathToStudyConfigurationMigration(catalogManager);
    }

    private static String STUDY_CONFIGURATION_MIGRATED = "studyConfigurationMigrated";

    @Override
    @SuppressWarnings("deprecation")
    protected void migrate(VariantStorageEngine variantStorageEngine, String sessionId) throws StorageEngineException, CatalogException {
        VariantStorageMetadataManager metadataManager = variantStorageEngine.getMetadataManager();


        for (Integer studyId : metadataManager.getStudyIds()) {
            StudyConfiguration sc = metadataManager.getStudyConfiguration(studyId, null).first();

            if (sc == null
                    || (sc.getFileIds().isEmpty()
                    && sc.getCohortIds().isEmpty()
                    && sc.getSampleIds().isEmpty())) {
                logger.info("Skip study " + studyId + ". Migration not needed");
                continue;
            }

            if (sc.getAttributes().getBoolean(STUDY_CONFIGURATION_MIGRATED, false)) {
                logger.info("StudyConfiguration \"" + sc.getName() + "\" (" + sc.getId() + ") already migrated. Skip!");
                continue;
            }

            StudyMetadata sm = new StudyMetadata(sc.getId(), sc.getName());
            sm.setAggregation(sc.getAggregation());
            sm.setTimeStamp(sc.getTimeStamp());
            sm.setVariantHeader(sc.getVariantHeader());
            sm.setAttributes(sc.getAttributes());

            metadataManager.unsecureUpdateStudyMetadata(sm);


            Map<Integer, List<Integer>> filesInSample = new HashMap<>();
            for (Map.Entry<String, Integer> entry : sc.getFileIds().entrySet()) {
                Integer fileId = entry.getValue();
                String fileName = entry.getKey();

                FileMetadata fileMetadata = new FileMetadata(studyId, fileId, fileName);
                if (sc.getFilePaths() != null) {
                    fileMetadata.setPath(sc.getFilePaths().inverse().get(fileId));
                }
                if (StringUtils.isEmpty(fileMetadata.getPath())) {
                    fileMetadata.setPath(filePathMigrator.getFilePath(sc.getName(), fileName, sessionId));
                }

                fileMetadata.setIndexStatus(sc.getIndexedFiles().contains(fileId) ? TaskMetadata.Status.READY : TaskMetadata.Status.NONE);
                fileMetadata.setSamples(sc.getSamplesInFiles().get(fileId));

                metadataManager.unsecureUpdateFileMetadata(studyId, fileMetadata);

                for (Integer sampleId : fileMetadata.getSamples()) {
                    filesInSample.computeIfAbsent(sampleId, id -> new ArrayList<>()).add(fileId);
                }
            }

            for (Map.Entry<String, Integer> entry : sc.getCohortIds().entrySet()) {
                Integer cohortId = entry.getValue();
                String cohortName = entry.getKey();

                CohortMetadata cohortMetadata = new CohortMetadata(studyId, cohortId, cohortName,
                        new ArrayList<>(sc.getCohorts().get(cohortId)), Collections.emptyList());
                TaskMetadata.Status status;
                if (sc.getCalculatedStats().contains(cohortId)) {
                    status = TaskMetadata.Status.READY;
                } else if (sc.getInvalidStats().contains(cohortId)) {
                    status = TaskMetadata.Status.ERROR;
                } else {
                    status = TaskMetadata.Status.NONE;
                }
                cohortMetadata.setStatsStatus(status);

                metadataManager.unsecureUpdateCohortMetadata(studyId, cohortMetadata);
            }

            for (Map.Entry<String, Integer> entry : sc.getSampleIds().entrySet()) {
                Integer sampleId = entry.getValue();
                String sampleName = entry.getKey();

                SampleMetadata sampleMetadata = new SampleMetadata(studyId, sampleId, sampleName);
                List<Integer> files = filesInSample.get(sampleId);
                if (files != null) {
                    sampleMetadata.setFiles(new ArrayList<>(files));
                } else {
                    sampleMetadata.setFiles(new ArrayList<>());
                }
                if (sampleMetadata.getFiles().stream().anyMatch(sc.getIndexedFiles()::contains)) {
                    sampleMetadata.setIndexStatus(TaskMetadata.Status.READY);
                }
                sampleMetadata.setCohorts(new HashSet<>());
                sc.getCohorts().forEach((cohortId, samples) -> {
                    if (samples.contains(sampleId)) {
                        sampleMetadata.getCohorts().add(cohortId);
                    }
                });

                metadataManager.unsecureUpdateSampleMetadata(studyId, sampleMetadata);
            }

            for (TaskMetadata taskMetadata : sc.getBatches()) {
                metadataManager.unsecureUpdateTask(studyId, taskMetadata);
            }

            sc.getAttributes().put(STUDY_CONFIGURATION_MIGRATED, true);
            metadataManager.updateStudyConfiguration(sc, null);
        }
    }

}
