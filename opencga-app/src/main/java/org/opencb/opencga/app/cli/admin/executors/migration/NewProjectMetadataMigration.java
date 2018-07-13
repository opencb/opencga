package org.opencb.opencga.app.cli.admin.executors.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.admin.options.MigrationCommandOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName;

/**
 * Created on 10/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class NewProjectMetadataMigration {

    private final StorageConfiguration storageConfiguration;
    private final CatalogManager catalogManager;
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(NewProjectMetadataMigration.class);

    public NewProjectMetadataMigration(StorageConfiguration storageConfiguration, CatalogManager catalogManager,
                                       MigrationCommandOptions.MigrateV1_4_0CommandOptions options) {
        this.storageConfiguration = storageConfiguration;
        this.catalogManager = catalogManager;
        objectMapper = new ObjectMapper()
                .addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    public void migrate(String sessionId) throws Exception {
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(storageConfiguration);

        List<Project> projects = catalogManager.getProjectManager().get(new Query(), new QueryOptions(
                QueryOptions.INCLUDE, Arrays.asList(
                ProjectDBAdaptor.QueryParams.NAME.key(),
                ProjectDBAdaptor.QueryParams.ID.key(),
                ProjectDBAdaptor.QueryParams.FQN.key(),
                ProjectDBAdaptor.QueryParams.ORGANISM.key(),
                ProjectDBAdaptor.QueryParams.STUDY.key()
        )), sessionId).getResult();

        Set<DataStore> dataStores = new HashSet<>();
        for (Project project : projects) {
            logger.info("Migrating project " + project.getName());

            for (Study study : project.getStudies()) {
                logger.info("Migrating study " + study.getName());

                long numIndexedFiles = catalogManager.getFileManager()
                        .count(study.getFqn(), new Query(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), Arrays.asList(
                                FileIndex.IndexStatus.TRANSFORMED,
                                FileIndex.IndexStatus.TRANSFORMING,
                                FileIndex.IndexStatus.LOADING,
                                FileIndex.IndexStatus.INDEXING,
                                FileIndex.IndexStatus.READY
                                )), sessionId)
                        .getNumTotalResults();

                if (numIndexedFiles > 0) {
                    DataStore dataStore = StorageOperation.getDataStore(catalogManager, study.getFqn(), File.Bioformat.VARIANT, sessionId);
                    if (dataStores.add(dataStore)) {

                        VariantStorageEngine variantStorageEngine = storageEngineFactory
                                .getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());

                        variantStorageEngine.getStudyConfigurationManager().lockAndUpdateProject(projectMetadata -> {
                            if (projectMetadata == null || StringUtils.isEmpty(projectMetadata.getSpecies())) {
                                logger.info("Create ProjectMetadata for project " + project.getFqn());

                                String scientificName = toCellBaseSpeciesName(project.getOrganism().getScientificName());
                                projectMetadata = new ProjectMetadata(
                                        scientificName,
                                        project.getOrganism().getAssembly(),
                                        project.getCurrentRelease());
                            } else {
                                logger.info("ProjectMetadata already exists for project " + project.getFqn() + ". Nothing to do!");
                            }
                            return projectMetadata;
                        });
                    }
                } else {
                    logger.info("Nothing to migrate!");
                }
            }
        }
    }

}
