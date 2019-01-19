package org.opencb.opencga.app.cli.admin.executors.migration;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.ID;
import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.URI;

/**
 * Created on 04/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AddFilePathToStudyConfigurationMigration {

    protected static final QueryOptions QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(URI.key()));
    private final StorageConfiguration storageConfiguration;
    private final CatalogManager catalogManager;
    private final Logger logger = LoggerFactory.getLogger(AddFilePathToStudyConfigurationMigration.class);

    public AddFilePathToStudyConfigurationMigration(StorageConfiguration storageConfiguration, CatalogManager catalogManager) {
        this.storageConfiguration = storageConfiguration;
        this.catalogManager = catalogManager;
    }

    public void migrate(String sessionId) throws Exception {
        List<Project> projects = catalogManager.getProjectManager().get(new Query(), new QueryOptions(
                QueryOptions.INCLUDE, Arrays.asList(
                ProjectDBAdaptor.QueryParams.NAME.key(),
                ProjectDBAdaptor.QueryParams.ID.key()
        )), sessionId).getResult();

        StorageEngineFactory factory = StorageEngineFactory.get(storageConfiguration);

        Set<DataStore> dataStores = new HashSet<>();
        for (Project project : projects) {
            logger.info("Migrating project " + project.getName());
            for (Study study : project.getStudies()) {
                DataStore dataStore = StorageOperation.getDataStore(catalogManager, study.getFqn(), File.Bioformat.VARIANT, sessionId);
                if (dataStores.add(dataStore)) {
                    VariantStorageEngine engine = factory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());

                    VariantStorageMetadataManager scm = engine.getMetadataManager();
                    for (String studyName : scm.getStudyNames(null)) {
                        StudyConfiguration sc = scm.getStudyConfiguration(studyName, null).first();
                        logger.info("Migrating study " + sc.getName());

                        for (Map.Entry<String, Integer> entry : sc.getFileIds().entrySet()) {
                            String fileName = entry.getKey();
                            Integer fileId = entry.getValue();
                            if (!sc.getFilePaths().containsValue(fileId)) {
                                // Search filePath in catalog
                                logger.info("Register path from file = " + fileName);
                                QueryResult<File> queryResult = catalogManager.getFileManager().get(studyName, new Query()
                                        .append(ID.key(), fileName), QUERY_OPTIONS, sessionId);
                                File file = null;
                                if (queryResult.getResult().size() == 1) {
                                    file = queryResult.first();
                                } else {
                                    for (File i : queryResult.getResult()) {
                                        if (i.getIndex().getStatus() != null && Status.READY.equals(i.getIndex().getStatus().getName())) {
                                            if (file != null) {
                                                throw new IllegalStateException("Error migrating storage. "
                                                        + "Unable to determine which file is indexed");
                                            }
                                            file = i;
                                        }
                                    }
                                }
                                if (file == null) {
                                    throw new IllegalStateException("Error migrating storage. File not found in catalog");
                                }
                                sc.getFilePaths().put(file.getUri().getPath(), fileId);
                            }
                        }
                        scm.updateStudyConfiguration(sc, null);
                    }
                }
            }
        }
    }
}