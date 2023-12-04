package org.opencb.opencga.app.cli.admin.executors.migration.storage;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 14/03/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractStorageMigrator {

    protected final CatalogManager catalogManager;
    protected final StorageConfiguration storageConfiguration;
    private final Logger logger = LoggerFactory.getLogger(AbstractStorageMigrator.class);

    public AbstractStorageMigrator(StorageConfiguration storageConfiguration, CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        this.storageConfiguration = storageConfiguration;
    }

//    public void migrate(String sessionId) throws CatalogException, StorageEngineException {
//
//        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
//
//        List<Project> projects = catalogManager.getProjectManager().search(organizationId, new Query(), new QueryOptions(
//                QueryOptions.INCLUDE, Arrays.asList(
//                ProjectDBAdaptor.QueryParams.NAME.key(),
//                ProjectDBAdaptor.QueryParams.ID.key(),
//                ProjectDBAdaptor.QueryParams.FQN.key(),
//                ProjectDBAdaptor.QueryParams.ORGANISM.key(),
//                ProjectDBAdaptor.QueryParams.STUDY.key()
//        )), sessionId).getResults();
//
//        Set<DataStore> dataStores = new HashSet<>();
//        for (Project project : projects) {
//            logger.info("Migrating project " + project.getName());
//
//            for (Study study : project.getStudies()) {
//                logger.info("Migrating study " + study.getName());
//
//                DataStore dataStore = VariantStorageManager.getDataStore(catalogManager, study.getFqn(), File.Bioformat.VARIANT, sessionId);
//                // Check only once per datastore
//                if (dataStores.add(dataStore)) {
//                    VariantStorageEngine variantStorageEngine =
//                            storageEngineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
//
//                    migrate(variantStorageEngine, sessionId);
//
//                } else {
//                    logger.info("Nothing to migrate!");
//                }
//            }
//        }
//    }

    protected abstract void migrate(VariantStorageEngine variantStorageEngine, String sessionId)
            throws StorageEngineException, CatalogException;

}
