package org.opencb.opencga.analysis.variant;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.CatalogProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.StorageManagerFactory;

import java.util.Arrays;

/**
 * Created by pfurio on 23/08/16.
 */
public abstract class AbstractFileIndexer {

    public static DataStore getDataStore(CatalogManager catalogManager, long studyId, File.Bioformat bioformat, String sessionId)
            throws CatalogException {
        Study study = catalogManager.getStudyManager().read(studyId, new QueryOptions(), sessionId).first();
        DataStore dataStore;
        if (study.getDataStores() != null && study.getDataStores().containsKey(bioformat)) {
            dataStore = study.getDataStores().get(bioformat);
        } else {
            long projectId = catalogManager.getStudyManager().getProjectId(study.getId());
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(CatalogProjectDBAdaptor.QueryParams.ALIAS.key(), CatalogProjectDBAdaptor.QueryParams.DATASTORES.key())
            );
            Project project = catalogManager.getProjectManager().read(projectId, queryOptions, sessionId).first();
            if (project != null && project.getDataStores() != null && project.getDataStores().containsKey(bioformat)) {
                dataStore = project.getDataStores().get(bioformat);
            } else { //get default datastore
                //Must use the UserByStudyId instead of the file owner.
                String userId = catalogManager.getStudyManager().getUserId(studyId);
                String alias = project.getAlias();

                // TODO: We should be reading storageConfiguration, where the database prefix should be stored.
//                String prefix = Config.getAnalysisProperties().getProperty(OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX, "opencga_");
                String prefix;
                if (StringUtils.isNotEmpty(catalogManager.getCatalogConfiguration().getDatabasePrefix())) {
                    prefix = catalogManager.getCatalogConfiguration().getDatabasePrefix();
                    if (!prefix.endsWith("_")) {
                        prefix += "_";
                    }
                } else {
                    prefix = "opencga_";
                }

                String dbName = prefix + userId + "_" + alias;
                dataStore = new DataStore(StorageManagerFactory.get().getDefaultStorageManagerName(), dbName);
            }
        }
        return dataStore;
    }
}
