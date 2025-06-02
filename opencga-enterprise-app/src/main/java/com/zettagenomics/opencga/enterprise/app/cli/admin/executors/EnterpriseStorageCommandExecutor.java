package com.zettagenomics.opencga.enterprise.app.cli.admin.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.cli.admin.executors.StorageCommandExecutor;
import org.opencb.opencga.app.cli.admin.options.StorageCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.DataStore;

import java.util.Collections;
import java.util.List;

public class EnterpriseStorageCommandExecutor extends StorageCommandExecutor {
    public EnterpriseStorageCommandExecutor(StorageCommandOptions storageCommandOptions) {
        super(storageCommandOptions);
    }


    @Override
    protected DataStore getCvdbDatastore(String project, CatalogManager catalogManager) throws CatalogException {
        DataStore dataStore = VariantStorageManager.getDataStoreByProjectId(catalogManager, project, File.Bioformat.CVDB, token);
        if (dataStore.getOptions() == null) {
            dataStore.setOptions(new ObjectMap());
        }
        dataStore.getOptions().put("collections", [...]);
        return dataStore;
    }

    /**
     * Get list of projects that have CVDB data.
     * @return List of projects
     * @throws Exception on error
     */
    @Override
    protected List<String> getCvdbProjects(List<String> organizationIds, CatalogManager catalogManager) throws Exception {
        // TODO
    }

}
