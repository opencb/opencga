package com.zettagenomics.opencga.enterprise.app.cli.admin.executors;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.cli.admin.executors.StorageCommandExecutor;
import org.opencb.opencga.app.cli.admin.options.StorageCommandOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.project.Project;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;

public class EnterpriseStorageCommandExecutor extends StorageCommandExecutor {
    public EnterpriseStorageCommandExecutor(StorageCommandOptions storageCommandOptions) {
        super(storageCommandOptions);
    }

    protected DataStore getCvdbDatastore(String project, CatalogManager catalogManager, Path opencgaHome)
            throws CatalogException, CvdbException {
        // Get CVDB engine
        CvdbSolrEngine cvdbSolrEngine = getCvdbEngine(catalogManager, opencgaHome);

        DataStore dataStore = VariantStorageManager.getDataStoreByProjectId(catalogManager, project, File.Bioformat.CVDB, token);
        if (dataStore.getOptions() == null) {
            dataStore.setOptions(new ObjectMap());
        }

        // Get CVDB collection names
        List<String> collectionNames = cvdbSolrEngine.getCollectionNames(dataStore.getDbName());
        dataStore.getOptions().put("collections", collectionNames);

        return dataStore;
    }

    /**
     * Get list of projects that have CVDB data.
     * @return List of projects
     * @throws Exception on error
     */
    protected List<String> getCvdbProjects(List<String> organizationIds, CatalogManager catalogManager, Path opencgaHome)
            throws CatalogException, CvdbException {
        // Get CVDB engine
        CvdbSolrEngine cvdbEngine = getCvdbEngine(catalogManager, opencgaHome);

        List<String> projectFqns = new ArrayList<>();

        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions(INCLUDE, Arrays.asList(ProjectDBAdaptor.QueryParams.ID.key(),
                ProjectDBAdaptor.QueryParams.FQN.key()));
        for (String organizationId : organizationIds) {
            List<Project> projects = catalogManager.getProjectManager().search(organizationId, query, queryOptions, token).getResults();
            for (Project project : projects) {
                String dbPrefix = VariantStorageManager.buildDatabaseName(catalogManager.getConfiguration().getDatabasePrefix(), "cvdb",
                        organizationId, project.getId());
                if (cvdbEngine.existCollections(dbPrefix)) {
                    projectFqns.add(project.getFqn());
                }
            }
        }

        return  projectFqns;
    }

    private CvdbSolrEngine getCvdbEngine(CatalogManager catalogManager, Path opencgaHome) {
        // CVDB engine
        EnterpriseConfiguration enterpriseConfig = EnterpriseConfiguration.load(opencgaHome.resolve("/enterprise-configuration.yml"));
        return new CvdbSolrEngine(catalogManager.getConfiguration(), enterpriseConfig.getCvdb());
    }


}
