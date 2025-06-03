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
        return cvdbSolrEngine.getCvdbDatastore(project, token);
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
        return cvdbEngine.getCvdbProjects(organizationIds, token);
    }

    private CvdbSolrEngine getCvdbEngine(CatalogManager catalogManager, Path opencgaHome) {
        // CVDB engine
        Path path = opencgaHome.resolve("conf/enterprise-configuration.yml");
        logger.info("Loading enterprise configuration from '{}' (OpenCGA home: '{}')", path, opencgaHome);
        EnterpriseConfiguration enterpriseConfig = EnterpriseConfiguration.load(path);
        return new CvdbSolrEngine(catalogManager.getConfiguration(), enterpriseConfig.getCvdb());
    }


}
