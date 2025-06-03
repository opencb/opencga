package com.zettagenomics.opencga.enterprise.app.cli.admin.executors;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.opencb.opencga.app.cli.admin.executors.StorageCommandExecutor;
import org.opencb.opencga.app.cli.admin.options.StorageCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.project.DataStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class EnterpriseStorageCommandExecutor extends StorageCommandExecutor {
    public EnterpriseStorageCommandExecutor(StorageCommandOptions storageCommandOptions) {
        super(storageCommandOptions);
    }

    protected DataStore getCvdbDatastore(String project, CatalogManager catalogManager, Path opencgaHome)
            throws CatalogException, CvdbException, IOException {
        // Get CVDB engine
        CvdbSolrEngine cvdbSolrEngine = getCvdbEngine(catalogManager, opencgaHome);
        return cvdbSolrEngine.getCvdbDatastore(project, token);
    }

    protected List<String> getCvdbProjects(List<String> organizationIds, CatalogManager catalogManager, Path opencgaHome)
            throws CatalogException, CvdbException, IOException {
        // Get CVDB engine
        CvdbSolrEngine cvdbEngine = getCvdbEngine(catalogManager, opencgaHome);
        return cvdbEngine.getCvdbProjects(organizationIds, token);
    }

    private CvdbSolrEngine getCvdbEngine(CatalogManager catalogManager, Path opencgaHome) throws IOException {
        // CVDB engine
        logger.info("Loading enterprise configuration from OpenCGA home: '{}')", opencgaHome);
        EnterpriseConfiguration enterpriseConfig = EnterpriseConfiguration.load(opencgaHome);
        return new CvdbSolrEngine(catalogManager.getConfiguration(), enterpriseConfig.getCvdb());
    }
}
