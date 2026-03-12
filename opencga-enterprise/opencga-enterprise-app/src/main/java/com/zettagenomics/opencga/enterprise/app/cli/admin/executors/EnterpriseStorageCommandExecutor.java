package com.zettagenomics.opencga.enterprise.app.cli.admin.executors;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.app.cli.admin.executors.StorageCommandExecutor;
import org.opencb.opencga.app.cli.admin.options.StorageCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.project.DataStore;

import java.io.IOException;
import java.util.List;

public class EnterpriseStorageCommandExecutor extends StorageCommandExecutor {
    public EnterpriseStorageCommandExecutor(StorageCommandOptions storageCommandOptions) {
        super(storageCommandOptions);
    }

    protected DataStore getCvdbDatastore(String projectFqn, CatalogManager catalogManager)
            throws CatalogException, CvdbException, IOException {
        CvdbSolrEngine cvdbEngine = getCvdbEngine(catalogManager);
        DataStore cvdbDatastore = cvdbEngine.getCvdbDatastore(projectFqn, token);
        cvdbEngine.close();
        logger.debug("Returning CVDB datastore: {}", cvdbDatastore);
        return cvdbDatastore;
    }

    protected List<String> getCvdbProjects(List<String> organizationIds, CatalogManager catalogManager)
            throws CatalogException, CvdbException, IOException {
        CvdbSolrEngine cvdbEngine = getCvdbEngine(catalogManager);
        List<String> cvdbProjects = cvdbEngine.getCvdbProjects(organizationIds, token);
        cvdbEngine.close();
        logger.debug("Returning CVDB project FQNs: {}", cvdbProjects == null ? "" : StringUtils.join(cvdbProjects, ", "));
        return cvdbProjects;
    }

    private CvdbSolrEngine getCvdbEngine(CatalogManager catalogManager) throws IOException {
        // CVDB engine
        EnterpriseConfiguration enterpriseConfig = EnterpriseConfiguration.load(opencgaHome);

        return new CvdbSolrEngine(enterpriseConfig.getCvdb(), catalogManager);
    }
}
