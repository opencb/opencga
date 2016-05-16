package org.opencb.opencga.app.cli.analysis;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.StorageManagerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 10/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AnalysisStorageCommandExecutor extends AnalysisCommandExecutor {

    protected CatalogManager catalogManager;
    protected StorageManagerFactory storageManagerFactory;


    public AnalysisStorageCommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
        super(options);
    }


    protected void configure()
            throws IllegalAccessException, ClassNotFoundException, InstantiationException, CatalogException {

        //  Creating CatalogManager
        catalogManager = new CatalogManager(catalogConfiguration);

        // Creating StorageManagerFactory
        storageManagerFactory = new StorageManagerFactory(storageConfiguration);

    }


    protected Job getJob(long studyId, String jobId, String sessionId) throws CatalogException {
        return catalogManager.getAllJobs(studyId, new Query(CatalogJobDBAdaptor.QueryParams.RESOURCE_MANAGER_ATTRIBUTES.key() + "." + Job.JOB_SCHEDULER_NAME, jobId), null, sessionId).first();
    }

    protected Map<Long, String> getStudyIds(String sessionId) throws CatalogException {
        return catalogManager.getAllStudies(new Query(), new QueryOptions("include", "projects.studies.id,projects.studies.alias"), sessionId)
                .getResult()
                .stream()
                .collect(Collectors.toMap(Study::getId, Study::getAlias));
    }
}
