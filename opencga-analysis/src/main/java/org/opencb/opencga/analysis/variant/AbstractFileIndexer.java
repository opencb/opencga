package org.opencb.opencga.analysis.variant;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.opencb.opencga.catalog.monitor.executors.AbstractExecutor.JOB_STATUS_FILE;

/**
 * Created by pfurio on 23/08/16.
 */
public abstract class AbstractFileIndexer {

    protected final CatalogManager catalogManager;
    protected final Logger logger;
    private ObjectMapper objectMapper = new ObjectMapper();

    public AbstractFileIndexer(CatalogManager catalogManager, Logger logger) {
        this.catalogManager = catalogManager;
        this.logger = logger;
    }

    protected void outdirMustBeEmpty(Path outdir) throws CatalogIOException, AnalysisExecutionException {
        List<URI> uris = catalogManager.getCatalogIOManagerFactory().get(outdir.toUri()).listFiles(outdir.toUri());
        if (!uris.isEmpty()) {
            throw new AnalysisExecutionException("Unable to execute index. Outdir '" + outdir + "' must be empty!");
        }
    }

    public StudyConfiguration updateStudyConfiguration(String sessionId, long studyId, DataStore dataStore)
            throws IOException, CatalogException, AnalysisExecutionException {
        try (VariantDBAdaptor dbAdaptor = StorageManagerFactory.get().getVariantStorageManager(dataStore.getStorageEngine())
                .getDBAdaptor(dataStore.getDbName());
             StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager()){
            new CatalogStudyConfigurationFactory(catalogManager)
                    .updateStudyConfigurationFromCatalog(studyId, studyConfigurationManager, sessionId);
            return studyConfigurationManager.getStudyConfiguration((int) studyId, null).first();
        } catch (StorageManagerException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new AnalysisExecutionException("Unable to update StudyConfiguration", e);
        }
    }

    protected List<File> copyResults(Path tmpOutdirPath, long catalogPathOutDir, String sessionId) throws CatalogException, IOException {
        File outDir = catalogManager.getFile(catalogPathOutDir, new QueryOptions(), sessionId).first();

        FileScanner fileScanner = new FileScanner(catalogManager);
//        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(tmpOutdirPath.toUri());

        List<File> files;
        try {
            logger.info("Scanning files from {} to move to {}", tmpOutdirPath, outDir.getUri());
            // Avoid copy the job.status file!
            Predicate<URI> fileStatusFilter = uri -> !uri.getPath().endsWith(JOB_STATUS_FILE);
            files = fileScanner.scan(outDir, tmpOutdirPath.toUri(), FileScanner.FileScannerPolicy.DELETE, true, false, fileStatusFilter, -1, sessionId);
        } catch (IOException e) {
            logger.warn("IOException when scanning temporal directory. Error: {}", e.getMessage());
            throw e;
        } catch (CatalogException e) {
            logger.warn("CatalogException when scanning temporal directory. Error: {}", e.getMessage());
            throw e;
        }
        return files;
    }

    public Job.JobStatus readJobStatus(Path outdir) throws IOException {
        return objectMapper.reader(Job.JobStatus.class).readValue(outdir.resolve(JOB_STATUS_FILE).toFile());
    }

    public void writeJobStatus(Path outdir, Job.JobStatus jobStatus) throws IOException {
        objectMapper.writer().writeValue(outdir.resolve(JOB_STATUS_FILE).toFile(), jobStatus);
    }

    public static DataStore getDataStore(CatalogManager catalogManager, long studyId, File.Bioformat bioformat, String sessionId)
            throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyId, new QueryOptions(), sessionId).first();
        DataStore dataStore;
        if (study.getDataStores() != null && study.getDataStores().containsKey(bioformat)) {
            dataStore = study.getDataStores().get(bioformat);
        } else {
            long projectId = catalogManager.getStudyManager().getProjectId(study.getId());
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(ProjectDBAdaptor.QueryParams.ALIAS.key(), ProjectDBAdaptor.QueryParams.DATASTORES.key())
            );
            Project project = catalogManager.getProjectManager().get(projectId, queryOptions, sessionId).first();
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
