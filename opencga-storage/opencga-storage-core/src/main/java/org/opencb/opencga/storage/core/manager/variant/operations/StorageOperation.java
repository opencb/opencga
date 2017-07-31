/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.manager.variant.operations;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.variant.CatalogStudyConfigurationFactory;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.opencb.opencga.catalog.monitor.executors.AbstractExecutor.*;

/**
 * Created by pfurio on 23/08/16.
 */
public abstract class StorageOperation {

    public static final String CATALOG_PATH = "catalogPath";

    protected final CatalogManager catalogManager;
    protected final StorageEngineFactory storageEngineFactory;
    protected final Logger logger;
    private ObjectMapper objectMapper = new ObjectMapper();

    public StorageOperation(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory, Logger logger) {
        this.catalogManager = catalogManager;
        this.storageEngineFactory = storageEngineFactory;
        this.logger = logger;
    }


    protected void outdirMustBeEmpty(Path outdir, ObjectMap options) throws CatalogIOException, StorageEngineException {
        if (!isCatalogPathDefined(options)) {
            // This restriction is only necessary if the output files are going to be moved to Catalog.
            // If CATALOG_PATH is NOT defined, output does not need to be empty.
            return;
        }
        List<URI> uris = catalogManager.getCatalogIOManagerFactory().get(outdir.toUri()).listFiles(outdir.toUri());
        if (!uris.isEmpty()) {
            // Only allow stdout and stderr files
            for (URI uri : uris) {
                // Obtain the extension
                int i = uri.toString().lastIndexOf(".");
                if (i <= 0) {
                    throw new StorageEngineException("Unable to execute storage operation. Outdir '" + outdir + "' must be empty!");
                }
                String extension = uri.toString().substring(i);
                // If the extension is not one of the ones created by the daemons, throw the exception.
                if (!ERR_LOG_EXTENSION.equalsIgnoreCase(extension) && !OUT_LOG_EXTENSION.equalsIgnoreCase(extension)) {
                    throw new StorageEngineException("Unable to execute storage operation. Outdir '" + outdir + "' must be empty!");
                }
            }
        }
    }

    private boolean isCatalogPathDefined(ObjectMap options) {
        return options != null && StringUtils.isNotEmpty(options.getString(CATALOG_PATH));
    }

    protected Long getCatalogOutdirId(long studyId, ObjectMap options, String sessionId) throws CatalogException {
        return getCatalogOutdirId(Long.toString(studyId), options, sessionId);
    }

    protected Long getCatalogOutdirId(String studyStr, ObjectMap options, String sessionId) throws CatalogException {
        Long catalogOutDirId;
        if (isCatalogPathDefined(options)) {
            String catalogOutDirIdStr = options.getString(CATALOG_PATH);
            catalogOutDirId = catalogManager.getFileManager().getId(catalogOutDirIdStr, studyStr, sessionId).getResourceId();
            if (catalogOutDirId <= 0) {
                throw new CatalogException("Output directory " + catalogOutDirIdStr + " could not be found within catalog.");
            }
        } else {
            catalogOutDirId = null;
        }
        return catalogOutDirId;
    }

    public StudyConfiguration updateStudyConfiguration(String sessionId, long studyId, DataStore dataStore)
            throws IOException, CatalogException, StorageEngineException {

        CatalogStudyConfigurationFactory studyConfigurationFactory = new CatalogStudyConfigurationFactory(catalogManager);
        StudyConfigurationManager studyConfigurationManager = getVariantStorageEngine(dataStore).getStudyConfigurationManager();
        try {
            // Update StudyConfiguration. Add new elements and so
            studyConfigurationFactory.updateStudyConfigurationFromCatalog(studyId, studyConfigurationManager, sessionId);
            StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration((int) studyId, null).first();
            // Update Catalog file and cohort status.
            studyConfigurationFactory.updateCatalogFromStudyConfiguration(studyConfiguration, null, sessionId);
            return studyConfiguration;
        } catch (StorageEngineException e) {
            throw new StorageEngineException("Unable to update StudyConfiguration", e);
        }
    }

    protected Thread buildHook(Path outdir) {
        return buildHook(outdir, null);
    }

    protected Thread buildHook(Path outdir, Runnable onError) {
        return new Thread(() -> {
                try {
                    // If the status has not been changed by the method and is still running, we assume that the execution failed.
                    Job.JobStatus status = readJobStatus(outdir);
                    if (status.getName().equalsIgnoreCase(Job.JobStatus.RUNNING)) {
                        writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job finished with an error."));
                        if (onError != null) {
                            onError.run();
                        }
                    }
                } catch (IOException e) {
                    logger.error("Error modifying " + AbstractExecutor.JOB_STATUS_FILE, e);
                }
            });
    }

    protected List<File> copyResults(Path tmpOutdirPath, long catalogPathOutDir, String sessionId) throws CatalogException, IOException {
        File outDir = catalogManager.getFile(catalogPathOutDir, new QueryOptions(), sessionId).first();

        FileScanner fileScanner = new FileScanner(catalogManager);
//        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(tmpOutdirPath.toUri());

        List<File> files;
        try {
            logger.info("Scanning files from {} to move to {}", tmpOutdirPath, outDir.getUri());
            // Avoid copy the job.status file!
            Predicate<URI> fileStatusFilter = uri -> !uri.getPath().endsWith(JOB_STATUS_FILE)
                    && !uri.getPath().endsWith(OUT_LOG_EXTENSION)
                    && !uri.getPath().endsWith(ERR_LOG_EXTENSION);
            files = fileScanner.scan(outDir, tmpOutdirPath.toUri(), FileScanner.FileScannerPolicy.DELETE, true, false, fileStatusFilter, -1,
                    sessionId);

            // TODO: Check whether we want to store the logs as well. At this point, we are also storing them.
            // Do not execute checksum for log files! They may not be closed yet
            fileStatusFilter = uri -> uri.getPath().endsWith(OUT_LOG_EXTENSION) || uri.getPath().endsWith(ERR_LOG_EXTENSION);
            files.addAll(fileScanner.scan(outDir, tmpOutdirPath.toUri(), FileScanner.FileScannerPolicy.DELETE, false, false,
                    fileStatusFilter, -1, sessionId));

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
        return getDataStore(catalogManager, study, bioformat, sessionId);
    }

    public static DataStore getDataStore(CatalogManager catalogManager, Study study, File.Bioformat bioformat, String sessionId)
            throws CatalogException {
        DataStore dataStore;
        if (study.getDataStores() != null && study.getDataStores().containsKey(bioformat)) {
            dataStore = study.getDataStores().get(bioformat);
        } else {
            long projectId = catalogManager.getStudyManager().getProjectId(study.getId());
            dataStore = getDataStoreByProjectId(catalogManager, projectId, bioformat, sessionId);
        }
        return dataStore;
    }

    protected static DataStore getDataStoreByProjectId(CatalogManager catalogManager, long projectId, File.Bioformat bioformat,
                                                       String sessionId)
            throws CatalogException {
        DataStore dataStore;
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(ProjectDBAdaptor.QueryParams.ALIAS.key(), ProjectDBAdaptor.QueryParams.DATASTORES.key()));
        Project project = catalogManager.getProjectManager().get(projectId, queryOptions, sessionId).first();
        if (project.getDataStores() != null && project.getDataStores().containsKey(bioformat)) {
            dataStore = project.getDataStores().get(bioformat);
        } else { //get default datastore
            //Must use the UserByStudyId instead of the file owner.
            String userId = catalogManager.getProjectManager().getUserId(projectId);
            // Replace possible dots at the userId. Usually a special character in almost all databases. See #532
            userId = userId.replace('.', '_');

            String databasePrefix = catalogManager.getConfiguration().getDatabasePrefix();

            String dbName = buildDatabaseName(databasePrefix, userId, project.getAlias());
            dataStore = new DataStore(StorageEngineFactory.get().getDefaultStorageManagerName(), dbName);
        }
        return dataStore;
    }

    protected VariantStorageEngine getVariantStorageEngine(DataStore dataStore) throws StorageEngineException {
        VariantStorageEngine variantStorageEngine;
        try {
            variantStorageEngine = storageEngineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new StorageEngineException("Unable to create StorageEngine", e);
        }
        return variantStorageEngine;
    }

    public static String buildDatabaseName(String databasePrefix, String userId, String alias) {
        String prefix;
        if (StringUtils.isNotEmpty(databasePrefix)) {
            prefix = databasePrefix;
            if (!prefix.endsWith("_")) {
                prefix += "_";
            }
        } else {
            prefix = "opencga_";
        }
        // Project alias contains the userId:
        // userId@projectAlias
        int idx = alias.indexOf('@');
        if (idx >= 0) {
            alias = alias.substring(idx + 1);
        }

        return prefix + userId + '_' + alias;
    }
}
