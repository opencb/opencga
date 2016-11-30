/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.local.variant.operations;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.local.variant.CatalogStudyConfigurationFactory;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
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

    protected final CatalogManager catalogManager;
    protected final StorageManagerFactory storageManagerFactory;
    protected final Logger logger;
    private ObjectMapper objectMapper = new ObjectMapper();

    public StorageOperation(CatalogManager catalogManager, StorageManagerFactory storageManagerFactory, Logger logger) {
        this.catalogManager = catalogManager;
        this.storageManagerFactory = storageManagerFactory;
        this.logger = logger;
    }

    protected void outdirMustBeEmpty(Path outdir) throws CatalogIOException, StorageManagerException {
        List<URI> uris = catalogManager.getCatalogIOManagerFactory().get(outdir.toUri()).listFiles(outdir.toUri());
        if (!uris.isEmpty()) {
            // Only allow stdout and stderr files
            for (URI uri : uris) {
                // Obtain the extension
                int i = uri.toString().lastIndexOf(".");
                if (i <= 0) {
                    throw new StorageManagerException("Unable to execute index. Outdir '" + outdir + "' must be empty!");
                }
                String extension = uri.toString().substring(i);
                // If the extension is not one of the ones created by the daemons, throw the exception.
                if (!ERR_LOG_EXTENSION.equalsIgnoreCase(extension) && !OUT_LOG_EXTENSION.equalsIgnoreCase(extension)) {
                    throw new StorageManagerException("Unable to execute index. Outdir '" + outdir + "' must be empty!");
                }
            }
        }
    }

    protected Long getCatalogOutdirId(long studyId, String catalogOutDirIdStr, String sessionId) throws CatalogException {
        Long catalogOutDirId;
        if (catalogOutDirIdStr != null) {
            catalogOutDirId = catalogManager.getFileManager().getId(catalogOutDirIdStr, studyId, sessionId);
            if (catalogOutDirId <= 0) {
                throw new CatalogException("Output directory " + catalogOutDirIdStr + " could not be found within catalog.");
            }
        } else {
            catalogOutDirId = null;
        }
        return catalogOutDirId;
    }

    public StudyConfiguration updateStudyConfiguration(String sessionId, long studyId, DataStore dataStore)
            throws IOException, CatalogException, StorageManagerException {

        CatalogStudyConfigurationFactory studyConfigurationFactory = new CatalogStudyConfigurationFactory(catalogManager);
        try (VariantDBAdaptor dbAdaptor = StorageManagerFactory.get().getVariantStorageManager(dataStore.getStorageEngine())
                .getDBAdaptor(dataStore.getDbName());
             StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager()) {

            // Update StudyConfiguration. Add new elements and so
            studyConfigurationFactory.updateStudyConfigurationFromCatalog(studyId, studyConfigurationManager, sessionId);
            StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration((int) studyId, null).first();
            // Update Catalog file and cohort status.
            studyConfigurationFactory.updateCatalogFromStudyConfiguration(studyConfiguration, null, sessionId);
            return studyConfiguration;
        } catch (StorageManagerException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new StorageManagerException("Unable to update StudyConfiguration", e);
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
            Predicate<URI> fileStatusFilter = uri -> !uri.getPath().endsWith(JOB_STATUS_FILE);
            // TODO: Check whether we want to store the logs as well. At this point, we are also storing them.
//            Predicate<URI> fileStatusFilter = uri -> (!uri.getPath().endsWith(JOB_STATUS_FILE)
//                    && !uri.getPath().endsWith(OUT_LOG_EXTENSION)
//                    && !uri.getPath().endsWith(ERR_LOG_EXTENSION));
            files = fileScanner.scan(outDir, tmpOutdirPath.toUri(), FileScanner.FileScannerPolicy.DELETE, true, false, fileStatusFilter, -1,
                    sessionId);
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
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(ProjectDBAdaptor.QueryParams.ALIAS.key(), ProjectDBAdaptor.QueryParams.DATASTORES.key())
            );
            Project project = catalogManager.getProjectManager().get(projectId, queryOptions, sessionId).first();
            if (project != null && project.getDataStores() != null && project.getDataStores().containsKey(bioformat)) {
                dataStore = project.getDataStores().get(bioformat);
            } else { //get default datastore
                //Must use the UserByStudyId instead of the file owner.
                String userId = catalogManager.getStudyManager().getUserId(study.getId());
                String alias = project.getAlias();

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
