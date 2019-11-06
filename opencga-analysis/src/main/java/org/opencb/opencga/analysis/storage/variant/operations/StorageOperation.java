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

package org.opencb.opencga.analysis.storage.variant.operations;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.storage.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.monitor.executors.BatchExecutor;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;

import static org.opencb.opencga.catalog.monitor.executors.BatchExecutor.*;

/**
 * Created by pfurio on 23/08/16.
 */
public abstract class StorageOperation {

    @Deprecated
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

    protected String getCatalogOutdirId(String studyStr, ObjectMap options, String sessionId) throws CatalogException {
        String catalogOutDirId;
        if (isCatalogPathDefined(options)) {
            String catalogOutDirIdStr = options.getString(CATALOG_PATH);
            catalogOutDirId = catalogManager.getFileManager().get(studyStr, catalogOutDirIdStr, FileManager.INCLUDE_FILE_IDS, sessionId)
                    .first().getId();
        } else {
            catalogOutDirId = null;
        }
        return catalogOutDirId;
    }

    public StudyMetadata synchronizeCatalogStudyFromStorage(DataStore dataStore, String study, String sessionId)
            throws CatalogException, StorageEngineException {
        VariantStorageMetadataManager metadataManager = getVariantStorageEngine(dataStore).getMetadataManager();
        CatalogStorageMetadataSynchronizer studyConfigurationFactory
                = new CatalogStorageMetadataSynchronizer(catalogManager, metadataManager);

        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        if (studyMetadata != null) {
            // Update Catalog file and cohort status.
            studyConfigurationFactory.synchronizeCatalogStudyFromStorage(studyMetadata, sessionId);
        }
        return studyMetadata;
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
                    logger.error("Error modifying " + BatchExecutor.JOB_STATUS_FILE, e);
                }
            });
    }

    @Deprecated
    protected List<File> copyResults(Path tmpOutdirPath, String study, String catalogPathOutDir, String sessionId)
            throws CatalogException, IOException {
        File outDir = catalogManager.getFileManager().get(study, catalogPathOutDir, new QueryOptions(), sessionId).first();

        FileScanner fileScanner = new FileScanner(catalogManager);
//        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(tmpOutdirPath.toUri());

        List<File> files;
        try {
            logger.info("Scanning files from {} to move to {}", tmpOutdirPath, outDir.getUri());
            // Avoid copy the job.status file!
            Predicate<URI> fileStatusFilter = uri -> !uri.getPath().endsWith(JOB_STATUS_FILE)
                    && !uri.getPath().endsWith(OUT_LOG_EXTENSION)
                    && !uri.getPath().endsWith(ERR_LOG_EXTENSION);
            files = fileScanner.scan(outDir, tmpOutdirPath.toUri(), FileScanner.FileScannerPolicy.DELETE, false, true, fileStatusFilter,
                    sessionId);

            // TODO: Check whether we want to store the logs as well. At this point, we are also storing them.
            // Do not execute checksum for log files! They may not be closed yet
            fileStatusFilter = uri -> uri.getPath().endsWith(OUT_LOG_EXTENSION) || uri.getPath().endsWith(ERR_LOG_EXTENSION);
            files.addAll(fileScanner.scan(outDir, tmpOutdirPath.toUri(), FileScanner.FileScannerPolicy.DELETE, false, false,
                    fileStatusFilter, sessionId));

        } catch (IOException e) {
            logger.warn("IOException when scanning temporal directory. Error: {}", e.getMessage());
            throw e;
        } catch (CatalogException e) {
            logger.warn("CatalogException when scanning temporal directory. Error: {}", e.getMessage());
            throw e;
        }
        return files;
    }

    @Deprecated
    public Job.JobStatus readJobStatus(Path outdir) throws IOException {
        return objectMapper.readerFor(Job.JobStatus.class).readValue(outdir.resolve(JOB_STATUS_FILE).toFile());
    }

    @Deprecated
    public void writeJobStatus(Path outdir, Job.JobStatus jobStatus) throws IOException {
        objectMapper.writer().writeValue(outdir.resolve(JOB_STATUS_FILE).toFile(), jobStatus);
    }

    protected VariantStorageEngine getVariantStorageEngine(DataStore dataStore) throws StorageEngineException {
        return storageEngineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
    }

    public static boolean isVcfFormat(File file) {
        File.Format format = file.getFormat();
        if (isVcfFormat(format)) {
            return true;
        } else {
            // Do not trust the file format. Defect format from URI
            format = org.opencb.opencga.catalog.managers.FileUtils.detectFormat(file.getUri());
            if (isVcfFormat(format)) {
                // Overwrite temporary the format
                file.setFormat(format);
                return true;
            } else {
                return false;
            }
        }
    }

    private static boolean isVcfFormat(File.Format format) {
        return format.equals(File.Format.VCF) || format.equals(File.Format.GVCF) || format.equals(File.Format.BCF);
    }

}
