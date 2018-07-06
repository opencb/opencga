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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.variant.metadata.CatalogStudyConfigurationFactory;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    protected String getCatalogOutdirId(String studyStr, ObjectMap options, String sessionId) throws CatalogException {
        String catalogOutDirId;
        if (isCatalogPathDefined(options)) {
            String catalogOutDirIdStr = options.getString(CATALOG_PATH);
            catalogOutDirId = catalogManager.getFileManager()
                    .getUid(catalogOutDirIdStr, studyStr, sessionId).getResource().getId();
        } else {
            catalogOutDirId = null;
        }
        return catalogOutDirId;
    }

    public StudyConfiguration updateCatalogFromStudyConfiguration(String sessionId, String study, DataStore dataStore)
            throws IOException, CatalogException, StorageEngineException {

        CatalogStudyConfigurationFactory studyConfigurationFactory = new CatalogStudyConfigurationFactory(catalogManager);
        StudyConfigurationManager studyConfigurationManager = getVariantStorageEngine(dataStore).getStudyConfigurationManager();

        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(study, null).first();
        if (studyConfiguration != null) {
            // Update Catalog file and cohort status.
            studyConfigurationFactory.updateCatalogFromStudyConfiguration(studyConfiguration, sessionId);
        }
        return studyConfiguration;
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
        return objectMapper.readerFor(Job.JobStatus.class).readValue(outdir.resolve(JOB_STATUS_FILE).toFile());
    }

    public void writeJobStatus(Path outdir, Job.JobStatus jobStatus) throws IOException {
        objectMapper.writer().writeValue(outdir.resolve(JOB_STATUS_FILE).toFile(), jobStatus);
    }

    public static DataStore getDataStore(CatalogManager catalogManager, String studyStr, File.Bioformat bioformat, String sessionId)
            throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyStr, new QueryOptions(), sessionId).first();
        return getDataStore(catalogManager, study, bioformat, sessionId);
    }

    private static DataStore getDataStore(CatalogManager catalogManager, Study study, File.Bioformat bioformat, String sessionId)
            throws CatalogException {
        DataStore dataStore;
        if (study.getDataStores() != null && study.getDataStores().containsKey(bioformat)) {
            dataStore = study.getDataStores().get(bioformat);
        } else {
            String projectId = catalogManager.getStudyManager().getProjectFqn(study.getFqn());
            dataStore = getDataStoreByProjectId(catalogManager, projectId, bioformat, sessionId);
        }
        return dataStore;
    }

    public static DataStore getDataStoreByProjectId(CatalogManager catalogManager, String projectStr, File.Bioformat bioformat,
                                                    String sessionId)
            throws CatalogException {
        DataStore dataStore;
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(ProjectDBAdaptor.QueryParams.ID.key(), ProjectDBAdaptor.QueryParams.DATASTORES.key()));
        Project project = catalogManager.getProjectManager().get(projectStr, queryOptions, sessionId).first();
        if (project.getDataStores() != null && project.getDataStores().containsKey(bioformat)) {
            dataStore = project.getDataStores().get(bioformat);
        } else { //get default datastore
            //Must use the UserByStudyId instead of the file owner.
            String userId = catalogManager.getProjectManager().getOwner(project.getUid());
            // Replace possible dots at the userId. Usually a special character in almost all databases. See #532
            userId = userId.replace('.', '_');

            String databasePrefix = catalogManager.getConfiguration().getDatabasePrefix();

            String dbName = buildDatabaseName(databasePrefix, userId, project.getId());
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

    public static void updateProjectMetadata(CatalogManager catalog, StudyConfigurationManager scm, String project, String sessionId)
            throws CatalogException, StorageEngineException {
        final Project p = catalog.getProjectManager().get(project,
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        ProjectDBAdaptor.QueryParams.ORGANISM.key(), ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key())),
                sessionId)
                .first();

        StorageOperation.updateProjectMetadata(scm, p.getOrganism(), p.getCurrentRelease());
    }

    public static void updateProjectMetadata(StudyConfigurationManager scm, Project.Organism organism, int release)
            throws CatalogException, StorageEngineException {
        String scientificName = AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName(organism.getScientificName());

        scm.lockAndUpdateProject(projectMetadata -> {
            if (projectMetadata == null) {
                projectMetadata = new ProjectMetadata();
            }
            projectMetadata.setSpecies(scientificName);
            projectMetadata.setAssembly(organism.getAssembly());
            projectMetadata.setRelease(release);
            return projectMetadata;
        });
    }


    /**
     * If the study is aggregated and a mapping file is provided, pass it to
     * and create in catalog the cohorts described in the mapping file.
     *
     * If the study aggregation was not defined, updateStudy with the provided aggregation type
     *
     * @param studyId   StudyId where calculate stats
     * @param options   Options
     * @param sessionId Users sessionId
     * @return          Effective study aggregation type
     * @throws CatalogException if something is wrong with catalog
     */
    public Aggregation getAggregation(String studyId, QueryOptions options, String sessionId) throws CatalogException {
        QueryOptions include = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.ATTRIBUTES.key());
        Study study = catalogManager.getStudyManager().get(studyId, include, sessionId).first();
        Aggregation argsAggregation = options.get(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), Aggregation.class, Aggregation.NONE);
        Object studyAggregationObj = study.getAttributes().get(VariantStorageEngine.Options.AGGREGATED_TYPE.key());
        Aggregation studyAggregation = null;
        if (studyAggregationObj != null) {
            studyAggregation = AggregationUtils.valueOf(studyAggregationObj.toString());
        }

        final Aggregation aggregation;
        if (AggregationUtils.isAggregated(argsAggregation)) {
            if (studyAggregation != null && !studyAggregation.equals(argsAggregation)) {
                // FIXME: Throw an exception?
                logger.warn("Calculating statistics with aggregation " + argsAggregation + " instead of " + studyAggregation);
            }
            aggregation = argsAggregation;
            // If studyAggregation is not define, update study aggregation
            if (studyAggregation == null) {
                //update study aggregation
                Map<String, Aggregation> attributes = Collections.singletonMap(VariantStorageEngine.Options.AGGREGATED_TYPE.key(),
                        argsAggregation);
                ObjectMap parameters = new ObjectMap("attributes", attributes);
                catalogManager.getStudyManager().update(studyId, parameters, null, sessionId);
            }
        } else {
            if (studyAggregation == null) {
                aggregation = Aggregation.NONE;
            } else {
                aggregation = studyAggregation;
            }
        }
        return aggregation;
    }


}
