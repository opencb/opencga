/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.analysis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.analysis.files.FileScanner;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Index;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 4/11/14.
 *
 *  Scans the temporal output directory from a job to find all generated files.
 *  Modifies the job status to set the output and endTime.
 *  If the job was type:INDEX, modify the index status.
 */

public class AnalysisOutputRecorder {


    private static Logger logger = LoggerFactory.getLogger(AnalysisOutputRecorder.class);
    private final CatalogManager catalogManager;
    private final String sessionId;
    private final boolean calculateChecksum = false;    //TODO: Read from config file
    private final FileScanner.FileScannerPolicy fileScannerPolicy = FileScanner.FileScannerPolicy.DELETE; //TODO: Read from config file
    private final ObjectMapper objectMapper = new ObjectMapper();

    public AnalysisOutputRecorder(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    public void recordJobOutputAndPostProcess(Job job, boolean jobFailed) throws CatalogException {
        /** Modifies the job to set the output and endTime. **/

        recordJobOutput(job);

        postProcessJob(job, jobFailed);
    }

    /**
     * Scans the temporal output folder for the job and adds all the output files to catalog
     *
     * @param job
     */
    public void recordJobOutput(Job job) throws CatalogException {

        try {
            /** Scans the output directory from a job or index to find all files. **/
            URI tmpOutDirUri = job.getTmpOutDirUri();
            logger.debug("Scan the temporal output directory ({}) from a job to find all generated files.", tmpOutDirUri);
            File outDir = catalogManager.getFile(job.getOutDirId(), new QueryOptions("path", true), sessionId).getResult().get(0);

            FileScanner fileScanner = new FileScanner(catalogManager);
            List<File> files = fileScanner.scan(outDir, tmpOutDirUri, fileScannerPolicy, calculateChecksum, true, job.getId(), sessionId);
            List<Long> fileIds = files.stream().map(File::getId).collect(Collectors.toList());
            CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(tmpOutDirUri);
            if (!ioManager.exists(tmpOutDirUri)) {
                logger.warn("Output folder doesn't exist");
                return;
            }
            List<URI> uriList = ioManager.listFiles(tmpOutDirUri);
            if (uriList.isEmpty()) {
                ioManager.deleteDirectory(tmpOutDirUri);
            } else {
                logger.error("Error processing job output. Temporal job out dir is not empty. " + uriList);
            }


            ObjectMap parameters = new ObjectMap();
            parameters.put("output", fileIds);
            parameters.put("endTime", System.currentTimeMillis());
            catalogManager.modifyJob(job.getId(), parameters, sessionId);
            job.setOutput(fileIds);
            job.setEndTime(parameters.getLong("endTime"));

            //TODO: "input" files could be modified by the tool. Have to be scanned, calculate the new Checksum and
        } catch (IOException e) {
            logger.error("Error while processing Job", e);
            throw new CatalogException(e);
        }
    }

    public void postProcessJob(Job job, boolean jobFailed) throws CatalogException {
        String type = job.getAttributes().containsKey(Job.TYPE) ?
                job.getAttributes().get(Job.TYPE).toString() : Job.Type.ANALYSIS.toString();
        switch(Job.Type.valueOf(type)) {
            case INDEX:
                final StorageETLResult storageETLResult = readStorageETLResult(job.getId());
                postProcessIndexJob(job, storageETLResult, null, sessionId);
                break;
            case COHORT_STATS:
                List<Integer> cohortIds = new ObjectMap(job.getAttributes()).getAsIntegerList("cohortIds");
                ObjectMap updateParams = new ObjectMap(CatalogCohortDBAdaptor.QueryParams.STATUS_STATUS.key(), jobFailed? Cohort.CohortStatus.INVALID : Cohort.CohortStatus.READY);
                for (Integer cohortId : cohortIds) {
                    catalogManager.modifyCohort(cohortId, updateParams, new QueryOptions(), sessionId);
                }
                break;
            case ANALYSIS:
                break;
            default:
                break;
        }
    }

    public void saveStorageResult(Job job, StorageETLResult storageETLResult) throws CatalogException {
        if (storageETLResult != null) {
            catalogManager.modifyJob(job.getId(), new ObjectMap("attributes", new ObjectMap("storageETLResult", storageETLResult)), sessionId);
        }
    }

    public StorageETLResult readStorageETLResult(long jobId) throws CatalogException {
        Object object = catalogManager.getJob(jobId, null, sessionId).first().getAttributes().get("storageETLResult");
        final StorageETLResult storageETLResult;
        try {
            if (object != null) {
                storageETLResult = objectMapper.readValue(objectMapper.writeValueAsString(object), StorageETLResult.class);
            } else {
                storageETLResult = null;
            }
        } catch (IOException e) {
            throw new CatalogException(e);
        }
        return storageETLResult;
    }

    public void postProcessIndexJob(Job job, StorageETLResult storageETLResult, Exception e, String sessionId) throws CatalogException {
        boolean jobFailed = storageETLResult == null || storageETLResult.getLoadError() != null || storageETLResult.getTransformError() != null;

        Integer indexedFileId = (Integer) job.getAttributes().get(Job.INDEXED_FILE_ID);
        File indexedFile = catalogManager.getFile(indexedFileId, sessionId).first();
        final Index index;

        boolean transformedSuccess = storageETLResult != null && storageETLResult.isTransformExecuted() && storageETLResult.getTransformError() == null;
        boolean loadedSuccess = storageETLResult != null && storageETLResult.isLoadExecuted() && storageETLResult.getLoadError() == null;

        if (indexedFile.getIndex() != null) {
            index = indexedFile.getIndex();
            switch (index.getStatus().getStatus()) {
                case Index.IndexStatus.NONE:
                case Index.IndexStatus.TRANSFORMED:
                    logger.warn("Unexpected index status. Expected "
                            + Index.IndexStatus.TRANSFORMING + ", "
                            + Index.IndexStatus.LOADING + " or "
                            + Index.IndexStatus.INDEXING
                            + " and got " + index.getStatus());
                case Index.IndexStatus.READY: //Do not show warn message when index status is READY.
                    break;
                case Index.IndexStatus.TRANSFORMING:
                    if (jobFailed) {
                        logger.warn("Job failed. Restoring status from " +
                                Index.IndexStatus.TRANSFORMING + " to " + Index.IndexStatus.NONE);
                        index.getStatus().setStatus(Index.IndexStatus.NONE);
                    } else {
                        index.getStatus().setStatus(Index.IndexStatus.TRANSFORMED);
                    }
                    break;
                case Index.IndexStatus.LOADING:
                    if (jobFailed) {
                        logger.warn("Job failed. Restoring status from " +
                                Index.IndexStatus.LOADING + " to " + Index.IndexStatus.TRANSFORMED);
                        index.getStatus().setStatus(Index.IndexStatus.TRANSFORMED);
                    } else {
                        index.getStatus().setStatus(Index.IndexStatus.READY);
                    }
                    break;
                case Index.IndexStatus.INDEXING:
                    if (jobFailed) {
                        String newStatus;
                        // If transform was executed, restore status to Transformed.
                        if (transformedSuccess) {
                            newStatus = Index.IndexStatus.TRANSFORMED;
                        } else {
                            newStatus = Index.IndexStatus.NONE;
                        }
                        logger.warn("Job failed. Restoring status from " +
                                Index.IndexStatus.INDEXING + " to " + newStatus);
                        index.getStatus().setStatus(newStatus);
                    } else {
                        index.getStatus().setStatus(Index.IndexStatus.READY);
                    }
                    break;
            }
        } else {
            index = new Index(job.getUserId(), job.getDate(), new Index.IndexStatus(Index.IndexStatus.READY), job.getId(),
                    new HashMap<>());
            logger.warn("Expected INDEX object on the indexed file " +
                    "{ id:" + indexedFile.getId() + ", path:\"" + indexedFile.getPath() + "\"}");
        }

        if (transformedSuccess) {
            FileMetadataReader.get(catalogManager).updateVariantFileStats(job, sessionId);
        }

        catalogManager.modifyFile(indexedFileId, new ObjectMap("index", index), sessionId); //Modify status
        boolean calculateStats = Boolean.parseBoolean(job.getAttributes().getOrDefault(VariantStorageManager.Options.CALCULATE_STATS.key(),
                VariantStorageManager.Options.CALCULATE_STATS.defaultValue()).toString());

        if (index.getStatus().getStatus().equals(Index.IndexStatus.READY) && calculateStats) {
            QueryResult<Cohort> queryResult = catalogManager.getAllCohorts(catalogManager.getStudyIdByJobId(job.getId()),
                    new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), StudyEntry.DEFAULT_COHORT), new QueryOptions(), sessionId);
            if (queryResult.getNumResults() != 0) {
                logger.debug("Default cohort status set to READY");
                Cohort defaultCohort = queryResult.first();
                catalogManager.modifyCohort(defaultCohort.getId(),
                        new ObjectMap(CatalogCohortDBAdaptor.QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.READY),
                        new QueryOptions(), sessionId);
            }
        }
    }

}
