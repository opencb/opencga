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

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.analysis.files.FileScanner;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Index;
import org.opencb.opencga.catalog.models.Job;
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

    public AnalysisOutputRecorder(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    public void recordJobOutputAndPostProcess(Job job, boolean jobFailed) {
        recordJobOutput(job);

        /** Modifies the job to set the output and endTime. **/
        try {
            postProcessJob(job, jobFailed);
        } catch (CatalogException e) {
            e.printStackTrace(); //TODO: Handle exception
        }
    }
    public void recordJobOutput(Job job) {

        try {
            /** Scans the output directory from a job or index to find all files. **/
            URI tmpOutDirUri = job.getTmpOutDirUri();
            logger.debug("Scan the temporal output directory ({}) from a job to find all generated files.", tmpOutDirUri);
            File outDir = catalogManager.getFile(job.getOutDirId(), new QueryOptions("path", true), sessionId).getResult().get(0);

            FileScanner fileScanner = new FileScanner(catalogManager);
            List<File> files = fileScanner.scan(outDir, tmpOutDirUri, fileScannerPolicy, calculateChecksum, true, job.getId(), sessionId);
            List<Integer> fileIds = files.stream().map(File::getId).collect(Collectors.toList());
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
        } catch (CatalogException | IOException e) {
            e.printStackTrace();
            logger.error("Error while processing Job", e);
        }
    }

    public void postProcessJob(Job job, boolean jobFailed) throws CatalogException {
        String type = job.getAttributes().containsKey(Job.TYPE) ?
                job.getAttributes().get(Job.TYPE).toString() : Job.Type.ANALYSIS.toString();
        switch(Job.Type.valueOf(type)) {
            case INDEX:
                Integer indexedFileId = (Integer) job.getAttributes().get(Job.INDEXED_FILE_ID);
                File indexedFile = catalogManager.getFile(indexedFileId, sessionId).first();
                final Index index;
                if (indexedFile.getIndex() != null) {
                    index = indexedFile.getIndex();
                    switch (index.getStatus()) {
                        case NONE:
                        case TRANSFORMED:
                            logger.warn("Unexpected index status. Expected "
                                    + Index.Status.TRANSFORMING + ", "
                                    + Index.Status.LOADING + " or "
                                    + Index.Status.INDEXING
                                    + " and got " + index.getStatus());
                        case READY: //Do not show warn message when index status is READY.
                            break;
                        case TRANSFORMING:
                            if (jobFailed) {
                                logger.warn("Job failed. Restoring status from " +
                                        Index.Status.TRANSFORMING + " to " + Index.Status.NONE);
                                index.setStatus(Index.Status.NONE);
                            } else {
                                index.setStatus(Index.Status.TRANSFORMED);
                                FileMetadataReader.get(catalogManager).updateVariantFileStats(job, sessionId);
                            }
                            break;
                        case LOADING:
                            if (jobFailed) {
                                logger.warn("Job failed. Restoring status from " +
                                        Index.Status.LOADING + " to " + Index.Status.TRANSFORMED);
                                index.setStatus(Index.Status.TRANSFORMED);
                            } else {
                                index.setStatus(Index.Status.READY);
                            }
                            break;
                        case INDEXING:
                            if (jobFailed) {
                                logger.warn("Job failed. Restoring status from " +
                                        Index.Status.INDEXING + " to " + Index.Status.NONE);
                                index.setStatus(Index.Status.NONE);
                            } else {
                                index.setStatus(Index.Status.READY);
                                FileMetadataReader.get(catalogManager).updateVariantFileStats(job, sessionId);
                            }
                            break;
                    }
                } else {
                    index = new Index(job.getUserId(), job.getDate(), Index.Status.READY, job.getId(), new HashMap<>());
                    logger.warn("Expected INDEX object on the indexed file " +
                            "{ id:" + indexedFile.getId() + ", path:\"" + indexedFile.getPath() + "\"}");
                }
                catalogManager.modifyFile(indexedFileId, new ObjectMap("index", index), sessionId); //Modify status
                if (index.getStatus().equals(Index.Status.READY) && Boolean.parseBoolean(job.getAttributes().getOrDefault(VariantStorageManager.Options.CALCULATE_STATS.key(), VariantStorageManager.Options.CALCULATE_STATS.defaultValue()).toString())) {
                    QueryResult<Cohort> queryResult = catalogManager.getAllCohorts(catalogManager.getStudyIdByJobId(job.getId()), new QueryOptions(CatalogSampleDBAdaptor.CohortFilterOption.name.toString(), StudyEntry.DEFAULT_COHORT), sessionId);
                    if (queryResult.getNumResults() != 0) {
                        logger.debug("Default cohort status set to READY");
                        Cohort defaultCohort = queryResult.first();
                        catalogManager.modifyCohort(defaultCohort.getId(), new ObjectMap("status", Cohort.Status.READY), sessionId);
                    }
                }
                break;
            case COHORT_STATS:
                List<Integer> cohortIds = new ObjectMap(job.getAttributes()).getAsIntegerList("cohortIds");
                ObjectMap updateParams = new ObjectMap("status", jobFailed? Cohort.Status.INVALID : Cohort.Status.READY);
                for (Integer cohortId : cohortIds) {
                    catalogManager.modifyCohort(cohortId, updateParams, sessionId);
                }
                break;
            case ANALYSIS:
                break;
            default:
                break;
        }
    }

}
