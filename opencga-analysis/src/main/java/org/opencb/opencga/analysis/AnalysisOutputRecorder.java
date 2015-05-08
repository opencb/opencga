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

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.files.FileScanner;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Index;
import org.opencb.opencga.catalog.beans.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
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
    private final FileScanner.FileScannerPolicy delete = FileScanner.FileScannerPolicy.DELETE; //TODO: Read from config file

    public AnalysisOutputRecorder(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    public void recordJobOutput(Job job) {

        List<Integer> fileIds;

        try {
            /** Scans the output directory from a job or index to find all files. **/
            URI tmpOutDirUri = job.getTmpOutDirUri();
            logger.debug("Scan the temporal output directory ({}) from a job to find all generated files.", tmpOutDirUri);
            File outDir = catalogManager.getFile(job.getOutDirId(), new QueryOptions("path", true), sessionId).getResult().get(0);

            FileScanner fileScanner = new FileScanner(catalogManager, delete, calculateChecksum, true);
            List<File> files = fileScanner.scan(outDir, tmpOutDirUri, sessionId);
            fileIds = files.stream().map(File::getId).collect(Collectors.toList());
        } catch (CatalogException | IOException e) {
            e.printStackTrace();
            logger.error("Error while processing Job", e);
            return;
        }

        /** Modifies the job to set the output and endTime. **/
        try {
            switch(Job.Type.valueOf(job.getAttributes().get(Job.TYPE).toString())) {
                case INDEX:
                    Integer indexedFileId = (Integer) job.getAttributes().get(Job.INDEXED_FILE_ID);
                    File indexedFile = catalogManager.getFile(indexedFileId, sessionId).first();
                    if (indexedFile.getIndex() != null) {
                        Index index = indexedFile.getIndex();
                        index.setStatus(Index.Status.READY);
                        catalogManager.modifyFile(indexedFileId, new ObjectMap("index", index), sessionId); //Modify status
                    }
                    break;
                case ANALYSIS:
                default:
                    break;
            }
            ObjectMap parameters = new ObjectMap();
            parameters.put("output", fileIds);
            parameters.put("endTime", System.currentTimeMillis());
            catalogManager.modifyJob(job.getId(), parameters, sessionId);

            //TODO: "input" files could be modified by the tool. Have to be scanned, calculate the new Checksum and

        } catch (CatalogException e) {
            e.printStackTrace(); //TODO: Handle exception
        }
    }

}
