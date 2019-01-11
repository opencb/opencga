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

package org.opencb.opencga.analysis.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.old.AnalysisExecutionException;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.catalog.utils.CatalogSampleAnnotationsLoader;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pfurio on 08/06/16.
 */
public class AnalysisDemo {

    public AnalysisDemo() {
    }

    public static void insertPedigreeFile(CatalogManager catalogManager, Path inputFile, String sessionId)
            throws CatalogException, FileNotFoundException {
        String path = "data/peds";
        URI sourceUri = inputFile.toUri();
        File file = catalogManager.getFileManager().upload("user1@default:study1", new FileInputStream(new java.io.File(sourceUri)),
                new File().setPath(Paths.get(path, inputFile.getFileName().toString()).toString()), false, true, sessionId).first();

        // Load samples using the pedigree file
        CatalogSampleAnnotationsLoader catalogSampleAnnotationsLoader = new CatalogSampleAnnotationsLoader(catalogManager);
        catalogSampleAnnotationsLoader.loadSampleAnnotations(file, null, sessionId);
    }
//
//    public static void insertVariantFile(CatalogManager catalogManager, long studyId, Path inputFile, String sessionId)
//            throws CatalogException, StorageEngineException, AnalysisExecutionException, JsonProcessingException {
//        String path = "data/vcfs";
//        URI sourceUri = inputFile.toUri();
//        File file = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.VCF, File.Bioformat
//                .VARIANT, Paths.get(path, inputFile.getFileName().toString()).toString(), null, "Description", null, 0, -1, null, (long)
//                -1, null, null, true, null, null, sessionId).first();
//        new FileUtils(catalogManager).upload(sourceUri, file, null, sessionId, false, false, false, false);
//        FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, new QueryOptions(), sessionId, false);
//
//
//        long inputFileId = file.getUid();
//
//        QueryResult<File> outdirResult = catalogManager.getFileManager().get(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams
//                .PATH.key(), "data/jobs/"), null, sessionId);
//        long outDirId;
//        if (outdirResult.getResult().isEmpty()) {
//            outDirId = catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("data/jobs/").toString(), null,
//                    true, null, QueryOptions.empty(), sessionId).first().getUid();
//        } else {
//            outDirId = outdirResult.first().getUid();
//        }
//
//        boolean doTransform = false;
//        boolean doLoad = false;
//        boolean annotate = false;
//        boolean calculateStats = false;
//        boolean queue = false;
//        String logLevel = "info";
//
////        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
////
////        List<String> extraParams = cliOptions.commonOptions.params.entrySet()
////                .stream()
////                .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
////                .collect(Collectors.toList());
//
////        QueryOptions options = new QueryOptions()
////                .append(ExecutorFactory.EXECUTE, !queue)
////                .append(AnalysisFileIndexer.TRANSFORM, doTransform)
////                .append(AnalysisFileIndexer.LOAD, doLoad)
////                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), calculateStats)
////                .append(VariantStorageEngine.Options.ANNOTATE.key(), annotate)
////                .append(AnalysisFileIndexer.LOG_LEVEL, logLevel);
//
////        QueryResult<Job> result = analysisFileIndexer.index(inputFileId, outDirId, sessionId, options);
//    }

}
