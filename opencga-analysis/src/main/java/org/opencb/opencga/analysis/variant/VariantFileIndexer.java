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

package org.opencb.opencga.analysis.variant;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.api.IFileManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by imedina on 17/08/16.
 */
public class VariantFileIndexer {

    private final CatalogConfiguration catalogConfiguration;
    private final CatalogManager catalogManager;
    private final IFileManager fileManager;

    protected Logger logger;

    public static final String TRANSFORM = "transform";
    public static final String LOAD = "load";

    private final String VCF_EXTENSION = ".vcf";
    private final String AVRO_EXTENSION = ".avro";

    public VariantFileIndexer(CatalogConfiguration catalogConfiguration) throws CatalogException {
        this.catalogConfiguration = catalogConfiguration;
        this.catalogManager = new CatalogManager(catalogConfiguration);
        this.fileManager = this.catalogManager.getFileManager();

        logger = LoggerFactory.getLogger(VariantFileIndexer.class);
    }

    @Deprecated
    public VariantFileIndexer(CatalogManager catalogManager) {
        this.catalogConfiguration = null;
        this.catalogManager = catalogManager;
        this.fileManager = this.catalogManager.getFileManager();

        logger = LoggerFactory.getLogger(VariantFileIndexer.class);
    }

    public void index(String fileId, String outdirString, String sessionId, QueryOptions options)
            throws CatalogException, AnalysisExecutionException, IOException, IllegalAccessException, InstantiationException,
            ClassNotFoundException, StorageManagerException {

        Path outdir = Paths.get(outdirString);
        FileUtils.checkDirectory(outdir, true);

        if (options == null) {
            options = new QueryOptions();
        }

        boolean transform = true;
        boolean load = true;
        // If any of them is defined and at least one is different from false then we set the values
        if (options.getBoolean(TRANSFORM, false) || options.getBoolean(LOAD, false)) {
            transform = options.getBoolean(TRANSFORM, false);
            load = options.getBoolean(LOAD, false);
        }

        long start = System.currentTimeMillis();

        // Query catalog for user data
        String userId = catalogManager.getUserManager().getUserId(sessionId);

        Long fileIdLong = fileManager.getFileId(userId, fileId);
        File inputFile = fileManager.read(fileIdLong, QueryOptions.empty(), sessionId).first();

        long studyIdByInputFileId = fileManager.getStudyId(inputFile.getId());
        Study study = catalogManager.getStudyManager().read(studyIdByInputFileId, QueryOptions.empty(), sessionId).getResult().get(0);


        // We read all input files from fileId. This can either be a single file and then we just use it,
        // or this can be a directory, in that case we use all VCF files in that directory or subdirectory
        List<File> inputFiles = new ArrayList<>();
        if (inputFile.getType() == File.Type.FILE) {
            inputFiles.add(inputFile);
        } else {
            if (inputFile.getType() == File.Type.DIRECTORY) {
                Query query = new Query(CatalogFileDBAdaptor.QueryParams.PATH.key(), "~^" + inputFile.getPath() + "*");
                query.append(CatalogFileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF, File.Format.GVCF, File.Format.AVRO));
                QueryResult<File> fileQueryResult = fileManager.readAll(studyIdByInputFileId, query, options, sessionId);
                inputFiles.addAll(fileQueryResult.getResult());
            } else {
                throw new CatalogException(String.format("Expected file type {} or {} instead of {}",
                        File.Type.FILE, File.Type.DIRECTORY, inputFile.getType()));
            }
        }
        logger.debug("Index - Number of files to be indexed: {}, list of files: {}", inputFiles.size(), inputFiles.toString());


        // We get the credentials of the Datastore to insert the variants
        DataStore dataStore = null;
        if (study != null && study.getDataStores() != null && study.getDataStores().containsKey(inputFile.getBioformat())) {
            dataStore = study.getDataStores().get(inputFile.getBioformat());
        } else {
            Long projectId = catalogManager.getStudyManager().getProjectId(studyIdByInputFileId);
            QueryResult<Project> project = catalogManager.getProjectManager().read(projectId, QueryOptions.empty(), sessionId);
            if (project != null && project.first().getDataStores() != null
                    && project.first().getDataStores().containsKey(inputFile.getBioformat())) {
                dataStore = project.first().getDataStores().get(inputFile.getBioformat());
            } else {
                // we need to load the default datastore
            }
        }

        // now we can get the VariantStorageManager we need
        VariantStorageManager variantStorageManager = StorageManagerFactory.get().getVariantStorageManager(dataStore.getStorageEngine());
//        VariantStorageETL variantStorageETL = variantStorageManager.newStorageETL(true);

        List<File> filesToIndex;
        if (!transform && load) {
            // Just load the file
            // First we need to get the origianl VCF files
            filesToIndex = filterLoadFiles(inputFiles, studyIdByInputFileId, sessionId);
        } else {
            // Index file
            filesToIndex = filterTransformFiles(inputFiles);
        }

        List<URI> fileUris = new ArrayList<>(filesToIndex.size());
        for (File file : filesToIndex) {
            fileUris.add(file.getUri());
        }

        List<StorageETLResult> storageETLResults = variantStorageManager.index(fileUris, outdir.toUri(), false, transform, load);

        File file;
        for (int i = 0; i < storageETLResults.size(); i++) {

            file = filesToIndex.get(i);
            StorageETLResult storageETLResult = storageETLResults.get(i);

            if (transform && !load) {
                // Check transform errors
                if (storageETLResult.isTransformExecuted() && storageETLResult.getTransformError() != null) {
                    fileManager.updateFileIndexStatus(file, FileIndex.IndexStatus.TRANSFORMED, sessionId);
                }

            } else {
                if (!transform && load) {
                    if (storageETLResult.isLoadExecuted() && storageETLResult.getLoadError() != null) {
                        fileManager.updateFileIndexStatus(file, FileIndex.IndexStatus.READY, sessionId);

                        // Update default cohort with new indexed samples
                    }
                } else {
                    if (storageETLResult.isTransformExecuted() && storageETLResult.getTransformError() != null) {
                        if (storageETLResult.isLoadExecuted() && storageETLResult.getLoadError() != null) {
                            fileManager.updateFileIndexStatus(file, FileIndex.IndexStatus.READY, sessionId);

                            // Update default cohort with new indexed samples
                        } else {
                            fileManager.updateFileIndexStatus(file, FileIndex.IndexStatus.TRANSFORMED, sessionId);
                        }

                    }

                }
            }
        }

    }

    private List<File> filterTransformFiles(List<File> fileList) throws CatalogException {
        if (fileList == null || fileList.isEmpty()) {
            return new ArrayList<>();
        }

        List<File> filteredFiles = new ArrayList<>(fileList.size());
        for (File file : fileList) {
            if (file.getStatus().getName().equals(File.FileStatus.READY)
                    && file.getFormat().equals(File.Format.VCF)) {
                if (file.getIndex() != null) {
                    if (file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.NONE)) {
                        filteredFiles.add(file);
                    } else {
                        logger.warn("We can only transform VCF files not transformed, the status is {}",
                                file.getIndex().getStatus().getName());
                    }
                } else {
                    // This block should not happen ever
                    filteredFiles.add(file);
                    logger.warn("This block should not happen ever");
                }
            } else {
                logger.warn("");
            }
        }
        return filteredFiles;
    }

    private List<File> filterLoadFiles(List<File> fileList, long studyId, String sessionId) throws CatalogException {
        if (fileList == null || fileList.isEmpty()) {
            return new ArrayList<>();
        }

        File vcf;
        File avro;
        List<File> filteredFiles = new ArrayList<>(fileList.size());
        for (File file : fileList) {

            // This should use relatedFile in the future
            if (file.getFormat().equals(File.Format.VCF) || file.getFormat().equals(File.Format.GVCF)) {
                vcf = file;
                avro = search(studyId, file, VCF_EXTENSION, AVRO_EXTENSION, sessionId);
            } else {
                avro = file;
                vcf = search(studyId, file, AVRO_EXTENSION, VCF_EXTENSION, sessionId);
            }

            if (vcf == null || avro == null) {
                logger.warn("");
                continue;
            }

            if (vcf.getStatus().getName().equals(File.FileStatus.READY) && avro.getStatus().getName().equals(File.FileStatus.READY)) {
                if (vcf.getIndex() != null) {
                    if (vcf.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.TRANSFORMED)) {
                        filteredFiles.add(avro);
                    } else {
                        logger.warn("We can only transform VCF files not transformed, the status is {}",
                                file.getIndex().getStatus().getName());
                    }
                } else {
                    // This block should not happen ever
                    filteredFiles.add(file);
                    logger.warn("This block should not happen ever");
                }
            } else {
                logger.warn("");
            }
        }
        return filteredFiles;
    }

    /**
     * Look for the related avro or vcf file in catalog using one of the pairs as a starting point
     *
     * @param studyId study id of the files.
     * @param sourceFile avro or vcf file used to look for the vcf or avro file respectively.
     * @param sourceExtension Extension of the file being passed.
     * @param destinyExtension Extension of the file being looked for.
     * @param sessionId session id of the user that should have permissions for the file being looked for.
     * @return the obtained file or null otherwise.
     */
    private File search(long studyId, File sourceFile, String sourceExtension, String destinyExtension, String sessionId)
            throws CatalogException {
        // Look for the destiny file in the same folder where the source file is located
        Path sourcePath = Paths.get(sourceFile.getPath());

        Path parent = sourcePath.getParent();
        String destinyFileName = sourcePath.getFileName().toString().replace(sourceExtension, destinyExtension);

        // TODO: Use relatedFiles to look for the files
        Query query = new Query()
                .append(CatalogFileDBAdaptor.QueryParams.NAME.key(), destinyFileName)
                .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult<File> fileQueryResult = fileManager.readAll(parent.toString(), true, query, QueryOptions.empty(), sessionId);
        if (fileQueryResult.getNumResults() == 1) {
            return fileQueryResult.first();
        }

        // Look for the file in the same path
        String destinyPath = parent.resolve(destinyFileName).toString();

        for (File file : fileQueryResult.getResult()) {
            if (destinyPath.equals(file.getPath())) {
                return file;
            }
        }

        logger.error("{} files have been found as possible pairs of {} with id {}", fileQueryResult.getNumResults(), sourceFile.getName(),
                sourceFile.getId());
        return null;
//
//        Query query = new Query(CatalogFileDBAdaptor.QueryParams.PATH.key(), destinyPath);
//        QueryResult<File> fileQueryResult;
//        try {
//            fileQueryResult = fileManager.readAll(studyId, query, QueryOptions.empty(), sessionId);
//            if (fileQueryResult.getNumResults() == 1) {
//                return fileQueryResult.first();
//            }
//        } catch (CatalogException e) {
//            logger.error("An error occurred while searching for the {} pair. {}", destinyExtension.replace(".", ""), e.getMessage());
//            return null;
//        }
//
//        // Look for the destiny file anywhere in the study
//        query = new Query(CatalogFileDBAdaptor.QueryParams.NAME.key(), destinyFileName);
//        try {
//            fileQueryResult = fileManager.readAll(studyId, query, QueryOptions.empty(), sessionId);
//            if (fileQueryResult.getNumResults() == 1) {
//                return fileQueryResult.first();
//            }
//        } catch (CatalogException e) {
//            logger.error("An error occurred while searching for the {} pair. {}", destinyExtension.replace(".", ""), e.getMessage());
//            return null;
//        }
//
//        logger.warn("{} {} files could be found under the name {}. None of them will be used.", fileQueryResult.getNumResults(),
//                destinyExtension.replace(".", ""), destinyFileName);
//        return null;
    }
}
