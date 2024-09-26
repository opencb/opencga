/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.URLUtils;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ResourceUtils {

    public static final String URL = "http://resources.opencb.org/opencb/opencga/";

    public static final String RESOURCES_TXT_FILENAME = "resources.txt";
    public static final String ANALYSIS_FOLDER_NAME = "analysis";
    private static final String RESOURCES_FOLDER_NAME = "resources";

    // Locking mechanism to prevent concurrent downloads for the same analysis
    private static final Map<String, Lock> analysisLocks = new ConcurrentHashMap<>();
    private static final long LOCK_TIMEOUT = 2; // Timeout of 2 hours

    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUtils.class);

    public static File downloadThirdParty(URL url, Path outDir) throws IOException {
        return URLUtils.download(url, outDir);
    }

    public static File downloadAnalysis(String analysisId, String resouceName, Path outDir, Path openCgaHome) throws IOException {
        Path path = null;
        String filename = ANALYSIS_FOLDER_NAME + "/" + analysisId + "/" + resouceName;
        if (openCgaHome != null) {
            path = openCgaHome.resolve(filename);
        }
        if (path != null && path.toFile().exists()) {
            File outFile = outDir.resolve(path.toFile().getName()).toFile();
            LOGGER.info("downloadAnalysis from path: " + path + " to " + outFile.getAbsolutePath());
            FileUtils.copyFile(path.toFile(), outFile);

            return outFile;
        } else {
            LOGGER.info("downloadAnalysis from URL: " + (URL + filename) + ", (path does not exist: " + path + ")");
            return URLUtils.download(new URL(URL + filename), outDir);
        }
    }

    public static List<File> getResourceFiles(String analysisId, Path openCgaHome) throws IOException {
        return getResourceFiles(URL, analysisId, openCgaHome);
    }

    public static List<File> getResourceFiles(String baseUrl, String analysisId, Path openCgaHome) throws IOException {
        List<File> downloadedFiles = new ArrayList<>();

        // Get resource filenames for the input analysis
        Path resourcesTxt = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(analysisId).resolve(RESOURCES_TXT_FILENAME);
        List<String> filenames = readAllLines(resourcesTxt);

        // Create a lock for the analysis if not present, and get it for the input analysis
        analysisLocks.putIfAbsent(analysisId, new ReentrantLock());
        Lock lock = analysisLocks.get(analysisId);

        boolean lockAcquired = false;
        try {
            // Try to acquire the lock within the specified timeout
            lockAcquired = lock.tryLock(LOCK_TIMEOUT, TimeUnit.HOURS);

            if (lockAcquired) {
                // Create the analysis resources directory if it doesn't exist
                Path analysisResourcesPath = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME).resolve(analysisId);
                if (!Files.exists(analysisResourcesPath)) {
                    Files.createDirectories(analysisResourcesPath);
                }

                // Download each file
                for (String fileName : filenames) {
                    downloadedFiles.add(downloadFile(baseUrl, analysisId, fileName, analysisResourcesPath).toFile());
                }
                LOGGER.info("Download complete for analysis '" + analysisId + "'");
            } else {
                String msg = "Could not acquire lock for analysis '" + analysisId + "' within " + LOCK_TIMEOUT + " hours. Skipping...";
                LOGGER.error(msg);
                throw new RuntimeException(msg);
            }
        } catch (InterruptedException e) {
            // Restore interrupt status
            Thread.currentThread().interrupt();
            String msg = "Interrupted while trying to acquire lock for analysis '" + analysisId + "'";
            LOGGER.error(msg);
            throw new RuntimeException(msg, e);
        } finally {
            if (lockAcquired) {
                // Release the lock
                lock.unlock();
            }
        }

        return downloadedFiles;
    }

    public static List<String> readAllLines(Path path) throws IOException {
        List<String> lines;
        if (!Files.exists(path)) {
            String msg = "Filename '" + path + "' does not exist";
            LOGGER.error(msg);
            throw new IOException(msg);
        }
        try {
            lines = Files.readAllLines(path);
            if (CollectionUtils.isEmpty(lines)) {
                String msg = "Filename '" + path + "' is empty";
                LOGGER.error(msg);
                throw new IOException(msg);
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
        return lines;
    }

    private static Path downloadFile(String baseUrl, String analysisId, String filename, Path localPath) throws IOException {
        String fileUrl = baseUrl + analysisId + "/" + filename;
        Path localFile = localPath.resolve(filename);

        // Check if the file already exists
        if (Files.exists(localFile)) {
            LOGGER.info("File " + filename + " already exists, skipping download");
            return localFile;
        }

        try (BufferedInputStream in = new BufferedInputStream(new URL(fileUrl).openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(localFile.toFile())) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to download file: " + fileUrl, e);
            throw e;
        }

        if (!Files.exists(localFile)) {
            String msg = "Something wrong happened, file '" + filename + "' + does not exist after downloading at '" + fileUrl + "'";
            LOGGER.error(msg);
            throw new IOException(msg);
        }

        return localFile;
    }

    public static DownloadedRefGenome downloadRefGenome(String assembly, Path outDir, Path openCgaHome) throws IOException {
        // Download files
        File gzFile = null;
        File faiFile = null;
        File gziFile = null;

        // Get files to downloadAnalysis
        List<String> filenames = new LinkedList<>();
        filenames.add("Homo_sapiens." + assembly + ".dna.primary_assembly.fa.gz");
        filenames.add("Homo_sapiens." + assembly + ".dna.primary_assembly.fa.gz.fai");
        filenames.add("Homo_sapiens." + assembly + ".dna.primary_assembly.fa.gz.gzi");

        Path path = null;
        for (String filename : filenames) {
            File file;

            if (openCgaHome != null) {
                path = openCgaHome.resolve("analysis/commons/reference-genomes/" + filename);
            }
            if (path != null && path.toFile().exists()) {
                File outFile = outDir.resolve(path.toFile().getName()).toFile();
                LOGGER.info("downloadRefGenome from path: " + path + " to " + outFile.getAbsolutePath());
                FileUtils.copyFile(path.toFile(), outFile);
                file = outFile;
            } else {
                URL url = new URL(URL + "analysis/commons/reference-genomes/" + filename);
                LOGGER.info("downloadAnalysis from URL: " + URL + ", (path does not exist: " + path + ")");
                file = URLUtils.download(url, outDir);
                if (file == null) {
                    // Something wrong happened, remove downloaded files
                    cleanRefGenome(filenames, outDir);
                    return null;
                }
            }
            if (filename.endsWith("gz")) {
                gzFile = file;
            } else if (filename.endsWith("fai")) {
                faiFile = file;
            } else if (filename.endsWith("gzi")) {
                gziFile = file;
            }

            // Reset path for the next iteration
            path = null;
        }
        return new DownloadedRefGenome(assembly, gzFile, faiFile, gziFile);
    }

    //-------------------------------------------------------------------------
    // Support for downloading reference genomes
    //-------------------------------------------------------------------------

    public static String getAssembly(CatalogManager catalogManager, String studyId, String sessionId) throws CatalogException {
        String assembly = "";
        OpenCGAResult<Project> projectQueryResult;

        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(sessionId);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, jwtPayload);
        String organizationId = studyFqn.getOrganizationId();

        projectQueryResult = catalogManager.getProjectManager().search(organizationId, new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), studyId),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()), sessionId);
        if (CollectionUtils.isNotEmpty(projectQueryResult.getResults())
                && projectQueryResult.first().getOrganism() != null
                && projectQueryResult.first().getOrganism().getAssembly() != null) {
            assembly = projectQueryResult.first().getOrganism().getAssembly();
        }
        return assembly;
    }

    public static class DownloadedRefGenome {
        private String assembly;
        private File gzFile;
        private File faiFile;
        private File gziFile;

        public DownloadedRefGenome(String assembly, File gzFile, File faiFile, File gziFile) {
            this.assembly = assembly;
            this.gzFile = gzFile;
            this.faiFile = faiFile;
            this.gziFile = gziFile;
        }

        public String getAssembly() {
            return assembly;
        }

        public DownloadedRefGenome setAssembly(String assembly) {
            this.assembly = assembly;
            return this;
        }

        public File getGzFile() {
            return gzFile;
        }

        public DownloadedRefGenome setGzFile(File gzFile) {
            this.gzFile = gzFile;
            return this;
        }

        public File getFaiFile() {
            return faiFile;
        }

        public DownloadedRefGenome setFaiFile(File faiFile) {
            this.faiFile = faiFile;
            return this;
        }

        public File getGziFile() {
            return gziFile;
        }

        public DownloadedRefGenome setGziFile(File gziFile) {
            this.gziFile = gziFile;
            return this;
        }
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private static void cleanRefGenome(List<String> links, Path outDir) {
        for (String link : links) {
            String name = new File(link).getName();
            File file = outDir.resolve(name).toFile();
            if (file.exists()) {
                file.delete();
            }
        }
    }
}
