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
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.URLUtils;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.LinkedList;
import java.util.List;

public class AnalysisResourceUtils {

    protected String resourceBaseUrl;

    private static Logger logger = LoggerFactory.getLogger(AnalysisResourceUtils.class);

    public AnalysisResourceUtils(Configuration configuration) {
        resourceBaseUrl = configuration.getAnalysis().getResourceUrl();
        // Sanity check
        if (StringUtils.isEmpty(resourceBaseUrl)) {
            throw new InvalidParameterException("Missing resource URL in configuration file");
        }
    }

    public static File downloadThirdParty(URL url, Path outDir) throws IOException {
        return URLUtils.download(url, outDir);
    }

    public File downloadAnalysis(String analysisId, String resouceName, Path outDir, Path openCgaHome) throws IOException {
        Path path = null;
        String filename = analysisId + "/" + resouceName;
        if (openCgaHome != null) {
            path = openCgaHome.resolve(filename);
        }
        if (path != null && path.toFile().exists()) {
            File outFile = outDir.resolve(path.toFile().getName()).toFile();
            logger.info("Downloading from path: " + path + " to " + outFile.getAbsolutePath());
            FileUtils.copyFile(path.toFile(), outFile);

            return outFile;
        } else {
            logger.info("Downloading from URL: " + (resourceBaseUrl + filename) + ", (path does not exist: " + path + ")");
            return URLUtils.download(new URL(resourceBaseUrl + filename), outDir);
        }
    }

    public DownloadedRefGenome downloadRefGenome(String assembly, Path outDir, Path openCgaHome)
            throws IOException {
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
                path = openCgaHome.resolve("commons/reference-genomes/" + filename);
            }
            if (path != null && path.toFile().exists()) {
                File outFile = outDir.resolve(path.toFile().getName()).toFile();
                logger.info("Downloading from path: " + path + " to " + outFile.getAbsolutePath());
                FileUtils.copyFile(path.toFile(), outFile);
                file = outFile;
            } else {
                URL url = new URL(resourceBaseUrl + "commons/reference-genomes/" + filename);
                logger.info("Downloading from URL: " + resourceBaseUrl + ", (path does not exist: " + path + ")");
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
        projectQueryResult = catalogManager.getProjectManager().search(new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), studyId),
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

    public String getResourceBaseUrl() {
        return resourceBaseUrl;
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private void cleanRefGenome(List<String> links, Path outDir) {
        for (String link : links) {
            String name = new File(link).getName();
            File file = outDir.resolve(name).toFile();
            if (file.exists()) {
                file.delete();
            }
        }
    }
}
