/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this project except in compliance with the License.
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
package org.opencb.opencga.app.cli.main.parent;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.StudiesCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.study.TemplateParams;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.List;

public abstract class ParentStudiesCommandExecutor extends OpencgaCommandExecutor {

    private final StudiesCommandOptions studiesCommandOptions;

    public ParentStudiesCommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean command,
                                        StudiesCommandOptions studiesCommandOptions) throws CatalogAuthenticationException {
        super(options, command);
        this.studiesCommandOptions = studiesCommandOptions;
    }

    protected RestResponse<Job> templateRun() throws Exception {
        logger.debug("Run template");
        StudiesCommandOptions.TemplateRunCommandOptions c = studiesCommandOptions.templateRunCommandOptions;

        c.study = getSingleValidStudy(c.study);
        TemplateParams templateParams = new TemplateParams(c.id, c.overwrite, c.resume);
        ObjectMap params = new ObjectMap();

        return openCGAClient.getStudyClient().runTemplates(c.study, templateParams, params);
    }

    protected RestResponse<String> templateUpload() throws Exception {
        logger.debug("Upload template file");
        StudiesCommandOptions.TemplateUploadCommandOptions c = studiesCommandOptions.templateUploadCommandOptions;

        ObjectMap params = new ObjectMap();

        c.study = getSingleValidStudy(c.study);
        Path path = Paths.get(c.inputFile);
        if (!path.toFile().exists()) {
            throw new CatalogException("File '" + c.inputFile + "' not found");
        }
        IOManagerFactory ioManagerFactory = new IOManagerFactory();
        IOManager ioManager = ioManagerFactory.get(path.toUri());

        if (path.toFile().isDirectory()) {
            List<String> fileList = new LinkedList<>();

            ioManager.walkFileTree(path.toUri(), new SimpleFileVisitor<URI>() {
                @Override
                public FileVisitResult preVisitDirectory(URI dir, BasicFileAttributes attrs) throws IOException {
                    if (!dir.equals(path.toUri())) {
                        throw new IOException("More than one directory found");
                    }
                    return super.preVisitDirectory(dir, attrs);
                }

                @Override
                public FileVisitResult visitFileFailed(URI file, IOException exc) throws IOException {
                    throw new IOException("Error visiting file '" + file + "'");
                }

                @Override
                public FileVisitResult visitFile(URI fileUri, BasicFileAttributes attrs) throws IOException {
                    fileList.add(fileUri.getPath());
                    return super.visitFile(fileUri, attrs);
                }
            });

            Path manifestPath = path.resolve("manifest.zip");
            logger.debug("Compressing file in '" + manifestPath + "' before uploading");
            ioManager.zip(fileList, manifestPath.toFile());
            params.put("file", manifestPath.toString());
        } else if (c.inputFile.endsWith("zip")) {
            params.put("file", c.inputFile);
        } else {
            throw new CatalogException("File '" + c.inputFile + "' is not a zip file");
        }

        RestResponse<String> uploadResponse = openCGAClient.getStudyClient().uploadTemplates(c.study, params);
        if (path.toFile().isDirectory()) {
            Path manifestPath = path.resolve("manifest.zip");
            logger.debug("Removing generated zip file '" + manifestPath + "' after upload");
            ioManager.deleteFile(manifestPath.toUri());
        }

        return uploadResponse;
    }

    /**
     * This method selects a single valid study from these sources and in this order. First, checks if CLI param exists, second it reads the
     * projects and studies from the session file.
     *
     * @param study parameter from the CLI
     * @return a single valid Study from the CLI, configuration or from the session file
     * @throws CatalogException when no possible single study can be chosen
     */
    private String getSingleValidStudy(String study) throws CatalogException {
        // First, check the study parameter, if is not empty we just return it, this the user's selection.
        if (StringUtils.isNotEmpty(study)) {
            return study;
        } else {
            // Third, check if there is only one single project and study for this user in the current CLI session file.
            List<String> studies = sessionManager.getStudies();
            if (CollectionUtils.isNotEmpty(studies) && studies.size() == 1) {
                study = studies.get(0);
            } else {
                throw new CatalogException("None or more than one study found");
            }
        }
        return study;
    }
}
