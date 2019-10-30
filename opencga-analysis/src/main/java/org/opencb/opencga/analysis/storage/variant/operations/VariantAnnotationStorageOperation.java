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

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.analysis.storage.models.StudyInfo;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 24/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationStorageOperation extends StorageOperation {

    public VariantAnnotationStorageOperation(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, StorageEngineFactory.get(storageConfiguration),
                LoggerFactory.getLogger(VariantAnnotationStorageOperation.class));
    }

    public List<File> annotateVariants(@Nullable String projectStr, @Nullable List<StudyInfo> studyInfos, Query query, String outdirStr,
                                       String sessionId, ObjectMap options)
            throws CatalogException, StorageEngineException, URISyntaxException, IOException {
        if (options == null) {
            options = new ObjectMap();
        }

        // Outdir must be empty
        URI outdirUri = UriUtils.createDirectoryUri(outdirStr);
        final Path outdir = Paths.get(outdirUri);
        outdirMustBeEmpty(outdir, options);

        List<File> newFiles;
        Thread hook = buildHook(outdir);
        writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.RUNNING, "Job has just started"));
        Runtime.getRuntime().addShutdownHook(hook);
        // Up to this point, catalog has not been modified
        try {
            final List<String> studyIds;
            final String studyStr;
            final String alias;
            final DataStore dataStore;
            final Project.Organism organism;
            final int currentRelease;

            if (studyInfos == null || studyInfos.isEmpty()) {
                Project project = catalogManager.getProjectManager().get(projectStr, null, sessionId).first();
                studyStr = null;
                alias = project.getId();
                organism = project.getOrganism();
                currentRelease = project.getCurrentRelease();
                dataStore = getDataStoreByProjectId(catalogManager, projectStr, File.Bioformat.VARIANT, sessionId);
                studyIds = Collections.emptyList();
            } else {
                StudyInfo info = studyInfos.get(0);
                if (studyInfos.size() == 1) {
                    studyStr = info.getStudy().getFqn();
                    alias = info.getStudy().getAlias();
                } else {
                    studyStr = null;
                    alias = studyInfos.get(0).getProjectId();
                }
                dataStore = info.getDataStores().get(File.Bioformat.VARIANT);
                organism = info.getOrganism();
                studyIds = studyInfos.stream().map(StudyInfo::getStudyFQN).collect(Collectors.toList());
                Project project = catalogManager.getProjectManager().get(info.getProjectId(), null, sessionId).first();
                currentRelease = project.getCurrentRelease();
                for (int i = 1; i < studyInfos.size(); i++) {
                    info = studyInfos.get(i);
                    if (!dataStore.equals(info.getDataStores().get(File.Bioformat.VARIANT))) {
                        throw new CatalogException("Can't annotate studies from different databases");
                    }
                    if (!organism.equals(info.getOrganism())) {
                        throw new CatalogException("Can't annotate studies with different organisms");
                    }
                }
            }

            String outputFileName = options.getString(DefaultVariantAnnotationManager.FILE_NAME);
            if (StringUtils.isEmpty(outputFileName)) {
                outputFileName = buildOutputFileName(alias, query);
            }

            String catalogOutDirId = getCatalogOutdirId(studyStr, options, sessionId);

            Query annotationQuery = new Query(query);
            if (!options.getBoolean(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, false)) {
                annotationQuery.put(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
            }
            if (studyIds != null && !studyIds.isEmpty()) {
                annotationQuery.put(VariantQueryParam.STUDY.key(), studyIds);
            }

            QueryOptions annotationOptions = new QueryOptions(options)
                    .append(DefaultVariantAnnotationManager.OUT_DIR, outdirUri.getPath());
            annotationOptions.put(DefaultVariantAnnotationManager.FILE_NAME, outputFileName);

            String loadFileStr = annotationOptions.getString(VariantAnnotationManager.LOAD_FILE);
            if (StringUtils.isNotEmpty(loadFileStr)) {
                boolean fileExists;
                try {
                    URI uri = UriUtils.createUriSafe(loadFileStr);
                    fileExists = uri != null && Paths.get(uri).toFile().exists();
                } catch (RuntimeException ignored) {
                    fileExists = false;
                }
                if (!fileExists) {
                    File loadFile = catalogManager.getFileManager().get(studyStr, loadFileStr, null, sessionId).first();
                    if (loadFile == null) {
                        throw new CatalogException("File '" + loadFileStr + "' does not exist!");
                    }
                    annotationOptions.put(VariantAnnotationManager.LOAD_FILE, loadFile.getUri().toString());
                }
            }

//            StudyConfiguration studyConfiguration = updateStudyConfiguration(sessionId, studyId, dataStore);
            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);

            updateProjectMetadata(variantStorageEngine.getMetadataManager(), organism, currentRelease);

            variantStorageEngine.annotate(annotationQuery, annotationOptions);

            if (catalogOutDirId != null) {
                newFiles = copyResults(Paths.get(outdirUri), studyStr, catalogOutDirId, sessionId);
            } else {
                newFiles = Collections.emptyList();
            }

            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.DONE, "Job completed"));
        } catch (Exception e) {
            // Error!
            logger.error("Error annotating variants.", e);
            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job with error : " + e.getMessage()));
            throw new StorageEngineException("Error annotating variants.", e);
        } finally {
            // Remove hook
            Runtime.getRuntime().removeShutdownHook(hook);
        }

        return newFiles;
    }

    private String buildOutputFileName(String alias, Query query) {
        List<Region> regions = new ArrayList<>();
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.REGION)) {
            List<Region> c = Region.parseRegions(query.getString(VariantQueryParam.REGION.key()));
            if (c != null) {
                regions.addAll(c);
            }
        }
        if (regions.isEmpty() || regions.size() > 1) {
            return alias + '.' + TimeUtils.getTime();
        } else {
            return alias + ".region_" + regions.get(0).toString() + '.' + TimeUtils.getTime();
        }
    }
}
