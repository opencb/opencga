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

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.models.StudyInfo;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator;
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
            final List<Long> studyIds;
            final String studyStr;
            final String alias;
            final DataStore dataStore;
            final Project.Organism organism;

            if (studyInfos == null || studyInfos.isEmpty()) {
                long projectId = catalogManager.getProjectId(projectStr, sessionId);
                Project project = catalogManager.getProject(projectId, null, sessionId).first();
                studyStr = null;
                alias = project.getAlias();
                organism = project.getOrganism();
                dataStore = getDataStoreByProjectId(catalogManager, projectId, File.Bioformat.VARIANT, sessionId);
                studyIds = Collections.emptyList();
            } else {
                StudyInfo info = studyInfos.get(0);
                if (studyInfos.size() == 1) {
                    studyStr = String.valueOf(info.getStudy().getId());
                    alias = info.getStudyAlias();
                } else {
                    studyStr = null;
                    alias = studyInfos.get(0).getProjectAlias();
                }
                dataStore = info.getDataStores().get(File.Bioformat.VARIANT);
                organism = info.getOrganism();
                studyIds = studyInfos.stream().map(StudyInfo::getStudyId).collect(Collectors.toList());
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

            Long catalogOutDirId = getCatalogOutdirId(studyStr, options, sessionId);

            Query annotationQuery = new Query(query);
            if (!options.getBoolean(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, false)) {
                annotationQuery.put(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
            }
            if (studyIds != null && !studyIds.isEmpty()) {
                annotationQuery.put(VariantQueryParam.STUDIES.key(), studyIds);
            }

            QueryOptions annotationOptions = new QueryOptions(options)
                    .append(DefaultVariantAnnotationManager.OUT_DIR, outdirUri.getPath());
            annotationOptions.put(DefaultVariantAnnotationManager.FILE_NAME, outputFileName);

            String loadFileStr = options.getString(VariantAnnotationManager.LOAD_FILE);
            if (StringUtils.isNotEmpty(loadFileStr)) {
                if (!Paths.get(UriUtils.createUri(loadFileStr)).toFile().exists()) {
                    long fileId = catalogManager.getFileId(loadFileStr, studyStr, sessionId);
                    if (fileId < 0) {
                        throw new CatalogException("File '" + loadFileStr + "' does not exist!");
                    }
                    File loadFile = catalogManager.getFile(fileId, sessionId).first();
                    annotationOptions.put(VariantAnnotationManager.LOAD_FILE, loadFile.getUri().toString());
                }
            }
            if (organism == null) {
                annotationOptions.putIfAbsent(VariantAnnotationManager.SPECIES, "hsapiens");
                annotationOptions.putIfAbsent(VariantAnnotationManager.ASSEMBLY, "GRch37");
            } else {
                String scientificName = organism.getScientificName();
                scientificName = AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName(scientificName);
                annotationOptions.put(VariantAnnotationManager.SPECIES, scientificName);
                annotationOptions.put(VariantAnnotationManager.ASSEMBLY, organism.getAssembly());
            }

//            StudyConfiguration studyConfiguration = updateStudyConfiguration(sessionId, studyId, dataStore);
            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
            variantStorageEngine.annotate(annotationQuery, annotationOptions);

            if (catalogOutDirId != null) {
                newFiles = copyResults(Paths.get(outdirUri), catalogOutDirId, sessionId);
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
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.CHROMOSOME)) {
            List<Region> c = Region.parseRegions(query.getString(VariantQueryParam.CHROMOSOME.key()));
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
