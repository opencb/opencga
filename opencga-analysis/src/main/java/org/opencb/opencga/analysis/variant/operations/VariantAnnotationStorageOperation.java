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

package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.VariantStorageManager;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 24/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Analysis(id = VariantAnnotationStorageOperation.ID, description = "", type = Analysis.AnalysisType.VARIANT)
public class VariantAnnotationStorageOperation extends StorageOperation {

    public static final String ID = "variant-annotation-index";
    private List<String> studies;
    private String projectStr;
    private String outputFileName;
    private String region;
    private String loadFileStr;
    private boolean overwriteAnnotations;

    private DataStore dataStore;
    private Query annotationQuery;
    private QueryOptions annotationOptions;

    public VariantAnnotationStorageOperation setStudies(List<String> studies) {
        this.studies = studies;
        return this;
    }

    public VariantAnnotationStorageOperation setProject(String projectStr) {
        this.projectStr = projectStr;
        return this;
    }

    public VariantAnnotationStorageOperation setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
        return this;
    }

    public VariantAnnotationStorageOperation setRegion(String region) {
        this.region = region;
        return this;
    }

    public VariantAnnotationStorageOperation setLoadFile(String loadFileStr) {
        this.loadFileStr = loadFileStr;
        return this;
    }

    public VariantAnnotationStorageOperation setOverwriteAnnotations(boolean overwriteAnnotations) {
        this.overwriteAnnotations = overwriteAnnotations;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        studies = studies == null ? params.getAsStringList(VariantQueryParam.STUDY.key()) : studies;
        projectStr = params.getString(VariantCatalogQueryUtils.PROJECT.key(), projectStr);
        outputFileName = params.getString(DefaultVariantAnnotationManager.FILE_NAME, outputFileName);
        region = params.getString(VariantQueryParam.REGION.key(), region);
        loadFileStr = params.getString(VariantAnnotationManager.LOAD_FILE, loadFileStr);
        overwriteAnnotations = params.getBoolean(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), overwriteAnnotations);

        if (studies == null) {
            studies = Collections.emptyList();
        }
        if (studies.isEmpty() && StringUtils.isEmpty(projectStr)) {
            List<Project> projects = catalogManager.getProjectManager().get(new Query(), new QueryOptions(), token).getResults();
            if (projects.size() == 1) {
                projectStr = projects.get(0).getFqn();
            } else {
                throw new IllegalArgumentException("Expected either studies or project to annotate");
            }
        }

        if (!studies.isEmpty()) {
            // Ensure all studies are valid. Convert to FQN
            studies = catalogManager.getStudyManager()
                    .resolveIds(studies, catalogManager.getUserManager().getUserId(token))
                    .stream()
                    .map(Study::getFqn)
                    .collect(Collectors.toList());

            projectStr = catalogManager.getStudyManager().getProjectFqn(studies.get(0));

            if (studies.size() > 1) {
                for (String studyStr : studies) {
                    if (!projectStr.equals(catalogManager.getStudyManager().getProjectFqn(studyStr))) {
                        throw new CatalogException("Can't annotate studies from different projects!");
                    }
                }
            }
        }

        dataStore = VariantStorageManager.getDataStoreByProjectId(catalogManager, projectStr, File.Bioformat.VARIANT, token);

        annotationQuery = new Query();
        if (!overwriteAnnotations) {
            annotationQuery.put(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
        }
        if (CollectionUtils.isNotEmpty(studies)) {
            annotationQuery.put(VariantQueryParam.STUDY.key(), studies);
        }
        if (StringUtils.isNotEmpty(region)) {
            annotationQuery.put(VariantQueryParam.REGION.key(), region);
        }

        if (StringUtils.isEmpty(outputFileName)) {
            if (studies.size() == 1) {
                outputFileName = buildOutputFileName(studies.get(0));
            } else {
                outputFileName = buildOutputFileName(projectStr);
            }
        }

        annotationOptions = new QueryOptions(params)
                .append(DefaultVariantAnnotationManager.FILE_NAME, outputFileName);
        if (keepIntermediateFiles) {
            annotationOptions.append(DefaultVariantAnnotationManager.OUT_DIR, getOutDir());
        } else {
            annotationOptions.append(DefaultVariantAnnotationManager.OUT_DIR, getScratchDir());
        }

        if (StringUtils.isNotEmpty(loadFileStr)) {
            boolean fileExists = Files.exists(Paths.get(loadFileStr));
            if (fileExists) {
                annotationOptions.put(VariantAnnotationManager.LOAD_FILE, loadFileStr);
            } else {
                String studyStr;
                if (studies.size() == 1) {
                    studyStr = studies.get(0);
                } else if (studies.isEmpty()) {
                    throw new CatalogException("Unable to locate file '" + loadFileStr + "'. Missing study.");
                } else {
                    throw new CatalogException("Unable to locate file '" + loadFileStr + "'. More than one study found: " + studies);
                }
                File loadFile = catalogManager.getFileManager().get(studyStr, loadFileStr, null, token).first();
                if (loadFile == null) {
                    throw new CatalogException("File '" + loadFileStr + "' does not exist!");
                }
                annotationOptions.put(VariantAnnotationManager.LOAD_FILE, loadFile.getUri().toString());
            }
        }

        params.put(VariantQueryParam.STUDY.key(), studies);
        params.put(VariantCatalogQueryUtils.PROJECT.key(), projectStr);
        params.put(DefaultVariantAnnotationManager.FILE_NAME, outputFileName);
        params.put(VariantQueryParam.REGION.key(), region);
        params.put(VariantAnnotationManager.LOAD_FILE, loadFileStr);
        params.put(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), overwriteAnnotations);
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList("updateStorageMetadata", getId());
    }

    @Override
    public void run() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);

        step("updateStorageMetadata", () -> {
            Project project = catalogManager.getProjectManager().get(projectStr, null, token).first();
            Project.Organism organism = project.getOrganism();
            int currentRelease = project.getCurrentRelease();
            CatalogStorageMetadataSynchronizer.updateProjectMetadata(variantStorageEngine.getMetadataManager(), organism, currentRelease);
        });

        step(getId(), () -> {
            variantStorageEngine.annotate(annotationQuery, annotationOptions);
            if (keepIntermediateFiles) {
                java.io.File[] list = getOutDir().toFile().listFiles((fir, name) -> name.contains(outputFileName));
                if (list != null && list.length != 0) {
                    addFile(list[0].toPath());
                }
            }
        });
    }

    private String buildOutputFileName(String alias) {
        List<Region> regions = new ArrayList<>();
        if (StringUtils.isNotEmpty(region)) {
            List<Region> c = Region.parseRegions(region);
            if (c != null) {
                regions.addAll(c);
            }
        }
        alias = StringUtils.replaceChars(alias, ":@", "__");
        if (regions.size() != 1) {
            return alias + '.' + TimeUtils.getTime();
        } else {
            return alias + ".region_" + regions.get(0).toString() + '.' + TimeUtils.getTime();
        }
    }
}
