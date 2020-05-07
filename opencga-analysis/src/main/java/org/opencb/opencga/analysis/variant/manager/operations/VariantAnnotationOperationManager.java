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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created on 24/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationOperationManager extends OperationManager {

    public VariantAnnotationOperationManager(VariantStorageManager variantStorageManager, VariantStorageEngine engine) {
        super(variantStorageManager, engine);
    }

    public void annotationLoad(String projectStr, List<String> studies, ObjectMap params, String loadFile, String token) throws Exception {
        annotate(projectStr, studies, loadFile, null, false, null, null, params, token);
    }

    public void annotate(String projectStr, List<String> studies, String region, String outputFileName, Path outdir, ObjectMap params,
                         String token, boolean overwriteAnnotations) throws Exception {
        String loadFileStr = params.getString(VariantAnnotationManager.LOAD_FILE);
        annotate(projectStr, studies, loadFileStr, region, overwriteAnnotations, outputFileName, outdir, params, token);
    }

    private void annotate(String projectStr, List<String> studies, String loadFileStr, String region, boolean overwriteAnnotations,
                          String outputFileName, Path outdir, ObjectMap params, String token)
            throws Exception {
        Query annotationQuery = new Query();
        if (!overwriteAnnotations) {
            annotationQuery.put(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
        }
        if (studies == null) {
            studies = Collections.emptyList();
        }
        if (CollectionUtils.isNotEmpty(studies)) {
            annotationQuery.put(VariantQueryParam.STUDY.key(), studies);
        }
        if (StringUtils.isNotEmpty(region)) {
            annotationQuery.put(VariantQueryParam.REGION.key(), region);
        }

        if (StringUtils.isEmpty(outputFileName)) {
            if (studies.size() == 1) {
                outputFileName = buildOutputFileName(studies.get(0), region);
            } else {
                outputFileName = buildOutputFileName(projectStr, region);
            }
        }

        QueryOptions annotationOptions = new QueryOptions(params).append(DefaultVariantAnnotationManager.FILE_NAME, outputFileName);
        annotationOptions.append(DefaultVariantAnnotationManager.OUT_DIR, outdir);

        synchronizeProjectMetadata(projectStr, token);

        if (StringUtils.isEmpty(loadFileStr)) {
            variantStorageEngine.annotate(annotationQuery, annotationOptions);
        } else {
            Path loadFilePath = Paths.get(loadFileStr);
            boolean fileExists = Files.exists(loadFilePath);
            URI uri;
            if (!fileExists) {
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
                uri = loadFile.getUri();
            } else {
                uri = loadFilePath.toUri();
            }
            variantStorageEngine.annotationLoad(uri, annotationOptions);
        }
    }

    private void synchronizeProjectMetadata(String projectStr, String token) throws CatalogException, StorageEngineException {
        Project project = catalogManager.getProjectManager().get(projectStr, null, token).first();
        ProjectOrganism organism = project.getOrganism();
        int currentRelease = project.getCurrentRelease();
        CatalogStorageMetadataSynchronizer.updateProjectMetadata(variantStorageEngine.getMetadataManager(), organism, currentRelease);
    }

    private String buildOutputFileName(String alias, String region) {
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
