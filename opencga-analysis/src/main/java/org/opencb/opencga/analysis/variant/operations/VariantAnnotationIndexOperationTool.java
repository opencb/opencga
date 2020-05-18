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

package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationIndexParams;
import org.opencb.opencga.core.tools.ToolParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.util.List;

@Tool(id = VariantAnnotationIndexOperationTool.ID, description = VariantAnnotationIndexOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.PROJECT,
        resource = Enums.Resource.VARIANT)
public class VariantAnnotationIndexOperationTool extends OperationTool {
    public static final String ID = "variant-annotation-index";
    public static final String DESCRIPTION = "Create and load variant annotations into the database";
    private List<String> studies;
    private String projectStr;
    private VariantAnnotationIndexParams annotationParams;

    public VariantAnnotationIndexOperationTool setAnnotationParams(VariantAnnotationIndexParams annotationParams) {
        this.annotationParams = annotationParams;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        studies = studies == null ? params.getAsStringList(VariantQueryParam.STUDY.key()) : studies;
        projectStr = params.getString(VariantCatalogQueryUtils.PROJECT.key(), projectStr);

        if (annotationParams == null) {
            annotationParams = ToolParams.fromParams(VariantAnnotationIndexParams.class, params);
        }

        params.putIfNotEmpty(VariantStorageOptions.ANNOTATOR.key(), annotationParams.getAnnotator());
        params.putIfNotEmpty(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, annotationParams.getCustomName());
        params.put(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), annotationParams.isOverwriteAnnotations());
        params.put(DefaultVariantAnnotationManager.FILE_NAME, annotationParams.getOutputFileName());
        params.put(VariantQueryParam.REGION.key(), annotationParams.getRegion());
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            if (StringUtils.isEmpty(annotationParams.getLoad())) {
                variantStorageManager.annotate(
                        projectStr,
                        studies,
                        annotationParams.getRegion(),
                        annotationParams.isOverwriteAnnotations(),
                        getOutDir(keepIntermediateFiles).toString(),
                        annotationParams.getOutputFileName(),
                        params,
                        token);
            } else {
                variantStorageManager.annotationLoad(projectStr, studies, annotationParams.getLoad(), params, token);
            }
        });
    }
}
