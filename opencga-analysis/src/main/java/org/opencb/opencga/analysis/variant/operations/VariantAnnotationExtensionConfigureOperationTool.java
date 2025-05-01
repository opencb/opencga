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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationExtensionConfigureParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.VariantAnnotatorExtensionTask;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.VariantAnnotatorExtensionsFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Tool(id = VariantAnnotationExtensionConfigureOperationTool.ID, description = VariantAnnotationExtensionConfigureOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT, priority = Enums.Priority.HIGH)
public class VariantAnnotationExtensionConfigureOperationTool extends OperationTool {
    public static final String ID = "variant-annotation-extension-configure";
    public static final String DESCRIPTION = "Configure an annotation extension to be used when performing variant annotation";

    @ToolParams
    protected VariantAnnotationExtensionConfigureParams configureParams = new VariantAnnotationExtensionConfigureParams();

    private Project project = null;

    private List<String> resources = new ArrayList<>();
    private VariantAnnotatorExtensionTask variantAnnotatorExtensionTask;

    @Override
    protected void check() throws Exception {
        super.check();

        String projectFqn = getProjectFqn();
        if (StringUtils.isEmpty(projectFqn)) {
            throw new ToolException("Project not found from parameters: " + params.toJson());
        }
        project = catalogManager.getProjectManager().get(projectFqn, QueryOptions.empty(), token).first();

        // Check extension name
        ObjectMap options = new ObjectMap();
        options.put(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), configureParams.getExtension());
        List<VariantAnnotatorExtensionTask> extensions = new VariantAnnotatorExtensionsFactory().getVariantAnnotatorExtensions(options);
        if (CollectionUtils.isEmpty(extensions)) {
            throw new ToolException("No annotator extensions found for " + configureParams.getExtension());
        }
        if (extensions.size() > 1) {
            throw new ToolException("More than one annotation extension found for " + configureParams.getExtension());
        }
        variantAnnotatorExtensionTask = extensions.get(0);

        // Check extension resources
        if (CollectionUtils.isEmpty(configureParams.getResources())) {
            throw new ToolException("No resources found for annotation extension " + configureParams.getExtension());
        }
        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "fqn");
        List<Study> studies = getCatalogManager().getStudyManager().search(projectFqn, query, queryOptions, token).getResults();
        for (String resource : configureParams.getResources()) {
            boolean found = false;
            for (Study study : studies) {
                File file = getCatalogManager().getFileManager().get(study.getFqn(), resource, QueryOptions.empty(), token).first();
                if (file != null && Files.exists(Paths.get(file.getUri().getPath()))) {
                    resources.add(Paths.get(file.getUri().getPath()).toAbsolutePath().toString());
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new ToolException("Resource " + resource + " for annotation extension " + configureParams.getExtension()
                        + " does not exist in project " + project.getFqn());
            }
        }
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add(getId());
        return steps;
    }

    @Override
    protected void run() throws Exception {
        // Configure the variant annotator extension using resource physical paths
        variantAnnotatorExtensionTask.setup(new VariantAnnotationExtensionConfigureParams(
                configureParams.getExtension(), resources, configureParams.getConfiguration()));
        variantAnnotatorExtensionTask.checkAvailable();

        // Update project configuration with annotator extension options
        ObjectMap options;
        if (project.getInternal() != null && project.getInternal().getDatastores() != null
                && project.getInternal().getDatastores().getVariant() != null
                && MapUtils.isNotEmpty(project.getInternal().getDatastores().getVariant().getOptions())) {
            options = project.getInternal().getDatastores().getVariant().getOptions();
        } else {
            options = new ObjectMap();
        }
        updateOptions(options);
        getVariantStorageManager().configureProject(project.getFqn(), options, token);
    }

    private void updateOptions(ObjectMap options) {
        // Add annotator extension options
        if (options.containsKey(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key())) {
            List<String> annotatorExtensionList = options.getAsStringList(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key());
            if (!annotatorExtensionList.contains(configureParams.getExtension())) {
                annotatorExtensionList.add(configureParams.getExtension());
            }
            options.put(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), annotatorExtensionList);
        } else {
            options.put(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), configureParams.getExtension());
        }

        // Add the annotation extension options
        options.putAll(variantAnnotatorExtensionTask.getOptions());
    }
}
