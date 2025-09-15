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

package org.opencb.opencga.core.models.operations.variant;

import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.ArrayList;
import java.util.List;

public class VariantAnnotationExtensionConfigureParams extends ToolParams {

    public static final String DESCRIPTION = "Parameters to configure a variant annotation extension";

    @DataField(id = "extension", description = ParamConstants.VARIANT_ANNOTATION_EXTENSION_NAME_DESCR, required = true)
    private String extension;

    @DataField(id = "resources", description = ParamConstants.VARIANT_ANNOTATION_EXTENSION_RESOURCE_LIST_DESCR, required = true)
    private List<String> resources;

    @DataField(id = "params", description = ParamConstants.VARIANT_ANNOTATION_EXTENSION_PARAMS_DESCR)
    private ObjectMap params;

    @DataField(id = "overwrite", description = ParamConstants.VARIANT_ANNOTATION_EXTENSION_OVERWRITE_DESCR)
    private Boolean overwrite;

    public VariantAnnotationExtensionConfigureParams() {
        this("", new ArrayList<>(), new ObjectMap(), false);
    }

    public VariantAnnotationExtensionConfigureParams(String extension, List<String> resources, ObjectMap params, Boolean overwrite) {
        this.extension = extension;
        this.resources = resources;
        this.params = params;
        this.overwrite = overwrite;
    }

    public VariantAnnotationExtensionConfigureParams(VariantAnnotationExtensionConfigureParams configureParams) {
        this.extension = configureParams.extension;
        this.resources = new ArrayList<>(configureParams.resources);
        this.params = new ObjectMap(configureParams.params);
        this.overwrite = configureParams.overwrite;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantAnnotationExtensionConfigureParams{");
        sb.append("extension='").append(extension).append('\'');
        sb.append(", resources=").append(resources);
        sb.append(", params=").append(params);
        sb.append(", overwrite=").append(overwrite);
        sb.append('}');
        return sb.toString();
    }

    public String getExtension() {
        return extension;
    }

    public VariantAnnotationExtensionConfigureParams setExtension(String extension) {
        this.extension = extension;
        return this;
    }

    public List<String> getResources() {
        return resources;
    }

    public VariantAnnotationExtensionConfigureParams setResources(List<String> resources) {
        this.resources = resources;
        return this;
    }

    public ObjectMap getParams() {
        return params;
    }

    public VariantAnnotationExtensionConfigureParams setParams(ObjectMap params) {
        this.params = params;
        return this;
    }

    public Boolean getOverwrite() {
        return overwrite;
    }

    public VariantAnnotationExtensionConfigureParams setOverwrite(Boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }
}
