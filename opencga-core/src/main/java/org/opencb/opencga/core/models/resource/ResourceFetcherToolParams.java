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

package org.opencb.opencga.core.models.resource;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class ResourceFetcherToolParams extends ToolParams {

    public static final String DESCRIPTION = "Download-resources tool parameters";

    @DataField(id = "baseUrl", description = FieldConstants.DOWNLOAD_ALL_RESOURCES_BASEURL_DESCRIPTION)
    private String baseUrl;

    @DataField(id = "overwrite", description = FieldConstants.DOWNLOAD_ALL_RESOURCES_OVERWRITE_DESCRIPTION)
    private Boolean overwrite;

    public ResourceFetcherToolParams() {
    }

    public ResourceFetcherToolParams(String baseUrl, Boolean overwrite) {
        this.baseUrl = baseUrl;
        this.overwrite = overwrite;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DownloadResourcesParams{");
        sb.append("baseUrl='").append(baseUrl).append('\'');
        sb.append(", overwrite=").append(overwrite);
        sb.append('}');
        return sb.toString();
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public ResourceFetcherToolParams setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
        return this;
    }

    public Boolean getOverwrite() {
        return overwrite;
    }

    public ResourceFetcherToolParams setOverwrite(Boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }
}
