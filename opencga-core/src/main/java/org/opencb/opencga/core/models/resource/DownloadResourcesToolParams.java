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

import java.util.List;

public class DownloadResourcesToolParams extends ToolParams {

    public static final String DESCRIPTION = "Download-resources tool parameters";

    @DataField(id = "baseurl", description = FieldConstants.DOWNLOAD_ALL_RESOURCES_BASEURL_DESCRIPTION)
    private String baseurl;

    @DataField(id = "overwrite", description = FieldConstants.DOWNLOAD_ALL_RESOURCES_OVERWRITE_DESCRIPTION)
    private Boolean overwrite;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public DownloadResourcesToolParams() {
    }

    public DownloadResourcesToolParams(String baseurl, Boolean overwrite, String outdir) {
        this.baseurl = baseurl;
        this.overwrite = overwrite;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DownloadResourcesToolParams{");
        sb.append("baseurl='").append(baseurl).append('\'');
        sb.append(", overwrite=").append(overwrite);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getBaseurl() {
        return baseurl;
    }

    public DownloadResourcesToolParams setBaseurl(String baseurl) {
        this.baseurl = baseurl;
        return this;
    }

    public Boolean getOverwrite() {
        return overwrite;
    }

    public DownloadResourcesToolParams setOverwrite(Boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public DownloadResourcesToolParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
