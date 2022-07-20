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

package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class PlinkWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Plink params";
    @DataField(description = ParamConstants.PLINK_WRAPPER_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;
    @DataField(description = ParamConstants.PLINK_WRAPPER_PARAMS_PLINK_PARAMS_DESCRIPTION)
    private Map<String, String> plinkParams;

    public PlinkWrapperParams() {
    }

    public PlinkWrapperParams(String outdir, Map<String, String> plinkParams) {
        this.outdir = outdir;
        this.plinkParams = plinkParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PlinkWrapperParams{");
        sb.append("outdir='").append(outdir).append('\'');
        sb.append(", plinkParams=").append(plinkParams);
        sb.append('}');
        return sb.toString();
    }

    public String getOutdir() {
        return outdir;
    }

    public PlinkWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getPlinkParams() {
        return plinkParams;
    }

    public PlinkWrapperParams setPlinkParams(Map<String, String> plinkParams) {
        this.plinkParams = plinkParams;
        return this;
    }
}
