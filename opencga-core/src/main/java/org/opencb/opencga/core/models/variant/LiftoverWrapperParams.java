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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class LiftoverWrapperParams extends ToolParams {

    public static final String DESCRIPTION = "BCFtools +liftover plugin parameterss";

    @DataField(id = "files", description = FieldConstants.LIFTOVER_FILES_DESCRIPTION, required = true)
    private List<String> files;

    @DataField(id = "targetAssembly", description = FieldConstants.LIFTOVER_TARGET_ASSEMBLY_DESCRIPTION, required = true)
    private String targetAssembly;

    @DataField(id = "vcfDestination", description = FieldConstants.LIFTOVER_VCF_DESTINATION_DESCRIPTION)
    private String vcfDestination;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public LiftoverWrapperParams() {
    }

    public LiftoverWrapperParams(List<String> files, String targetAssembly, String vcfDestination, String outdir) {
        this.files = files;
        this.targetAssembly = targetAssembly;
        this.vcfDestination = vcfDestination;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LiftoverWrapperParams{");
        sb.append("files=").append(files);
        sb.append(", targetAssembly='").append(targetAssembly).append('\'');
        sb.append(", vcfDestination='").append(vcfDestination).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public List<String> getFiles() {
        return files;
    }

    public LiftoverWrapperParams setFiles(List<String> files) {
        this.files = files;
        return this;
    }

    public String getTargetAssembly() {
        return targetAssembly;
    }

    public LiftoverWrapperParams setTargetAssembly(String targetAssembly) {
        this.targetAssembly = targetAssembly;
        return this;
    }

    public String getVcfDestination() {
        return vcfDestination;
    }

    public LiftoverWrapperParams setVcfDestination(String vcfDestination) {
        this.vcfDestination = vcfDestination;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public LiftoverWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
