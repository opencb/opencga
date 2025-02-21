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

import java.util.List;

public class LiftoverWrapperParams extends ToolParams {

    public static final String DESCRIPTION = "BCFtools +liftover plugin params";

    private List<String> files;
    private String targetAssembly;
    private String vcfOutdir;
    private String outdir;

    public LiftoverWrapperParams() {
    }

    public LiftoverWrapperParams(List<String> files, String targetAssembly, String vcfOutdir, String outdir) {
        this.files = files;
        this.targetAssembly = targetAssembly;
        this.vcfOutdir = vcfOutdir;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LiftoverWrapperParams{");
        sb.append("files=").append(files);
        sb.append(", targetAssembly='").append(targetAssembly).append('\'');
        sb.append(", vcfOutdir='").append(vcfOutdir).append('\'');
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

    public String getVcfOutdir() {
        return vcfOutdir;
    }

    public LiftoverWrapperParams setVcfOutdir(String vcfOutdir) {
        this.vcfOutdir = vcfOutdir;
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
