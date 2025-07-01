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

package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.models.variant.qc.SampleQcAnalysisParams;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.nio.file.Path;

public abstract class SampleVariantQcAnalysisExecutor extends OpenCgaToolExecutor {

    protected Path vcfPath;
    protected Path jsonPath;
    protected SampleQcAnalysisParams qcParams;

    public SampleVariantQcAnalysisExecutor() {
    }

    public Path getVcfPath() {
        return vcfPath;
    }

    public SampleVariantQcAnalysisExecutor setVcfPath(Path vcfPath) {
        this.vcfPath = vcfPath;
        return this;
    }

    public Path getJsonPath() {
        return jsonPath;
    }

    public SampleVariantQcAnalysisExecutor setJsonPath(Path jsonPaths) {
        this.jsonPath = jsonPath;
        return this;
    }

    public SampleQcAnalysisParams getQcParams() {
        return qcParams;
    }

    public SampleVariantQcAnalysisExecutor setQcParams(SampleQcAnalysisParams qcParams) {
        this.qcParams = qcParams;
        return this;
    }
}
