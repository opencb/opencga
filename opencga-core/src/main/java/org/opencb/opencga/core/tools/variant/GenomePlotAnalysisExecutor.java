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

import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.io.File;

public abstract class GenomePlotAnalysisExecutor extends OpenCgaToolExecutor {

    private String study;
    private File configFile;

    public GenomePlotAnalysisExecutor() {
    }

    public GenomePlotAnalysisExecutor(String study, File configFile) {
        this.study = study;
        this.configFile = configFile;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenomePlotAnalysisExecutor{");
        sb.append("study='").append(study).append('\'');
        sb.append(", configFile=").append(configFile);
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public GenomePlotAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public File getConfigFile() {
        return configFile;
    }

    public GenomePlotAnalysisExecutor setConfigFile(File configFile) {
        this.configFile = configFile;
        return this;
    }
}
