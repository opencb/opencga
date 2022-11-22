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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class HRDetectAnalysisExecutor extends OpenCgaToolExecutor {

    private String study;
    private String sample;
    private Path snvRDataPath;
    private Path svRDataPath;
    private ObjectMap cnvQuery;
    private ObjectMap indelQuery;

    public HRDetectAnalysisExecutor() {
    }

    public HRDetectAnalysisExecutor(String study, String sample, Path snvRDataPath, Path svRDataPath, ObjectMap cnvQuery,
                                    ObjectMap indelQuery) {
        this.study = study;
        this.sample = sample;
        this.snvRDataPath = snvRDataPath;
        this.svRDataPath = svRDataPath;
        this.cnvQuery = cnvQuery;
        this.indelQuery = indelQuery;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HRDetectAnalysisExecutor{");
        sb.append("study='").append(study).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append(", snvRDataPath=").append(snvRDataPath);
        sb.append(", svRDataPath=").append(svRDataPath);
        sb.append(", cnvQuery=").append(cnvQuery);
        sb.append(", indelQuery=").append(indelQuery);
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public HRDetectAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public HRDetectAnalysisExecutor setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public Path getSnvRDataPath() {
        return snvRDataPath;
    }

    public HRDetectAnalysisExecutor setSnvRDataPath(Path snvRDataPath) {
        this.snvRDataPath = snvRDataPath;
        return this;
    }

    public Path getSvRDataPath() {
        return svRDataPath;
    }

    public HRDetectAnalysisExecutor setSvRDataPath(Path svRDataPath) {
        this.svRDataPath = svRDataPath;
        return this;
    }

    public ObjectMap getCnvQuery() {
        return cnvQuery;
    }

    public HRDetectAnalysisExecutor setCnvQuery(ObjectMap cnvQuery) {
        this.cnvQuery = cnvQuery;
        return this;
    }

    public ObjectMap getIndelQuery() {
        return indelQuery;
    }

    public HRDetectAnalysisExecutor setIndelQuery(ObjectMap indelQuery) {
        this.indelQuery = indelQuery;
        return this;
    }
}
