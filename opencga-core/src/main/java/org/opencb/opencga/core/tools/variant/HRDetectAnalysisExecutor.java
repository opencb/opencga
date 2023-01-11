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
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.nio.file.Path;

public abstract class HRDetectAnalysisExecutor extends OpenCgaToolExecutor {

    private String study;
    private String somaticSample;
    private String germlineSample;
    private String assembly;
    private Path snvRDataPath;
    private Path svRDataPath;
    private ObjectMap cnvQuery;
    private ObjectMap indelQuery;
    private String snv3CustomName;
    private String snv8CustomName;
    private String sv3CustomName;
    private String sv8CustomName;
    private Boolean bootstrap;

    public HRDetectAnalysisExecutor() {
    }

    public HRDetectAnalysisExecutor(String study, String somaticSample, String germlineSample, String assembly, Path snvRDataPath,
                                    Path svRDataPath, ObjectMap cnvQuery, ObjectMap indelQuery, String snv3CustomName,
                                    String snv8CustomName, String sv3CustomName, String sv8CustomName, Boolean bootstrap) {
        this.study = study;
        this.somaticSample = somaticSample;
        this.germlineSample = germlineSample;
        this.assembly = assembly;
        this.snvRDataPath = snvRDataPath;
        this.svRDataPath = svRDataPath;
        this.cnvQuery = cnvQuery;
        this.indelQuery = indelQuery;
        this.snv3CustomName = snv3CustomName;
        this.snv8CustomName = snv8CustomName;
        this.sv3CustomName = sv3CustomName;
        this.sv8CustomName = sv8CustomName;
        this.bootstrap = bootstrap;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HRDetectAnalysisExecutor{");
        sb.append("study='").append(study).append('\'');
        sb.append(", somaticSample='").append(somaticSample).append('\'');
        sb.append(", germlineSample='").append(germlineSample).append('\'');
        sb.append(", assembly='").append(assembly).append('\'');
        sb.append(", snvRDataPath=").append(snvRDataPath);
        sb.append(", svRDataPath=").append(svRDataPath);
        sb.append(", cnvQuery=").append(cnvQuery);
        sb.append(", indelQuery=").append(indelQuery);
        sb.append(", snv3CustomName='").append(snv3CustomName).append('\'');
        sb.append(", snv8CustomName='").append(snv8CustomName).append('\'');
        sb.append(", sv3CustomName='").append(sv3CustomName).append('\'');
        sb.append(", sv8CustomName='").append(sv8CustomName).append('\'');
        sb.append(", bootstrap=").append(bootstrap);
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

    public String getSomaticSample() {
        return somaticSample;
    }

    public HRDetectAnalysisExecutor setSomaticSample(String somaticSample) {
        this.somaticSample = somaticSample;
        return this;
    }

    public String getGermlineSample() {
        return germlineSample;
    }

    public HRDetectAnalysisExecutor setGermlineSample(String germlineSample) {
        this.germlineSample = germlineSample;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public HRDetectAnalysisExecutor setAssembly(String assembly) {
        this.assembly = assembly;
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

    public String getSnv3CustomName() {
        return snv3CustomName;
    }

    public HRDetectAnalysisExecutor setSnv3CustomName(String snv3CustomName) {
        this.snv3CustomName = snv3CustomName;
        return this;
    }

    public String getSnv8CustomName() {
        return snv8CustomName;
    }

    public HRDetectAnalysisExecutor setSnv8CustomName(String snv8CustomName) {
        this.snv8CustomName = snv8CustomName;
        return this;
    }

    public String getSv3CustomName() {
        return sv3CustomName;
    }

    public HRDetectAnalysisExecutor setSv3CustomName(String sv3CustomName) {
        this.sv3CustomName = sv3CustomName;
        return this;
    }

    public String getSv8CustomName() {
        return sv8CustomName;
    }

    public HRDetectAnalysisExecutor setSv8CustomName(String sv8CustomName) {
        this.sv8CustomName = sv8CustomName;
        return this;
    }

    public Boolean getBootstrap() {
        return bootstrap;
    }

    public HRDetectAnalysisExecutor setBootstrap(Boolean bootstrap) {
        this.bootstrap = bootstrap;
        return this;
    }
}
