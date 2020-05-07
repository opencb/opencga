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
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public abstract class GwasAnalysisExecutor extends OpenCgaToolExecutor {

    private String study;
    private List<String> sampleList1;
    private List<String> sampleList2;
    private String phenotype1;
    private String phenotype2;
    private String cohort1;
    private String cohort2;
    private Path outputFile;
    private GwasConfiguration configuration;

    public GwasAnalysisExecutor() {
    }

    protected List<String> getHeaderColumns() {
        List<String> columns = new ArrayList<>();
        columns.add("chromosome");
        columns.add("start");
        columns.add("end");
        columns.add("reference");
        columns.add("alternate");
        columns.add("dbSNP");
        columns.add("gene");
        columns.add("biotype");
        columns.add("consequence-types");
        if (configuration.getMethod() == GwasConfiguration.Method.CHI_SQUARE_TEST) {
            columns.add("chi-square");
        }
        columns.add("p-value");
        columns.add("odd-ratio");
        return columns;
    }

    protected String getHeaderLine() {
        return "#" + String.join("\t", getHeaderColumns());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GwasAnalysisExecutor{");
        sb.append("sampleList1=").append(sampleList1);
        sb.append(", sampleList2=").append(sampleList2);
        sb.append(", phenotype1='").append(phenotype1).append('\'');
        sb.append(", phenotype2='").append(phenotype2).append('\'');
        sb.append(", cohort1='").append(cohort1).append('\'');
        sb.append(", cohort2='").append(cohort2).append('\'');
        sb.append(", configuration=").append(configuration);
        sb.append(", executorParams=").append(getExecutorParams());
        sb.append(", outDir=").append(getOutDir());
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public GwasAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSampleList1() {
        return sampleList1;
    }

    public GwasAnalysisExecutor setSampleList1(List<String> sampleList1) {
        this.sampleList1 = sampleList1;
        return this;
    }

    public List<String> getSampleList2() {
        return sampleList2;
    }

    public GwasAnalysisExecutor setSampleList2(List<String> sampleList2) {
        this.sampleList2 = sampleList2;
        return this;
    }

    public String getPhenotype1() {
        return phenotype1;
    }

    public GwasAnalysisExecutor setPhenotype1(String phenotype1) {
        this.phenotype1 = phenotype1;
        return this;
    }

    public String getPhenotype2() {
        return phenotype2;
    }

    public GwasAnalysisExecutor setPhenotype2(String phenotype2) {
        this.phenotype2 = phenotype2;
        return this;
    }

    public String getCohort1() {
        return cohort1;
    }

    public GwasAnalysisExecutor setCohort1(String cohort1) {
        this.cohort1 = cohort1;
        return this;
    }

    public String getCohort2() {
        return cohort2;
    }

    public GwasAnalysisExecutor setCohort2(String cohort2) {
        this.cohort2 = cohort2;
        return this;
    }

    public Path getOutputFile() {
        return outputFile;
    }

    public GwasAnalysisExecutor setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    public GwasConfiguration getConfiguration() {
        return configuration;
    }

    public GwasAnalysisExecutor setConfiguration(GwasConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
}
