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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class KnockoutAnalysisParams extends ToolParams {

    public static final String DESCRIPTION = "Gene knockout analysis params";
    // Sample filter
    @DataField(description = ParamConstants.KNOCKOUT_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION)
    private List<String> sample;
    // family
    // phenotype
    // disorder
    // cohort
    // annotation

    // Gene filter
    @DataField(description = ParamConstants.KNOCKOUT_ANALYSIS_PARAMS_GENE_DESCRIPTION)
    private List<String> gene;
    @DataField(description = ParamConstants.KNOCKOUT_ANALYSIS_PARAMS_PANEL_DESCRIPTION)
    private List<String> panel;
    @DataField(description = ParamConstants.KNOCKOUT_ANALYSIS_PARAMS_BIOTYPE_DESCRIPTION)
    private String biotype;
    // HPO

    // Variant filter
    @DataField(description = ParamConstants.KNOCKOUT_ANALYSIS_PARAMS_CONSEQUENCE_TYPE_DESCRIPTION)
    private String consequenceType;
    @DataField(description = ParamConstants.KNOCKOUT_ANALYSIS_PARAMS_FILTER_DESCRIPTION)
    private String filter;
    @DataField(description = ParamConstants.KNOCKOUT_ANALYSIS_PARAMS_QUAL_DESCRIPTION)
    private String qual;

    // Do not generate genes file
    @DataField(description = ParamConstants.KNOCKOUT_ANALYSIS_PARAMS_SKIP_GENES_FILE_DESCRIPTION)
    private boolean skipGenesFile;

    @DataField(description = ParamConstants.KNOCKOUT_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;

    @DataField(description = ParamConstants.KNOCKOUT_ANALYSIS_PARAMS_INDEX_DESCRIPTION)
    private boolean index;

    public KnockoutAnalysisParams() {
    }

    public KnockoutAnalysisParams(List<String> sample, List<String> gene, List<String> panel, String biotype, String consequenceType,
                                  String filter, String qual, boolean skipGenesFile, String outdir,
                                  boolean index) {
        this.sample = sample;
        this.gene = gene;
        this.panel = panel;
        this.biotype = biotype;
        this.consequenceType = consequenceType;
        this.filter = filter;
        this.qual = qual;
        this.skipGenesFile = skipGenesFile;
        this.outdir = outdir;
        this.index = index;
    }

    public List<String> getSample() {
        return sample;
    }

    public KnockoutAnalysisParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public List<String> getGene() {
        return gene;
    }

    public KnockoutAnalysisParams setGene(List<String> gene) {
        this.gene = gene;
        return this;
    }

    public List<String> getPanel() {
        return panel;
    }

    public KnockoutAnalysisParams setPanel(List<String> panel) {
        this.panel = panel;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public KnockoutAnalysisParams setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getConsequenceType() {
        return consequenceType;
    }

    public KnockoutAnalysisParams setConsequenceType(String consequenceType) {
        this.consequenceType = consequenceType;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public KnockoutAnalysisParams setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public KnockoutAnalysisParams setQual(String qual) {
        this.qual = qual;
        return this;
    }

    public boolean isSkipGenesFile() {
        return skipGenesFile;
    }

    public KnockoutAnalysisParams setSkipGenesFile(boolean skipGenesFile) {
        this.skipGenesFile = skipGenesFile;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public KnockoutAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public boolean isIndex() {
        return index;
    }

    public KnockoutAnalysisParams setIndex(boolean index) {
        this.index = index;
        return this;
    }

}
