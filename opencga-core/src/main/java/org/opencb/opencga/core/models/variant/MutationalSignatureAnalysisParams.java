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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.ToolParams;

public class MutationalSignatureAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Mutational signature analysis params";

    private String sample;
    private String id;
    private String description;
    private ObjectMap query;

    // For fitting method
    private String catalogues;        // a file path
    private String cataloguesContent; // or the content of the catalogues file

    private int nBoot;
    private String sigVersion;
    private String organ;
    private float thresholdPerc;
    private float thresholdPVal;
    private int maxRareSigs;

    private String outdir;

    public MutationalSignatureAnalysisParams() {
    }

    public MutationalSignatureAnalysisParams(String sample, String id, String description, ObjectMap query, String catalogues,
                                             String cataloguesContent, int nBoot, String sigVersion, String organ, float thresholdPerc,
                                             float thresholdPVal, int maxRareSigs, String outdir) {
        this.sample = sample;
        this.id = id;
        this.description = description;
        this.query = query;
        this.catalogues = catalogues;
        this.cataloguesContent = cataloguesContent;
        this.nBoot = nBoot;
        this.sigVersion = sigVersion;
        this.organ = organ;
        this.thresholdPerc = thresholdPerc;
        this.thresholdPVal = thresholdPVal;
        this.maxRareSigs = maxRareSigs;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MutationalSignatureAnalysisParams{");
        sb.append("sample='").append(sample).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", query=").append(query);
        sb.append(", catalogues='").append(catalogues).append('\'');
        sb.append(", cataloguesContent='").append(cataloguesContent).append('\'');
        sb.append(", nBoot=").append(nBoot);
        sb.append(", sigversion='").append(sigVersion).append('\'');
        sb.append(", organ='").append(organ).append('\'');
        sb.append(", thresholdPerc=").append(thresholdPerc);
        sb.append(", thresholdPVal=").append(thresholdPVal);
        sb.append(", maxRareSigs=").append(maxRareSigs);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public MutationalSignatureAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getId() {
        return id;
    }

    public MutationalSignatureAnalysisParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public MutationalSignatureAnalysisParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ObjectMap getQuery() {
        return query;
    }

    public MutationalSignatureAnalysisParams setQuery(ObjectMap query) {
        this.query = query;
        return this;
    }

    public String getCatalogues() {
        return catalogues;
    }

    public MutationalSignatureAnalysisParams setCatalogues(String catalogues) {
        this.catalogues = catalogues;
        return this;
    }

    public String getCataloguesContent() {
        return cataloguesContent;
    }

    public MutationalSignatureAnalysisParams setCataloguesContent(String cataloguesContent) {
        this.cataloguesContent = cataloguesContent;
        return this;
    }

    public int getnBoot() {
        return nBoot;
    }

    public MutationalSignatureAnalysisParams setnBoot(int nBoot) {
        this.nBoot = nBoot;
        return this;
    }

    public String getSigVersion() {
        return sigVersion;
    }

    public MutationalSignatureAnalysisParams setSigVersion(String sigVersion) {
        this.sigVersion = sigVersion;
        return this;
    }

    public String getOrgan() {
        return organ;
    }

    public MutationalSignatureAnalysisParams setOrgan(String organ) {
        this.organ = organ;
        return this;
    }

    public float getThresholdPerc() {
        return thresholdPerc;
    }

    public MutationalSignatureAnalysisParams setThresholdPerc(float thresholdPerc) {
        this.thresholdPerc = thresholdPerc;
        return this;
    }

    public float getThresholdPVal() {
        return thresholdPVal;
    }

    public MutationalSignatureAnalysisParams setThresholdPVal(float thresholdPVal) {
        this.thresholdPVal = thresholdPVal;
        return this;
    }

    public int getMaxRareSigs() {
        return maxRareSigs;
    }

    public MutationalSignatureAnalysisParams setMaxRareSigs(int maxRareSigs) {
        this.maxRareSigs = maxRareSigs;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public MutationalSignatureAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
