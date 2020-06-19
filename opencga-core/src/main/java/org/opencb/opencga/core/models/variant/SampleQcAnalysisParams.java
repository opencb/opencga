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

public class SampleQcAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Sample QC analysis params";
    private String sample;
    private String bamFilename;
    private String fastaFilename;
    private String baitFilename;
    private String targetFilename;
    private String minorAlleleFreq;
    private String relatednessMethod;
    private String outdir;

    public SampleQcAnalysisParams() {
    }

    public SampleQcAnalysisParams(String sample, String bamFilename, String fastaFilename, String baitFilename, String targetFilename,
                                  String minorAlleleFreq, String relatednessMethod, String outdir) {
        this.sample = sample;
        this.bamFilename = bamFilename;
        this.fastaFilename = fastaFilename;
        this.baitFilename = baitFilename;
        this.targetFilename = targetFilename;
        this.minorAlleleFreq = minorAlleleFreq;
        this.relatednessMethod = relatednessMethod;
        this.outdir = outdir;
    }

    public String getSample() {
        return sample;
    }

    public SampleQcAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getBamFilename() {
        return bamFilename;
    }

    public SampleQcAnalysisParams setBamFilename(String bamFilename) {
        this.bamFilename = bamFilename;
        return this;
    }

    public String getFastaFilename() {
        return fastaFilename;
    }

    public SampleQcAnalysisParams setFastaFilename(String fastaFilename) {
        this.fastaFilename = fastaFilename;
        return this;
    }

    public String getBaitFilename() {
        return baitFilename;
    }

    public SampleQcAnalysisParams setBaitFilename(String baitFilename) {
        this.baitFilename = baitFilename;
        return this;
    }

    public String getTargetFilename() {
        return targetFilename;
    }

    public SampleQcAnalysisParams setTargetFilename(String targetFilename) {
        this.targetFilename = targetFilename;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public SampleQcAnalysisParams setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public SampleQcAnalysisParams setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public SampleQcAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
