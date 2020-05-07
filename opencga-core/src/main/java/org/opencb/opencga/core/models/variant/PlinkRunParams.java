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

import java.util.Map;

public class PlinkRunParams extends ToolParams {
    public static final String DESCRIPTION = "Plink params";
    private String tpedFile;  // Transpose PED file (.tped) containing SNP and genotype information
    private String tfamFile;  // Transpose FAM file (.tfam) containing individual and family information
    private String covarFile; // Covariate file
    private String outdir;
    private Map<String, String> plinkParams;

    public PlinkRunParams() {
    }

    public PlinkRunParams(String tpedFile, String tfamFile, String covarFile, String outdir, Map<String, String> plinkParams) {
        this.tpedFile = tpedFile;
        this.tfamFile = tfamFile;
        this.covarFile = covarFile;
        this.outdir = outdir;
        this.plinkParams = plinkParams;
    }

    public String getTpedFile() {
        return tpedFile;
    }

    public PlinkRunParams setTpedFile(String tpedFile) {
        this.tpedFile = tpedFile;
        return this;
    }

    public String getTfamFile() {
        return tfamFile;
    }

    public PlinkRunParams setTfamFile(String tfamFile) {
        this.tfamFile = tfamFile;
        return this;
    }

    public String getCovarFile() {
        return covarFile;
    }

    public PlinkRunParams setCovarFile(String covarFile) {
        this.covarFile = covarFile;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public PlinkRunParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getPlinkParams() {
        return plinkParams;
    }

    public PlinkRunParams setPlinkParams(Map<String, String> plinkParams) {
        this.plinkParams = plinkParams;
        return this;
    }
}
