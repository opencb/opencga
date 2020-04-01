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

public class RvtestsRunParams extends ToolParams {
    public static final String DESCRIPTION = "rvtest params";
    private String command;      // Valid values: rvtests or vcf2kinship
    private String vcfFile;      // VCF file
    private String phenoFile;    // Phenotype file
    private String pedigreeFile; // Pedigree file
    private String kinshipFile;  // Kinship file
    private String covarFile;    // Covariate file
    private String outdir;
    private Map<String, String> rvtestsParams;

    public RvtestsRunParams() {
    }

    public RvtestsRunParams(String command, String vcfFile, String phenoFile, String pedigreeFile, String kinshipFile, String covarFile,
                            String outdir, Map<String, String> rvtestsParams) {
        this.command = command;
        this.vcfFile = vcfFile;
        this.phenoFile = phenoFile;
        this.pedigreeFile = pedigreeFile;
        this.kinshipFile = kinshipFile;
        this.covarFile = covarFile;
        this.outdir = outdir;
        this.rvtestsParams = rvtestsParams;
    }

    public String getCommand() {
        return command;
    }

    public RvtestsRunParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getVcfFile() {
        return vcfFile;
    }

    public RvtestsRunParams setVcfFile(String vcfFile) {
        this.vcfFile = vcfFile;
        return this;
    }

    public String getPhenoFile() {
        return phenoFile;
    }

    public RvtestsRunParams setPhenoFile(String phenoFile) {
        this.phenoFile = phenoFile;
        return this;
    }

    public String getPedigreeFile() {
        return pedigreeFile;
    }

    public RvtestsRunParams setPedigreeFile(String pedigreeFile) {
        this.pedigreeFile = pedigreeFile;
        return this;
    }

    public String getKinshipFile() {
        return kinshipFile;
    }

    public RvtestsRunParams setKinshipFile(String kinshipFile) {
        this.kinshipFile = kinshipFile;
        return this;
    }

    public String getCovarFile() {
        return covarFile;
    }

    public RvtestsRunParams setCovarFile(String covarFile) {
        this.covarFile = covarFile;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public RvtestsRunParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getRvtestsParams() {
        return rvtestsParams;
    }

    public RvtestsRunParams setRvtestsParams(Map<String, String> rvtestsParams) {
        this.rvtestsParams = rvtestsParams;
        return this;
    }
}
