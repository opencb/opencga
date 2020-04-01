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

public class GatkRunParams extends ToolParams {
    public static final String DESCRIPTION = "gatk params";
    private String command;      // Valid value: HaplotypeCaller
    private String fastaFile;    // FASTA file
    private String bamFile;      // BAM file
    private String vcfFilename;  // VCF filename
    private String outdir;
    private Map<String, String> gatkParams;

    public GatkRunParams() {
    }

    public GatkRunParams(String command, String fastaFile, String bamFile, String vcfFilename, String outdir,
                         Map<String, String> gatkParams) {
        this.command = command;
        this.fastaFile = fastaFile;
        this.bamFile = bamFile;
        this.vcfFilename = vcfFilename;
        this.outdir = outdir;
        this.gatkParams = gatkParams;
    }

    public String getCommand() {
        return command;
    }

    public GatkRunParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getFastaFile() {
        return fastaFile;
    }

    public GatkRunParams setFastaFile(String fastaFile) {
        this.fastaFile = fastaFile;
        return this;
    }

    public String getBamFile() {
        return bamFile;
    }

    public GatkRunParams setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public String getVcfFilename() {
        return vcfFilename;
    }

    public GatkRunParams setVcfFilename(String vcfFilename) {
        this.vcfFilename = vcfFilename;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public GatkRunParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getGatkParams() {
        return gatkParams;
    }

    public GatkRunParams setGatkParams(Map<String, String> gatkParams) {
        this.gatkParams = gatkParams;
        return this;
    }
}
