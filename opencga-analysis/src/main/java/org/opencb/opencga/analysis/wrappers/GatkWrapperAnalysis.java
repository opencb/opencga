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

package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

@Tool(id = GatkWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = GatkWrapperAnalysis.DESCRIPTION)
public class GatkWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "gatk";
    public final static String DESCRIPTION = "GATK is a Genome Analysis Toolkit for variant discovery in high-throughput sequencing data.";

    public final static String GATK_DOCKER_IMAGE = "broadinstitute/gatk";

    public static final String COMMAND_PARAM = "command";
    public static final String FASTA_FILE_PARAM = "dictFile";
    public static final String BAM_FILE_PARAM = "bamFile";
    public static final String VCF_FILENAME_PARAM = "vcfFilename";

    private String command;
    private String fastaFile;
    private String bamFile;
    private String vcfFilename;

    private Map<String, URI> fileUriMap = new HashMap<>();

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(command)) {
            throw new ToolException("Missing Gatk command. Currently, only the command 'HaplotypeCaller' is supported");
        }

        switch (command) {
            case "HaplotypeCaller":
                break;
            default:
                // TODO: support other commands
                throw new ToolException("Gatk command '" + command + "' is not supported. Currently, only the commmand 'HaplotypeCaller' "
                        + "is suppported");
        }

        if (StringUtils.isEmpty(fastaFile)) {
            throw new ToolException("Missing FASTA file for reference genome");
        }

        if (StringUtils.isEmpty(bamFile)) {
            throw new ToolException("Missing BAM file for input alignments");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("Gatk command line:" + commandLine);
            try {
                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(commandLine)
                        .setOutputOutputStream(
                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(
                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

                // Check Gatk errors for HaplotypeCaller command
                File outFile = getOutDir().resolve(vcfFilename).toFile();
                if (!isValidFile(outFile)) {
                    File file = new File(getScratchDir() + "/" + STDERR_FILENAME);
                    String msg = "Something wrong executing Gatk";
                    if (file.exists()) {
                        msg = StringUtils.join(FileUtils.readLines(file, Charset.defaultCharset()), ". ");
                    }
                    throw new ToolException(msg);
                }
            } catch (Exception e) {
                throw new ToolException(e);
            }
        });
    }

    @Override
    public String getDockerImageName() {
        return GATK_DOCKER_IMAGE;
    }

    @Override
    public String getCommandLine() throws ToolException {
        StringBuilder sb = new StringBuilder("docker run ");

        // Mount management
        Map<String, String> srcTargetMap = new HashMap<>();
        updateFileMaps(fastaFile, sb, fileUriMap, srcTargetMap);
        updateFileMaps(bamFile, sb, fileUriMap, srcTargetMap);

        sb.append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ");

        // Docker image and version
        sb.append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // BWA command
        sb.append(" gatk ").append(command);

        // Gatk options
        for (String param : params.keySet()) {
            if (checkParam(param)) {
                String value = params.getString(param);
                sb.append(" -").append(param);
                if (StringUtils.isNotEmpty(value)) {
                    sb.append(" ").append(value);
                }
            }
        }

        // HaplotypeCaller command
        File file = new File(fileUriMap.get(fastaFile).getPath());
        sb.append(" -R ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());

        file = new File(fileUriMap.get(bamFile).getPath());
        sb.append(" -I ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());

        if (StringUtils.isEmpty(vcfFilename)) {
            vcfFilename = "out.vcf";
        }
        sb.append(" -O ").append(DOCKER_OUTPUT_PATH).append("/").append(vcfFilename);

        return sb.toString();
    }

    private boolean checkParam(String param) {
        if (param.equals(DOCKER_IMAGE_VERSION_PARAM)) {
            return false;
        } else if ("I".equals(param) || "R".equals(param) || "O".equals(param)) {
            return false;
        }
        return true;
    }


    public String getCommand() {
        return command;
    }

    public GatkWrapperAnalysis setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getFastaFile() {
        return fastaFile;
    }

    public GatkWrapperAnalysis setFastaFile(String fastaFile) {
        this.fastaFile = fastaFile;
        return this;
    }

    public String getBamFile() {
        return bamFile;
    }

    public GatkWrapperAnalysis setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public String getVcfFilename() {
        return vcfFilename;
    }

    public GatkWrapperAnalysis setVcfFilename(String vcfFilename) {
        this.vcfFilename = vcfFilename;
        return this;
    }
}
