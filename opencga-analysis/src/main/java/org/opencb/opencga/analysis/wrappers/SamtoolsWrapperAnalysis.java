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

import com.google.common.base.CaseFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.io.FileUtils.readLines;
import static org.opencb.opencga.core.api.ParamConstants.SAMTOOLS_COMMANDS;
import static org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine.ALIGNMENT_STATS_VARIABLE_SET;

@Tool(id = SamtoolsWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = SamtoolsWrapperAnalysis.DESCRIPTION)
public class SamtoolsWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "samtools";
    public final static String DESCRIPTION = "Samtools is a program for interacting with high-throughput sequencing data in SAM, BAM"
            + " and CRAM formats.";

    public final static String SAMTOOLS_DOCKER_IMAGE = "zlskidmore/samtools";

    public static final String INDEX_STATS_PARAM = "stats-index";

    private String command;
    private String inputFile;
    private String outputFilename;
    private String referenceFile;
    private String readGroupFile;
    private String bedFile;
    private String refSeqFile;
    private String referenceNamesFile;
    private String targetRegionFile;
    private String readsNotSelectedFilename;

    private File outputFile;
    private org.opencb.opencga.core.models.file.File inputCatalogFile;

    protected void check() throws Exception {
        super.check();

        OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult;
        try {
            fileResult = catalogManager.getFileManager().get(getStudy(), inputFile, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error accessing file '" + inputFile + "' of the study " + getStudy() + "'", e);
        }
        if (fileResult.getNumResults() <= 0) {
            throw new ToolException("File '" + inputFile + "' not found in study '" + getStudy() + "'");
        }
        inputCatalogFile = fileResult.getResults().get(0);

        if (StringUtils.isEmpty(command)) {
            throw new ToolException("Missing samtools command. Supported commands are 'sort', 'index' and 'view'");
        }

        switch (command) {
            case "dict":
            case "faidx":
            case "view":
            case "sort":
            case "index":
            case "stats":
            case "depth":
                break;
            default:
                // TODO: support the remaining samtools commands
                throw new ToolException("Samtools command '" + command + "' is not available. Supported commands are "
                        + SAMTOOLS_COMMANDS);
        }

        if (StringUtils.isEmpty(inputFile)) {
            throw new ToolException("Missing input file when executing 'samtools " + command + "'.");
        }

        if (StringUtils.isEmpty(outputFilename)) {
            String prefix = Paths.get(inputCatalogFile.getUri().getPath()).toFile().getName();
            switch (command) {
                case "index": {
                    if (prefix.endsWith("cram")) {
                        outputFilename = prefix + ".crai";
                    } else {
                        outputFilename = prefix + ".bai";
                    }
                    break;
                }
                case "faidx": {
                    outputFilename = prefix + ".fai";
                    break;
                }
                case "dict": {
                    outputFilename = prefix + ".dict";
                    break;
                }
                case "stats": {
                    outputFilename = prefix + ".stats.txt";
                    break;
                }
                case "depth": {
                    outputFilename = prefix + ".depth.txt";
                    break;
                }
                default: {
                    throw new ToolException("Missing output file name when executing 'samtools " + command + "'.");
                }
            }
        }

        // Set output file to use further
        outputFile = getOutDir().resolve(outputFilename).toFile();
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("Samtools command line: " + commandLine);
            try {
                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(getCommandLine())
                        .setOutputOutputStream(
                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(
                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

                // Check samtools errors
                boolean success = false;
                switch (command) {
                    case "dict":
                    case "index":
                    case "sort":
                    case "view": {
                        if (outputFile.exists()) {
                            success = true;

                            if (!"view".equals(command)) {
                                String catalogPath = getCatalogPath(inputFile);
                                File file = new File(fileUriMap.get(inputFile).getPath());

                                Path src = outputFile.toPath();
                                Path dest = new File(file.getParent()).toPath();

                                moveFile(getStudy(), src, dest, catalogPath, token);
                            }
                        }
                        break;
                    }
                    case "faidx": {
                        File file = new File(fileUriMap.get(inputFile).getPath());
                        File faidxFile = file.getParentFile().toPath().resolve(file.getName() + ".fai").toFile();
                        success = isValidFile(faidxFile);
                        if (success) {
                            String catalogPath = getCatalogPath(inputFile);
                            catalogManager.getFileManager().link(getStudy(), faidxFile.toURI(), catalogPath, new ObjectMap("parents", true),
                                    token);
                        }
                        break;
                    }
                    case "stats": {
                        File file = getScratchDir().resolve(STDOUT_FILENAME).toFile();
                        List<String> lines = readLines(file, Charset.defaultCharset());
                        if (lines.size() > 0 && lines.get(0).startsWith("# This file was produced by samtools stats")) {
                            FileUtils.copyFile(file, outputFile);
                            if (params.containsKey(INDEX_STATS_PARAM) && params.getBoolean(INDEX_STATS_PARAM) && !isIndexed()) {
                                SamtoolsStats alignmentStats = parseSamtoolsStats(outputFile);
                                indexStats(alignmentStats);
                            }
                            success = true;
                        }
                        break;
                    }
                    case "depth": {
                        File file = new File(getScratchDir() + "/" + STDOUT_FILENAME);
                        if (file.exists() && file.length() > 0) {
                            FileUtils.copyFile(file, outputFile);
                            success = true;
                        }
                        break;
                    }
                }

                if (!success) {
                    File file = getScratchDir().resolve(STDERR_FILENAME).toFile();
                    String msg = "Something wrong happened when executing Samtools";
                    if (file.exists()) {
                        msg = StringUtils.join(readLines(file, Charset.defaultCharset()), ". ");
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
        return SAMTOOLS_DOCKER_IMAGE;
    }

    @Override
    public String getCommandLine() throws ToolException {
        StringBuilder sb = new StringBuilder("docker run ");

        // Mount management
        Map<String, String> srcTargetMap = new HashMap<>();
        updateFileMaps(inputFile, sb, fileUriMap, srcTargetMap);
        updateFileMaps(referenceFile, sb, fileUriMap, srcTargetMap);
        updateFileMaps(readGroupFile, sb, fileUriMap, srcTargetMap);
        updateFileMaps(bedFile, sb, fileUriMap, srcTargetMap);
        updateFileMaps(referenceNamesFile, sb, fileUriMap, srcTargetMap);
        updateFileMaps(targetRegionFile, sb, fileUriMap, srcTargetMap);
        updateFileMaps(refSeqFile, sb, fileUriMap, srcTargetMap);

        sb.append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ");

        // Docker image and version
        sb.append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // Samtools command
        sb.append(" samtools ").append(command);

        // Samtools options
        for (String param : params.keySet()) {
            if (checkParam(param)) {
                String sep = param.length() == 1 ? " -" : " --";
                String value = params.getString(param);
                if (StringUtils.isEmpty(value)) {
                    sb.append(sep).append(param);
                } else {
                    switch (value.toLowerCase()) {
                        case "false":
                            // Nothing to do
                            break;
                        case "null":
                        case "true":
                            // Only param must be appended
                            sb.append(sep).append(param);
                            break;
                        default:
                            // Otherwise, param + value must be appended
                            sb.append(sep).append(param).append(" ").append(value);
                            break;
                    }
                }
            }
        }

        File file;
        switch (command) {
            case "depth": {
                if (StringUtils.isNotEmpty(referenceFile)) {
                    file = new File(fileUriMap.get(referenceFile).getPath());
                    sb.append(" --reference ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                            .append(file.getName());
                }
                if (StringUtils.isNotEmpty(bedFile)) {
                    file = new File(fileUriMap.get(bedFile).getPath());
                    sb.append(" -b ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                file = new File(fileUriMap.get(inputFile).getPath());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "faidx": {
                file = new File(fileUriMap.get(inputFile).getPath());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "stats": {
                if (StringUtils.isNotEmpty(referenceFile)) {
                    file = new File(fileUriMap.get(referenceFile).getPath());
                    sb.append(" --reference ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                            .append(file.getName());
                }
                if (StringUtils.isNotEmpty(targetRegionFile)) {
                    file = new File(fileUriMap.get(targetRegionFile).getPath());
                    sb.append(" -t ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                if (StringUtils.isNotEmpty(refSeqFile)) {
                    file = new File(fileUriMap.get(refSeqFile).getPath());
                    sb.append(" --ref-seq ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                            .append(file.getName());
                }
                file = new File(fileUriMap.get(inputFile).getPath());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "index": {
                file = new File(fileUriMap.get(inputFile).getPath());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                sb.append(" ").append(DOCKER_OUTPUT_PATH).append("/").append(outputFilename);
                break;
            }
            case "dict": {
                sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/").append(outputFilename);

                file = new File(fileUriMap.get(inputFile).getPath());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "sort": {
                if (StringUtils.isNotEmpty(referenceFile)) {
                    file = new File(fileUriMap.get(referenceFile).getPath());
                    sb.append(" --reference ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                            .append(file.getName());
                }
                sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/").append(outputFilename);

                file = new File(fileUriMap.get(inputFile).getPath());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
            case "view": {
                if (StringUtils.isNotEmpty(referenceFile)) {
                    file = new File(fileUriMap.get(referenceFile).getPath());
                    sb.append(" --reference ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                            .append(file.getName());
                }
                if (StringUtils.isNotEmpty(bedFile)) {
                    file = new File(fileUriMap.get(bedFile).getPath());
                    sb.append(" -L ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                if (StringUtils.isNotEmpty(readGroupFile)) {
                    file = new File(fileUriMap.get(readGroupFile).getPath());
                    sb.append(" -R ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                if (StringUtils.isNotEmpty(readsNotSelectedFilename)) {
                    sb.append(" -U ").append(DOCKER_OUTPUT_PATH).append("/").append(readsNotSelectedFilename);
                }
                if (StringUtils.isNotEmpty(referenceNamesFile)) {
                    file = new File(fileUriMap.get(referenceNamesFile).getPath());
                    sb.append(" -t ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/").append(outputFilename);

                file = new File(fileUriMap.get(inputFile).getPath());
                sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                break;
            }
        }

        return sb.toString();
    }

    private boolean checkParam(String param) {
        if (param.equals(DOCKER_IMAGE_VERSION_PARAM) || param.equals(INDEX_STATS_PARAM)) {
            return false;
        } else {
            switch (command) {
                case "dict": {
                    if ("o".equals(param)) {
                        return false;
                    }
                    break;
                }
                case "view": {
                    switch (param) {
                        case "t":
                        case "L":
                        case "U":
                        case "R":
                        case "T":
                        case "reference":
                        case "o": {
                            return false;
                        }
                    }
                    break;
                }
                case "stats": {
                    switch (param) {
                        case "reference":
                        case "r":
                        case "ref-seq":
                        case "t": {
                            return false;
                        }
                    }
                    break;
                }
                case "sort": {
                    switch (param) {
                        case "reference":
                        case "o": {
                            return false;
                        }
                    }
                    break;
                }
                case "depth": {
                    switch (param) {
                        case "b":
                        case "reference": {
                            return false;
                        }
                    }
                    break;
                }
            }
            return true;
        }
    }

    private SamtoolsStats parseSamtoolsStats(File file) throws IOException, ToolException, CatalogException {
        // Create a map with the summary numbers of the statistics (samtools stats)
        Map<String, Object> map = new HashMap<>();

        int count = 0;
        for (String line : readLines(file, Charset.defaultCharset())) {
            // Only take into account the "SN" section (summary numbers)
            if (line.startsWith("SN")) {
                count++;
                String[] splits = line.split("\t");
                String key = splits[1].replace("(cigar)", "cigar").split("\\(")[0].trim().replace("1st", "first").replace(":", "")
                        .replace(" ", "_").replace("-", "_");
                key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key);
                String value = splits[2].split(" ")[0];
                map.put(key, value);
            } else if (count > 0) {
                // SN (summary numbers) section has been processed
                break;
            }
        }

        // Convert map to AlignmentStats
        SamtoolsStats alignmentStats = JacksonUtils.getDefaultObjectMapper().convertValue(map, SamtoolsStats.class);

        // Set file ID
        alignmentStats.setFileId(inputCatalogFile.getId());

        return alignmentStats;
    }

    private void indexStats(SamtoolsStats alignmentStats) throws CatalogException, IOException {
        // Convert AlignmentStats to map in order to create an AnnotationSet
        Map<String, Object> annotations = JacksonUtils.getDefaultObjectMapper().convertValue(alignmentStats, Map.class);
        AnnotationSet annotationSet = new AnnotationSet(ALIGNMENT_STATS_VARIABLE_SET, ALIGNMENT_STATS_VARIABLE_SET, annotations);

        // Update catalog
        FileUpdateParams updateParams = new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));
        catalogManager.getFileManager().update(getStudy(), inputCatalogFile.getId(), updateParams, QueryOptions.empty(), token);
    }

    private boolean isIndexed() {
        OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult;
        try {
            fileResult = catalogManager.getFileManager().get(getStudy(), inputCatalogFile.getId(), QueryOptions.empty(), token);

            if (fileResult.getNumResults() == 1) {
                for (AnnotationSet annotationSet : fileResult.getResults().get(0).getAnnotationSets()) {
                    if (ALIGNMENT_STATS_VARIABLE_SET.equals(annotationSet.getId())) {
                        return true;
                    }
                }
            }
        } catch (CatalogException e) {
            return false;
        }

        return false;
    }

    public String getCommand() {
        return command;
    }

    public SamtoolsWrapperAnalysis setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getInputFile() {
        return inputFile;
    }

    public SamtoolsWrapperAnalysis setInputFile(String inputFile) {
        this.inputFile = inputFile;
        return this;
    }

    public String getOutputFilename() {
        return outputFilename;
    }

    public SamtoolsWrapperAnalysis setOutputFilename(String outputFilename) {
        this.outputFilename = outputFilename;
        return this;
    }

    public String getReferenceFile() {
        return referenceFile;
    }

    public SamtoolsWrapperAnalysis setReferenceFile(String referenceFile) {
        this.referenceFile = referenceFile;
        return this;
    }

    public String getReadGroupFile() {
        return readGroupFile;
    }

    public SamtoolsWrapperAnalysis setReadGroupFile(String readGroupFile) {
        this.readGroupFile = readGroupFile;
        return this;
    }

    public String getBedFile() {
        return bedFile;
    }

    public SamtoolsWrapperAnalysis setBedFile(String bedFile) {
        this.bedFile = bedFile;
        return this;
    }

    public String getReferenceNamesFile() {
        return referenceNamesFile;
    }

    public SamtoolsWrapperAnalysis setReferenceNamesFile(String referenceNamesFile) {
        this.referenceNamesFile = referenceNamesFile;
        return this;
    }

    public String getTargetRegionFile() {
        return targetRegionFile;
    }

    public SamtoolsWrapperAnalysis setTargetRegionFile(String targetRegionFile) {
        this.targetRegionFile = targetRegionFile;
        return this;
    }

    public String getRefSeqFile() {
        return refSeqFile;
    }

    public SamtoolsWrapperAnalysis setRefSeqFile(String refSeqFile) {
        this.refSeqFile = refSeqFile;
        return this;
    }

    public String getReadsNotSelectedFilename() {
        return readsNotSelectedFilename;
    }

    public SamtoolsWrapperAnalysis setReadsNotSelectedFilename(String readsNotSelectedFilename) {
        this.readsNotSelectedFilename = readsNotSelectedFilename;
        return this;
    }
}
