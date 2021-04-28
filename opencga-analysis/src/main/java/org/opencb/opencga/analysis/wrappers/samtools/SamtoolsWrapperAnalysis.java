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

package org.opencb.opencga.analysis.wrappers.samtools;

import com.google.common.base.CaseFormat;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.SamtoolsWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.SAMTOOLS_COMMANDS_SUPPORTED;
import static org.opencb.opencga.core.api.ParamConstants.SAMTOOLS_COMMAND_DESCRIPTION;

@Tool(id = SamtoolsWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = SamtoolsWrapperAnalysis.DESCRIPTION)
public class SamtoolsWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "samtools";
    public final static String DESCRIPTION = "Samtools is a program for interacting with high-throughput sequencing data in SAM, BAM"
            + " and CRAM formats. " + SAMTOOLS_COMMAND_DESCRIPTION;

    @ToolParams
    protected final SamtoolsWrapperParams analysisParams = new SamtoolsWrapperParams();

    private String bamFilePath = null;
    private String bedFilePath = null;
    private String readGroupFilePath = null;
    private String readsNotSelectedFilenamePath = null;
    private String referenceFilePath = null;
    private String referenceNamesFilePath = null;
    private String refSeqFilePath = null;

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(analysisParams.getCommand())) {
            throw new ToolException("Missing samtools command.");
        }

        if (!AnalysisUtils.isSupportedCommand(SAMTOOLS_COMMANDS_SUPPORTED)) {
            throw new ToolException("Samtools command '" + analysisParams.getCommand() + "' is not available. Supported commands are "
                    + SAMTOOLS_COMMANDS_SUPPORTED);
        }

        // Get files from catalog
        FileManager fileManager = catalogManager.getFileManager();
        if (StringUtils.isNotEmpty(analysisParams.getInputFile())) {
            bamFilePath = AnalysisUtils.getCatalogFile(analysisParams.getInputFile(), study, fileManager, token).getUri().getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getBedFile())) {
            bedFilePath = AnalysisUtils.getCatalogFile(analysisParams.getBedFile(), study, fileManager, token).getUri().getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getReadGroupFile())) {
            readGroupFilePath = AnalysisUtils.getCatalogFile(analysisParams.getReadGroupFile(), study, fileManager, token).getUri()
                    .getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getReadsNotSelectedFilename())) {
            readsNotSelectedFilenamePath = AnalysisUtils.getCatalogFile(analysisParams.getReadsNotSelectedFilename(), study, fileManager,
                    token).getUri().getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getReferenceFile())) {
            referenceFilePath = AnalysisUtils.getCatalogFile(analysisParams.getReferenceFile(), study, fileManager, token)
                    .getUri().getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getReferenceNamesFile())) {
            referenceNamesFilePath = AnalysisUtils.getCatalogFile(analysisParams.getReferenceNamesFile(), study, fileManager, token)
                    .getUri().getPath();
        }
        if (StringUtils.isNotEmpty(analysisParams.getRefSeqFile())) {
            refSeqFilePath = AnalysisUtils.getCatalogFile(analysisParams.getRefSeqFile(), study, fileManager, token).getUri().getPath();
        }
    }

    @Override
    protected void run() throws Exception {
        setUpStorageEngineExecutor(study);

        step(() -> {
            if (MapUtils.isNotEmpty(analysisParams.getSamtoolsParams())) {
                executorParams.appendAll(analysisParams.getSamtoolsParams());
            }

            getToolExecutor(SamtoolsWrapperAnalysisExecutor.class)
                    .setCommand(analysisParams.getCommand())
                    .setInputFile(bamFilePath)
                    .setBedFile(bedFilePath)
                    .setReadGroupFile(readGroupFilePath)
                    .setReadsNotSelectedFilename(readsNotSelectedFilenamePath)
                    .setReferenceFile(referenceFilePath)
                    .setReferenceNamesFile(referenceNamesFilePath)
                    .setRefSeqFile(refSeqFilePath)
                    .execute();
        });
    }


    public static SamtoolsStats parseSamtoolsStats(java.io.File file, String fileId) throws IOException {
        // Create a map with the summary numbers of the statistics (samtools stats)
        Map<String, Object> map = new HashMap<>();

        int count = 0;
        for (String line : FileUtils.readLines(file, Charset.defaultCharset())) {
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
        alignmentStats.setFileId(fileId);

        return alignmentStats;
    }
//
//    private void indexStats(SamtoolsStats alignmentStats) throws CatalogException, IOException {
//        // Convert AlignmentStats to map in order to create an AnnotationSet
//        Map<String, Object> annotations = JacksonUtils.getDefaultObjectMapper().convertValue(alignmentStats, Map.class);
//        AnnotationSet annotationSet = new AnnotationSet(ALIGNMENT_STATS_VARIABLE_SET, ALIGNMENT_STATS_VARIABLE_SET, annotations);
//
//        // Update catalog
//        FileUpdateParams updateParams = new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));
//        catalogManager.getFileManager().update(getStudy(), inputCatalogFile.getId(), updateParams, QueryOptions.empty(), token);
//    }
//
////    private boolean isIndexed() {
////        OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult;
////        try {
////            fileResult = catalogManager.getFileManager().get(getStudy(), inputCatalogFile.getId(), QueryOptions.empty(), token);
////
////            if (fileResult.getNumResults() == 1) {
////                for (AnnotationSet annotationSet : fileResult.getResults().get(0).getAnnotationSets()) {
////                    if (ALIGNMENT_STATS_VARIABLE_SET.equals(annotationSet.getId())) {
////                        return true;
////                    }
////                }
////            }
////        } catch (CatalogException e) {
////            return false;
////        }
////
////        return false;
////    }
//
//    public String getCommand() {
//        return command;
//    }
//
//    public SamtoolsWrapperAnalysis setCommand(String command) {
//        this.command = command;
//        return this;
//    }
//
//    public String getInputFile() {
//        return inputFile;
//    }
//
//    public SamtoolsWrapperAnalysis setInputFile(String inputFile) {
//        this.inputFile = inputFile;
//        return this;
//    }
//
//    public String getOutputFilename() {
//        return outputFilename;
//    }
//
//    public SamtoolsWrapperAnalysis setOutputFilename(String outputFilename) {
//        this.outputFilename = outputFilename;
//        return this;
//    }
//
//    public String getReferenceFile() {
//        return referenceFile;
//    }
//
//    public SamtoolsWrapperAnalysis setReferenceFile(String referenceFile) {
//        this.referenceFile = referenceFile;
//        return this;
//    }
//
//    public String getReadGroupFile() {
//        return readGroupFile;
//    }
//
//    public SamtoolsWrapperAnalysis setReadGroupFile(String readGroupFile) {
//        this.readGroupFile = readGroupFile;
//        return this;
//    }
//
//    public String getBedFile() {
//        return bedFile;
//    }
//
//    public SamtoolsWrapperAnalysis setBedFile(String bedFile) {
//        this.bedFile = bedFile;
//        return this;
//    }
//
//    public String getReferenceNamesFile() {
//        return referenceNamesFile;
//    }
//
//    public SamtoolsWrapperAnalysis setReferenceNamesFile(String referenceNamesFile) {
//        this.referenceNamesFile = referenceNamesFile;
//        return this;
//    }
//
//    public String getTargetRegionFile() {
//        return targetRegionFile;
//    }
//
//    public SamtoolsWrapperAnalysis setTargetRegionFile(String targetRegionFile) {
//        this.targetRegionFile = targetRegionFile;
//        return this;
//    }
//
//    public String getRefSeqFile() {
//        return refSeqFile;
//    }
//
//    public SamtoolsWrapperAnalysis setRefSeqFile(String refSeqFile) {
//        this.refSeqFile = refSeqFile;
//        return this;
//    }
//
//    public String getReadsNotSelectedFilename() {
//        return readsNotSelectedFilename;
//    }
//
//    public SamtoolsWrapperAnalysis setReadsNotSelectedFilename(String readsNotSelectedFilename) {
//        this.readsNotSelectedFilename = readsNotSelectedFilename;
//        return this;
//    }
}
