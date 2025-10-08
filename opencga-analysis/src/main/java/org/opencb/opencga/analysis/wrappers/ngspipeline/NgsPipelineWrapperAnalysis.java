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

package org.opencb.opencga.analysis.wrappers.ngspipeline;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.NgsPipelineWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.wrappers.ngspipeline.NgsPipelineWrapperAnalysisExecutor.RUN_CMD;
import static org.opencb.opencga.analysis.wrappers.ngspipeline.NgsPipelineWrapperAnalysisExecutor.PREPARE_CMD;
import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;

@Tool(id = NgsPipelineWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = NgsPipelineWrapperAnalysis.DESCRIPTION)
public class NgsPipelineWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "ngs-pipeline";
    public static final String DESCRIPTION = "NGS pipeline that performs QC (e.g.: FastQC), mapping (e.g.: BWA),"
            + " variant calling (e.g., GATK) and variant indexing in OpenCGA storage.";

    private static final String PREPARE_RESOURCES_STEP = "prepare-resources";
    private static final String INDEX_VARIARIANTS_STEP = "index-variants";

    private String referenceUrl = null;
    private List<File> opencgaFiles = new ArrayList<>();
    private File indexPath = null;
    private File vcfFile = null;

    @ToolParams
    protected final NgsPipelineWrapperParams analysisParams = new NgsPipelineWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check command
        if (StringUtils.isEmpty(analysisParams.getCommand())) {
            throw new ToolException("NGS pipeline command parameter is mandatory.");
        }
        if (!analysisParams.getCommand().equalsIgnoreCase(PREPARE_CMD) && !analysisParams.getCommand().equalsIgnoreCase(RUN_CMD)) {
            throw new ToolException("NGS pipeline command '" + analysisParams.getCommand() + "' is not valid. Supported commands"
                    + " are: '" + PREPARE_CMD + "' and '"
                    + RUN_CMD + "'.");
        }

        // Check input files
        if (CollectionUtils.isEmpty(analysisParams.getInput())) {
            throw new ToolException("NGS pipeline input parameter is mandatory.");
        }
        if (analysisParams.getCommand().equalsIgnoreCase(PREPARE_CMD)) {
            // In prepare command, only one input file is required and it can be a URL to the reference FASTA file to download
            // or a OpencGA file ID
            if (analysisParams.getInput().size() != 1) {
                throw new ToolException("NGS pipeline prepare command requires one and only one input file; got "
                        + analysisParams.getInput().size());
            }
            if (analysisParams.getInput().get(0).startsWith("http://")
                    || analysisParams.getInput().get(0).startsWith("https://")
                    || analysisParams.getInput().get(0).startsWith("ftp://")) {
                referenceUrl = analysisParams.getInput().get(0);
            } else {
                logger.info("Checking reference file {}", analysisParams.getInput().get(0));
                opencgaFiles.add(getCatalogManager().getFileManager().get(study, analysisParams.getInput().get(0), QueryOptions.empty(),
                        token).first());
            }
        } else {
            // In pipeline command, input files must be OpencGA file IDs
            for (String file : analysisParams.getInput()) {
                logger.info("Checking file {}", file);
                File opencgaFile = getCatalogManager().getFileManager().get(study, file, QueryOptions.empty(), token).first();
                if (opencgaFile.getType() != File.Type.FILE) {
                    throw new ToolException("NGS pipeline input path '" + file + "' is not a file.");
                }
                opencgaFiles.add(opencgaFile);
            }

            // Check the index path
            if (StringUtils.isEmpty(analysisParams.getIndexDir())) {
                throw new ToolException("NGS pipeline index path is mandatory for running the pipeline.");
            }
            logger.info("Checking index path {}", analysisParams.getIndexDir());
            indexPath = getCatalogManager().getFileManager().get(study, analysisParams.getIndexDir(), QueryOptions.empty(), token).first();
            if (indexPath.getType() != File.Type.DIRECTORY) {
                throw new ToolException("NGS pipeline index path '" + analysisParams.getIndexDir() + "' is not a folder.");
            }

            // Check pipeline parameters
            if (MapUtils.isEmpty(analysisParams.getPipelineParams())) {
                throw new ToolException("NGS pipeline parameters are mandatory.");
            }
        }
    }

    @Override
    protected List<String> getSteps() {
        if (PREPARE_CMD.equalsIgnoreCase(analysisParams.getCommand())) {
            return Arrays.asList(PREPARE_RESOURCES_STEP);
        } else {
            return Arrays.asList(ID, INDEX_VARIARIANTS_STEP);
        }
    }

    protected void run() throws ToolException, IOException {
        if (PREPARE_CMD.equalsIgnoreCase(analysisParams.getCommand())) {
            // Pipeline preparation
            step(PREPARE_RESOURCES_STEP, this::prepareResources);
        } else {
            // Run NGS pipeline script
            step(ID, this::runNgsPipeline);

            // Index variants in OpenCGA
            step(INDEX_VARIARIANTS_STEP, this::indexVariants);
        }
    }

    protected void prepareResources() throws IOException, ResourceException, ToolException {
        // Get executor
        NgsPipelineWrapperAnalysisExecutor executor = getToolExecutor(NgsPipelineWrapperAnalysisExecutor.class);

        List<String> input = new ArrayList<>();
        if (referenceUrl != null) {
            input.add(referenceUrl);
        } else {
            input.add(opencgaFiles.get(0).getUri().getPath());
        }

        // Set parameters and execute
        executor.setStudy(study)
                .setScriptPath(getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(ID))
                .setCommand(PREPARE_CMD)
                .setInput(input)
                .setPrepareIndices(analysisParams.getPrepareIndices())
                .execute();

        // TODO: check output?
    }

    protected void runNgsPipeline() throws ToolException, CatalogException {
        // Get executor
        NgsPipelineWrapperAnalysisExecutor executor = getToolExecutor(NgsPipelineWrapperAnalysisExecutor.class);

        // Get physical files from OpenCGA files
        List<String> input = opencgaFiles.stream().map(f -> Paths.get(f.getUri().getPath()).toAbsolutePath().toString())
                .collect(Collectors.toList());

        // Set parameters and execute
        executor.setStudy(study)
                .setScriptPath(getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(ID))
                .setCommand(RUN_CMD)
                .setInput(input)
                .setIndexPath(indexPath.getUri().getPath())
                .setPipelineParams(analysisParams.getPipelineParams())
                .execute();

        // TODO: check the VCF output file is generated
    }

    protected void indexVariants() throws CatalogException, StorageEngineException, ToolException {
        // Remove the extension .gz if exists in the first OpenCGA filename
        String filename = opencgaFiles.get(0).getName();
        if (filename.toLowerCase().endsWith(".gz")) {
            filename = filename.substring(0, filename.length() - 3);
        }

        Path vcfPath = getOutDir().resolve("variant-calling").resolve(filename + ".sorted.gatk.vcf").toAbsolutePath();
        if (!Files.exists(vcfPath)) {
            throw new ToolException("Could not find the VCF: " + vcfPath);
        }
        vcfFile = catalogManager.getFileManager().link(study, new FileLinkParams(vcfPath.toAbsolutePath().toString(),
                "", "", "", null, null, null, null, null), false, token).first();

        ObjectMap storageOptions = new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        getVariantStorageManager().index(study, vcfFile.getId(), getScratchDir().toAbsolutePath().toString(), storageOptions, token);
    }


//    private void linkOutFile(String outFilename, File inputFile) throws CatalogException, ToolException {
//        Path parentPath = null;
//        String opencgaPath = null;
//
//
//        // OpenCGA catalog link, if the output file exists
//        Path outFile = parentPath.resolve(outFilename);
//        if (Files.exists(outFile)) {
//            URI uri = outFile.toUri();
//            StopWatch stopWatch = StopWatch.createStarted();
//            FileLinkParams linkParams = new FileLinkParams().setUri(uri.toString());
//            if (opencgaPath != null) {
//                linkParams.setPath(opencgaPath);
//            }
//            logger.info("Linking file, uri =  {}; OpenCGA path = {}", uri, opencgaPath);
//            // Link 'parents' to true to ensure the directory is created
//            OpenCGAResult<File> result = catalogManager.getFileManager().link(getStudy(), linkParams, true, getToken());
//            if (result.getEvents().stream().anyMatch(e -> e.getMessage().equals(ParamConstants.FILE_ALREADY_LINKED))) {
//                logger.info("File already linked - SKIP");
//            } else {
//                String duration = TimeUtils.durationToString(stopWatch);
//                logger.info("File link took {}", duration);
//                File file = result.first();
//                addGeneratedFile(file);
//            }
//        } else {
//            logger.warn("Something wrong happened, exptected output file {} does not exit", outFile.toAbsolutePath());
//        }
//    }
//
//    public static String getLiftoverFilename(String inputVcfFilename, String assembly) {
//        return basename(inputVcfFilename) + "." + assembly + ".liftover.vcf.gz";
//    }
//
//    public static String getLiftoverRejectedFilename(String inputVcfFilename, String assembly) {
//        return basename(inputVcfFilename) + "." + assembly + ".liftover.rejected.vcf";
//    }
//
//    private static String basename(String inputVcfFilename) {
//        if (inputVcfFilename.endsWith(".vcf.gz")) {
//            return inputVcfFilename.replace(".vcf.gz", "");
//        } else if (inputVcfFilename.endsWith(".vcf")) {
//            return inputVcfFilename.replace(".vcf", "");
//        } else {
//            throw new IllegalArgumentException("File " + inputVcfFilename + " must end with .vcf or .vcf.gz");
//        }
//    }
//
//    public static void deleteDirectory(java.io.File directory) throws IOException {
//        if (!directory.exists()) {
//            return;
//        }
//
//        // If it's a directory, delete contents recursively
//        if (directory.isDirectory()) {
//            java.io.File[] files = directory.listFiles();
//            if (files != null) { // Not null if directory is not empty
//                for (java.io.File file : files) {
//                    // Recursively delete subdirectories and files
//                    deleteDirectory(file);
//                }
//            }
//        }
//
//        // Finally, delete the directory or file itself
//        Files.delete(directory.toPath());
//    }
}
