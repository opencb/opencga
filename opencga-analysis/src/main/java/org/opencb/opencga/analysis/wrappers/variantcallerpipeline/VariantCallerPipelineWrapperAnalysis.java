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

package org.opencb.opencga.analysis.wrappers.variantcallerpipeline;


import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.variant.VariantCallerPipelineWrapperParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.tools.ResourceManager.RESOURCES_DIRNAME;

@Tool(id = VariantCallerPipelineWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = VariantCallerPipelineWrapperAnalysis.DESCRIPTION)
public class VariantCallerPipelineWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "variant-caller-pipeline";
    public static final String DESCRIPTION = "Variant caller pipeline that performs QC (e.g.: FastQC), mapping (e.g.: BWA),"
            + " variant calling (e.g., GATK) and variant indexing in OpenCGA storage.";

    private static final String PREPARE_RESOURCES_STEP = "prepare-resources";
    private static final String INDEX_VARIARIANTS_STEP = "index-variants";
    private static final String ANNOTATE_VARIANTS_STEP = "annotate-variants";
    private static final String INDEX_SECONDARY_ANNOTATION = "index-secondary-annotation";
    private static final String CLEAN_RESOURCES_STEP = "clean-resources";

    private List<File> opencgaFiles = new ArrayList<>();
    private Path resourcePath;

    private org.opencb.opencga.catalog.utils.ResourceManager resourceManager = null;
    private List<String> variantCallerPipelineResourceIds = null;

    @ToolParams
    protected final VariantCallerPipelineWrapperParams analysisParams = new VariantCallerPipelineWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

//        if (CollectionUtils.isEmpty(analysisParams.getFiles())) {
//            throw new ToolException("Variant caller pipeline 'files' parameter is mandatory.");
//        }

        // Check files
//        for (String file : analysisParams.getFiles()) {
//            logger.info("Checking file {}", file);
//            opencgaFiles.add(getCatalogManager().getFileManager().get(study, file, QueryOptions.empty(), token).first());
//        }

        // Check resources
        resourceManager = new org.opencb.opencga.catalog.utils.ResourceManager(getOpencgaHome());
        for (String variantCallerPipelineResourceId : variantCallerPipelineResourceIds) {
            resourceManager.checkResourcePath(variantCallerPipelineResourceId);
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(PREPARE_RESOURCES_STEP, ID, INDEX_VARIARIANTS_STEP, ANNOTATE_VARIANTS_STEP, INDEX_SECONDARY_ANNOTATION,
                CLEAN_RESOURCES_STEP);
    }

    protected void run() throws ToolException, IOException {
        // Download and copy liftover resource files in the job dir
        step(PREPARE_RESOURCES_STEP, this::prepareResources);

        // Run variant caller pipeline script
        step(ID, this::runVariantCallerPipeline);

        // Index variants in OpenCGA
        step(INDEX_VARIARIANTS_STEP, this::indexVariants);

        // Annotate variants
        step(ANNOTATE_VARIANTS_STEP, this::annotateVariants);

        // Index secondary annotation
        step(INDEX_SECONDARY_ANNOTATION, this::indexSecondaryAnnotation);

        // Do we have to clean the liftover resource folder
        step(CLEAN_RESOURCES_STEP, this::cleanResources);
    }

    protected void prepareResources() throws IOException, ResourceException {
        // Create folder where the liftover resources will be saved (within the job dir, aka outdir)
        resourcePath = Files.createDirectories(getOutDir().resolve(RESOURCES_DIRNAME));

        // Create resources from the installation folder
        for (String variantCallerPipelineResourceId : variantCallerPipelineResourceIds) {
            Path installPath = resourceManager.checkResourcePath(variantCallerPipelineResourceId);
            FileUtils.copyFile(installPath.toFile(), resourcePath.resolve(installPath.getFileName()).toFile());
        }
    }

    private void cleanResources() throws IOException {
        deleteDirectory(resourcePath.toFile());
    }

    protected void runVariantCallerPipeline() throws ToolException, CatalogException {
        // Get executor
        VariantCallerPipelineWrapperAnalysisExecutor executor = getToolExecutor(VariantCallerPipelineWrapperAnalysisExecutor.class);

        // Get physical files from OpenCGA files
        List<java.io.File> files = opencgaFiles.stream().map(f -> Paths.get(f.getUri().getPath()).toFile()).collect(Collectors.toList());

        // Set parameters and execute
        executor.setStudy(study)
//                .setLiftoverPath(getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(ID))
//                .setFiles(files)
//                .setTargetAssembly(targetAssembly)
//                .setVcfDest(vcfDest)
//                .setResourcePath(resourcePath)
                .execute();

        // If vcfDest is null, Liftover (and rejected) VCF files are NOT stored in the job dir (therefore they are not linked automatically
        // by the daemon), so they have to be linked to OpenCGA catalog
//        if (!StringUtils.isEmpty(vcfDest)) {
//            for (File opencgaFile : opencgaFiles) {
//                // Link Liftover and rejected VCF files
//                linkOutFile(getLiftoverFilename(opencgaFile.getName(), targetAssembly), opencgaFile);
//                linkOutFile(getLiftoverRejectedFilename(opencgaFile.getName(), targetAssembly), opencgaFile);
//            }
//        }
    }

    protected void indexVariants() throws ToolException, CatalogException {
    }

    protected void annotateVariants() throws ToolException, CatalogException {
    }

    protected void indexSecondaryAnnotation() throws ToolException, CatalogException {
    }

    private void linkOutFile(String outFilename, File inputFile) throws CatalogException, ToolException {
        Path parentPath = null;
        String opencgaPath = null;

//        if (SAME_AS_INPUT_VCF.equals(vcfDest)) {
//            parentPath = Paths.get(inputFile.getUri().getPath()).getParent();
//            if (StringUtils.isNotEmpty(inputFile.getPath())) {
//                opencgaPath = Paths.get(inputFile.getPath()).getParent().resolve(outFilename).toString();
//            }
//        } else {
//            parentPath = Paths.get(vcfDest);
//        }

        // OpenCGA catalog link, if the output file exists
        Path outFile = parentPath.resolve(outFilename);
        if (Files.exists(outFile)) {
            URI uri = outFile.toUri();
            StopWatch stopWatch = StopWatch.createStarted();
            FileLinkParams linkParams = new FileLinkParams().setUri(uri.toString());
            if (opencgaPath != null) {
                linkParams.setPath(opencgaPath);
            }
            logger.info("Linking file, uri =  {}; OpenCGA path = {}", uri, opencgaPath);
            // Link 'parents' to true to ensure the directory is created
            OpenCGAResult<File> result = catalogManager.getFileManager().link(getStudy(), linkParams, true, getToken());
            if (result.getEvents().stream().anyMatch(e -> e.getMessage().equals(ParamConstants.FILE_ALREADY_LINKED))) {
                logger.info("File already linked - SKIP");
            } else {
                String duration = TimeUtils.durationToString(stopWatch);
                logger.info("File link took {}", duration);
                File file = result.first();
                addGeneratedFile(file);
            }
        } else {
            logger.warn("Something wrong happened, exptected output file {} does not exit", outFile.toAbsolutePath());
        }
    }

    public static String getLiftoverFilename(String inputVcfFilename, String assembly) {
        return basename(inputVcfFilename) + "." + assembly + ".liftover.vcf.gz";
    }

    public static String getLiftoverRejectedFilename(String inputVcfFilename, String assembly) {
        return basename(inputVcfFilename) + "." + assembly + ".liftover.rejected.vcf";
    }

    private static String basename(String inputVcfFilename) {
        if (inputVcfFilename.endsWith(".vcf.gz")) {
            return inputVcfFilename.replace(".vcf.gz", "");
        } else if (inputVcfFilename.endsWith(".vcf")) {
            return inputVcfFilename.replace(".vcf", "");
        } else {
            throw new IllegalArgumentException("File " + inputVcfFilename + " must end with .vcf or .vcf.gz");
        }
    }

    public static void deleteDirectory(java.io.File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }

        // If it's a directory, delete contents recursively
        if (directory.isDirectory()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) { // Not null if directory is not empty
                for (java.io.File file : files) {
                    // Recursively delete subdirectories and files
                    deleteDirectory(file);
                }
            }
        }

        // Finally, delete the directory or file itself
        Files.delete(directory.toPath());
    }
}
