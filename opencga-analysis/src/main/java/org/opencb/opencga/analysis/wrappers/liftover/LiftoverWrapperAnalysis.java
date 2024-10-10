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

package org.opencb.opencga.analysis.wrappers.liftover;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.variant.LiftoverWrapperParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.ResourceManager;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.api.FieldConstants.*;
import static org.opencb.opencga.core.tools.ResourceManager.ANALYSIS_DIRNAME;
import static org.opencb.opencga.core.tools.ResourceManager.RESOURCES_DIRNAME;

@Tool(id = LiftoverWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = LiftoverWrapperAnalysis.DESCRIPTION)
public class LiftoverWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "liftover";
    public static final String DESCRIPTION = "BCFtools liftover plugin maps coordinates from assembly 37 to 38.";

    private static final String PREPARE_RESOURCES_STEP = "prepare-resources";
    private static final String CLEAN_RESOURCES_STEP = "clean-resources";
    private static final String VALID_TARGET_ASSEMBLIES = "valid options are '" + LIFTOVER_GRCH38 + "' and '" + LIFTOVER_HG38 + "'";

    private List<File> opencgaFiles = new ArrayList<>();
    private String targetAssembly;
    private String vcfDest;
    private Path resourcePath;

    @ToolParams
    protected final LiftoverWrapperParams analysisParams = new LiftoverWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        if (CollectionUtils.isEmpty(analysisParams.getFiles())) {
            throw new ToolException("Liftover 'files' parameter is mandatory.");
        }

        // Check files
        for (String file : analysisParams.getFiles()) {
            opencgaFiles.add(getCatalogManager().getFileManager().get(study, file, QueryOptions.empty(), token).first());
        }

        // Check target assembly
        if (StringUtils.isEmpty(analysisParams.getTargetAssembly())) {
            throw new ToolException("Liftover 'targetAssembly' parameter is mandatory, " + VALID_TARGET_ASSEMBLIES);
        }
        if (LIFTOVER_GRCH38.equalsIgnoreCase(analysisParams.getTargetAssembly())) {
            targetAssembly = LIFTOVER_GRCH38;
        }
        if (LIFTOVER_HG38.equalsIgnoreCase(analysisParams.getTargetAssembly())) {
            targetAssembly = LIFTOVER_HG38;
        }
        if (!LIFTOVER_GRCH38.equals(targetAssembly) && !LIFTOVER_HG38.equals(targetAssembly)) {
            throw new ToolException("Unknown Liftover 'targetAssembly' parameter ('" + analysisParams.getTargetAssembly() + "'), "
                    + VALID_TARGET_ASSEMBLIES);
        }

        // Check destination
        vcfDest = analysisParams.getVcfDestination();
        if (StringUtils.isEmpty(vcfDest)) {
            logger.info("Liftover 'vcfDestination' parameter is empty, the resultant VCF files will be stored in the job directory: {}",
                    getOutDir());
        } else if (!LIFTOVER_VCF_INPUT_FOLDER.equals(vcfDest)) {
            File opencgaFile = getCatalogManager().getFileManager().get(study, analysisParams.getVcfDestination(), QueryOptions.empty(),
                    token).first();
            Path path = Paths.get(opencgaFile.getUri().getPath()).toAbsolutePath();
            if (!Files.exists(path)) {
                getCatalogManager().getIoManagerFactory().get(path.toUri()).createDirectory(path.toUri(), true);
            }
            if (!Files.exists(path)) {
                throw new ToolException("Liftover 'vcfDestination' parameter (" + analysisParams.getVcfDestination() + ") but folder ("
                        + path + ") does not exist");
            }
            vcfDest = path.toString();
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(PREPARE_RESOURCES_STEP, ID, CLEAN_RESOURCES_STEP);
    }

    protected void run() throws ToolException, IOException {
        // Download and copy liftover resource files in the job dir
        step(PREPARE_RESOURCES_STEP, this::prepareResources);

        // Run liftover script
        step(ID, this::runLiftover);

        // Do we have to clean the liftover resource folder
        step(CLEAN_RESOURCES_STEP, this::cleanResources);
    }

    protected void prepareResources() throws IOException, ToolException {
        // Create folder where the liftover resources will be saved (within the job dir, aka outdir)
        resourcePath = Files.createDirectories(getOutDir().resolve(RESOURCES_DIRNAME));

        // Identify Liftover resources to download only the required ones
        Map<String, List<String>> mapResources = new HashMap<>();
        switch (targetAssembly) {
            case LIFTOVER_GRCH38: {
                mapResources.put("reference-genome", Arrays.asList("Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz",
                        "Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"));
                break;
            }
            case LIFTOVER_HG38: {
                mapResources.put("reference-genome", Arrays.asList("hg19.fa.gz", "hg38.fa.gz"));
                break;
            }
            default: {
                throw new ToolException("Unknown Liftover 'targetAssembly' parameter ('" + analysisParams.getTargetAssembly() + "'), "
                        + VALID_TARGET_ASSEMBLIES);
            }
        }

        // Download resources and copy them to the job dir
        // (this URL is temporary, it should be replaced by the resourceUrl from configuration file)
        ResourceManager resourceManager = new ResourceManager(getOpencgaHome(), "http://resources.opencb.org/task-6766/");
        for (Map.Entry<String, List<String>> entry : mapResources.entrySet()) {
            for (String resourceName : entry.getValue()) {
                java.io.File resourceFile = resourceManager.getResourceFile(entry.getKey(), resourceName);
                Files.copy(resourceFile.toPath(), resourcePath.resolve(resourceFile.getName()));
            }
        }
    }

    private void cleanResources() throws IOException {
        deleteDirectory(resourcePath.toFile());
    }

    protected void runLiftover() throws ToolException, CatalogException {
        // Get executor
        LiftoverWrapperAnalysisExecutor executor = getToolExecutor(LiftoverWrapperAnalysisExecutor.class);

        // Get physical files from OpenCGA files
        List<java.io.File> files = opencgaFiles.stream().map(f -> Paths.get(f.getUri().getPath()).toFile()).collect(Collectors.toList());

        // Set parameters and execute
        executor.setStudy(study)
                .setLiftoverPath(getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(ID))
                .setFiles(files)
                .setTargetAssembly(targetAssembly)
                .setVcfDest(vcfDest)
                .setResourcePath(resourcePath)
                .execute();

        // If vcfDest is null, Liftover (and rejected) VCF files are NOT stored in the job dir (therefor they are not linked automatically
        // by the daemon), so they have to be linked to OpenCGA catalog
        if (!StringUtils.isEmpty(vcfDest)) {
            for (File opencgaFile : opencgaFiles) {
                Path parentPath;
                if (LIFTOVER_VCF_INPUT_FOLDER.equals(vcfDest)) {
                    parentPath = Paths.get(opencgaFile.getUri().getPath()).getParent();
                } else {
                    parentPath = Paths.get(vcfDest);
                }
                // Link Liftover and rejected VCF files, if they exist
                linkOutFile(parentPath.resolve(getLiftoverFilename(opencgaFile.getName(), targetAssembly)));
                linkOutFile(parentPath.resolve(getLiftoverRejectedFilename(opencgaFile.getName(), targetAssembly)));
            }
        }
    }

    private void linkOutFile(Path outFile) throws CatalogException, ToolException {
        if (Files.exists(outFile)) {
            URI uri = outFile.toUri();
            StopWatch stopWatch = StopWatch.createStarted();
            FileLinkParams linkParams = new FileLinkParams().setUri(uri.toString());
            logger.info("Linking file {}", uri);
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
