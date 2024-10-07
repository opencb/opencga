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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.LiftoverWrapperParams;
import org.opencb.opencga.core.tools.ResourceManager;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.core.api.FieldConstants.*;

@Tool(id = LiftoverWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = LiftoverWrapperAnalysis.DESCRIPTION)
public class LiftoverWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "liftover";
    public static final String DESCRIPTION = "BCFtools liftover plugin maps coordinates from assembly 37 to 38.";

    public static final String RESOURCES_FOLDER = "resources";
    private static final String PREPARE_RESOURCES_STEP = "prepare-resources";

    private List<File> files = new ArrayList<>();
    private String vcfDest;
    private Path resourcePath;

    @ToolParams
    protected final LiftoverWrapperParams analysisParams = new LiftoverWrapperParams();

    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        if (CollectionUtils.isEmpty(analysisParams.getFiles())) {
            throw new ToolException("Liftover 'files' parameter is mandatory.");
        }

        // Check files
        org.opencb.opencga.core.models.file.File opencgaFile;
        for (String file : analysisParams.getFiles()) {
            opencgaFile = getCatalogManager().getFileManager().get(study, file, QueryOptions.empty(), token).first();
            files.add(Paths.get(opencgaFile.getUri().getPath()).toFile());
        }

        // Check target assembly
        if (StringUtils.isEmpty(analysisParams.getTargetAssembly())) {
            throw new ToolException("Liftover 'targetAssembly' parameter is mandatory, valid options are '" + LIFTOVER_GRCH38 + "' and '"
                    + LIFTOVER_HG38 + "'.");
        }

        if (!LIFTOVER_GRCH38.equals(analysisParams.getTargetAssembly()) && !LIFTOVER_HG38.equals(analysisParams.getTargetAssembly())) {
            throw new ToolException("Unknown Liftover 'targetAssembly' parameter ('" + analysisParams.getTargetAssembly()
                    + "') , valid options are '" + LIFTOVER_GRCH38 + "' and '" + LIFTOVER_HG38 + "'.");
        }

        // Check destination
        vcfDest = analysisParams.getVcfDestination();
        if (StringUtils.isEmpty(vcfDest)) {
            logger.info("Liftover 'vcfDestination' parameter is empty, the resultant VCF files will be stored in the job directory: {}",
                    getOutDir());
        } else if (!LIFTOVER_VCF_INPUT_FOLDER.equals(vcfDest)) {
            opencgaFile = getCatalogManager().getFileManager().get(study, analysisParams.getVcfDestination(), QueryOptions.empty(), token)
                    .first();
            vcfDest = Paths.get(opencgaFile.getUri().getPath()).toAbsolutePath().toString();
            if (!Files.exists(Paths.get(vcfDest))) {
                throw new ToolException("Liftover 'vcfDestination' parameter (" + analysisParams.getVcfDestination() + ") with folder ("
                        + vcfDest + ") does not exist");
            }
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(PREPARE_RESOURCES_STEP, ID);
    }

    protected void run() throws ToolException, IOException {
        // Download and copy liftover resource files in the job dir
        step(PREPARE_RESOURCES_STEP, this::prepareResources);

        // Run liftover script
        step(ID, this::runLiftover);

        // Do we have to clean the liftover resource folder
//        Files.newDirectoryStream(resourcePath).forEach(file -> {
//            try {
//                Files.delete(file);
//            } catch (IOException e) {
//                logger.warn("Error deleting file '{}': {}", file, e.getMessage());
//            }
//        });
    }


    private void prepareResources() throws IOException, ToolException {
        // Create folder where the liftover resources will be saved (within the job dir, aka outdir)
        resourcePath = Files.createDirectories(getOutDir().resolve(RESOURCES_FOLDER));

        // Identify Liftover resources to download only the required ones
        Map<String, List<String>> mapResources = new HashMap<>();
        switch (analysisParams.getTargetAssembly().toUpperCase()) {
            case LIFTOVER_GRCH38: {
                mapResources.put(ID, Collections.singletonList("GRCh37_to_GRCh38.chain.gz"));
                mapResources.put("reference-genome", Arrays.asList("Homo_sapiens.GRCh37.dna.primary_assembly.fa.gz",
                        "Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"));
                break;
            }
            case LIFTOVER_HG38: {
                mapResources.put(ID, Collections.singletonList("hg19ToHg38.over.chain.gz"));
                mapResources.put("reference-genome", Arrays.asList("hg19.fa.gz", "hg38.fa.gz"));
                break;
            }
            default: {
                throw new ToolException("Unknown Liftover 'targetAssembly' parameter ('" + analysisParams.getTargetAssembly()
                        + "') , valid options are '" + LIFTOVER_GRCH38 + "' and '" + LIFTOVER_HG38 + "'.");
            }
        }

        // Download resources and copy them to the job dir
        // (this URL is temporary, it should be replaced by the resourceUrl from configuration file)
        ResourceManager resourceManager = new ResourceManager(getOpencgaHome(), "http://resources.opencb.org/task-6766/");
        for (Map.Entry<String, List<String>> entry : mapResources.entrySet()) {
            for (String resourceName : entry.getValue()) {
                File resourceFile = resourceManager.getResourceFile(entry.getKey(), resourceName);
                Files.copy(resourceFile.toPath(), resourcePath.resolve(resourceFile.getName()));
            }
        }
    }

    private void runLiftover() throws Exception {
        // Get executor
        LiftoverWrapperAnalysisExecutor executor = getToolExecutor(LiftoverWrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setLiftoverPath(getOpencgaHome().resolve("analysis").resolve(ID))
                .setFiles(files)
                .setTargetAssembly(analysisParams.getTargetAssembly())
                .setVcfDest(vcfDest)
                .setResourcePath(resourcePath)
                .execute();
    }
}
