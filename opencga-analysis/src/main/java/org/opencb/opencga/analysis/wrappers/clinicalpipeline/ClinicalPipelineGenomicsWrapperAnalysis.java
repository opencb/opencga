package org.opencb.opencga.analysis.wrappers.clinicalpipeline;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.ClinicalPipelineGenomicsWrapperParams;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineSample;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineGenomicsWrapperAnalysisExecutor.*;
import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;

@Tool(id = ClinicalPipelineGenomicsWrapperAnalysis.ID, resource = Enums.Resource.VARIANT,
        description = ClinicalPipelineGenomicsWrapperAnalysis.DESCRIPTION)
public class ClinicalPipelineGenomicsWrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "ngs-pipeline-genomics";
    public static final String DESCRIPTION = "Execute the clinical genomics pipeline that performs QC (e.g.: FastQC), mapping (e.g.: BWA),"
            + " variant calling (e.g., GATK) and variant indexing in OpenCGA storage.";

    public static final String SAMPLE_SEP = ";";
    public static final String SAMPLE_FIELD_SEP = "::";
    public static final String SAMPLE_FILE_SEP = ",";

    private static final String GENOMICS_PIPELINE_STEP = "genomics-pipeline";
    private static final String VARIANT_INDEX_STEP = "variant-index";

    private List<String> pipelineSteps;
    private PipelineConfig updatedPipelineConfig = new PipelineConfig();

    @ToolParams
    protected final ClinicalPipelineGenomicsWrapperParams analysisParams = new ClinicalPipelineGenomicsWrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check pipeline configuration, if a pipeline file is provided, check the file exists and then read the file content
        // to add it to the params
        if (StringUtils.isEmpty(analysisParams.getPipelineParams().getPipelineFile())
                && analysisParams.getPipelineParams().getPipeline() == null) {
            throw new ToolException("Missing clinical pipeline configuration. You can either provide a pipeline configuration JSON"
                    + " file or directly the pipeline configuration.");
        }

        // Get pipeline config
        if (StringUtils.isEmpty(analysisParams.getPipelineParams().getPipelineFile())) {
            String pipelineFile = analysisParams.getPipelineParams().getPipelineFile();
            logger.info("Checking clinical pipeline configuration file {}", pipelineFile);
            File opencgaFile = getCatalogManager().getFileManager().get(study, pipelineFile, QueryOptions.empty(), token).first();
            if (opencgaFile.getType() != File.Type.FILE) {
                throw new ToolException("Clinical pipeline definition path '" + pipelineFile + "' is not a file.");
            }
            Path pipelinePath = Paths.get(opencgaFile.getUri()).toAbsolutePath();
            updatedPipelineConfig = JacksonUtils.getDefaultObjectMapper().readerFor(PipelineConfig.class).readValue(pipelinePath.toFile());
        } else {
            logger.info("Getting clinical pipeline configuration provided directly in the parameters");
            updatedPipelineConfig = new PipelineConfig(analysisParams.getPipelineParams().getPipeline());
        }

        // If samples are provided in the pipeline params, set them in the pipeline config to be process later
        if (CollectionUtils.isNotEmpty(analysisParams.getPipelineParams().getSamples())) {
            List<PipelineSample> pipelineSamples = new ArrayList<>();
            for (String sample : analysisParams.getPipelineParams().getSamples()) {
                pipelineSamples.add(createPipelineSampleFromString(sample));
            }
            updatedPipelineConfig.getInput().setSamples(pipelineSamples);
        }

        // If index dir is provided in the pipeline params, set it in the pipeline config to be process later
        if (StringUtils.isNotEmpty(analysisParams.getPipelineParams().getIndexDir())) {
            updatedPipelineConfig.getInput().setIndexDir(analysisParams.getPipelineParams().getIndexDir());
        }

        // Now we can process the files in samples in the updated pipeline config to check they exists and get the real paths
        for (PipelineSample sample : updatedPipelineConfig.getInput().getSamples()) {
            List<String> updatedFiles = new ArrayList<>();
            for (String file : sample.getFiles()) {
                logger.info("Checking sample file {}", file);
                File opencgaFile = getCatalogManager().getFileManager().get(study, file, QueryOptions.empty(), token).first();
                if (opencgaFile.getType() != File.Type.FILE) {
                    throw new ToolException("Clinical pipeline sample file '" + file + "' for sample ID '" + sample.getId()
                            + "' is not a file.");
                }
                // Add the real path to the updated files
                updatedFiles.add(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());
            }
            // Set updated files in the sample
            sample.setFiles(updatedFiles);
        }


        // And finally, the index dir
        String indexDir = updatedPipelineConfig.getInput().getIndexDir();
        if (StringUtils.isEmpty(indexDir)) {
            throw new ToolException("Missing clinical pipeline index directory. You can either provide an index directory in the"
                    + " pipeline configuration or in the execute parameters.");
        }
        logger.info("Checking index dir {}", indexDir);
        File opencgaFile = getCatalogManager().getFileManager().get(study, indexDir, QueryOptions.empty(), token).first();
        if (opencgaFile.getType() != File.Type.DIRECTORY) {
            throw new ToolException("Clinical pipeline index dir '" + indexDir + "' is not a folder.");
        }
        // Update the index dir in the pipeline config
        updatedPipelineConfig.getInput().setIndexDir(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());

        // Check the pipeline steps
        if (CollectionUtils.isNotEmpty(analysisParams.getPipelineParams().getSteps())) {
            for (String step : analysisParams.getPipelineParams().getSteps()) {
                if (!VALID_PIPELINE_STEPS.contains(step)) {
                    throw new ToolException("Clinical pipeline step '" + step + "' is not valid. Supported steps are: "
                            + String.join(", ", VALID_PIPELINE_STEPS));
                }
            }
            pipelineSteps = analysisParams.getPipelineParams().getSteps();
        } else {
            pipelineSteps = Arrays.asList(QUALITY_CONTROL_STEP, ALIGNMENT_STEP, VARIANT_CALLING_STEP);
        }
    }

    @Override
    protected List<String> getSteps() {
        if (analysisParams.getPipelineParams().getVariantIndexParams() != null) {
            return Arrays.asList(GENOMICS_PIPELINE_STEP, VARIANT_INDEX_STEP);
        } else {
            return Collections.singletonList(GENOMICS_PIPELINE_STEP);
        }
    }

    protected void run() throws ToolException, IOException {
        // Execute the pipeline
        step(GENOMICS_PIPELINE_STEP, this::runPipelineExecutor);

        if (analysisParams.getPipelineParams().getVariantIndexParams() != null) {
            // Index variants in OpenCGA storage
            step(VARIANT_INDEX_STEP, this::indexVariants);
        }
    }

    protected void runPipelineExecutor() throws  ToolException {
        // Get executor
        ClinicalPipelineGenomicsWrapperAnalysisExecutor executor = getToolExecutor(ClinicalPipelineGenomicsWrapperAnalysisExecutor.class);

        // Set parameters and execute (depending on the updated params, it will prepare or execute the pipeline)
        executor.setStudy(study)
                .setScriptPath(getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve(ID))
                .setPipelineConfig(updatedPipelineConfig)
                .setPipelineSteps(pipelineSteps)
                .execute();

        // TODO: check output?
    }

    protected void indexVariants() throws CatalogException, StorageEngineException, ToolException, IOException {
        // Find the .sorted.gatk.vcf file within the variant-calling folder
        Path vcfPath;
        try (Stream<Path> stream = Files.list(getOutDir().resolve("variant-calling"))) {
            vcfPath = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".sorted.gatk.vcf"))
                    .findFirst()
                    .orElse(null);
        }

        if (vcfPath == null || !Files.exists(vcfPath)) {
            throw new ToolException("Could not find the generated VCF: " + vcfPath);
        }
        File vcfFile = catalogManager.getFileManager().link(study, new FileLinkParams(vcfPath.toAbsolutePath().toString(),
                "", "", "", null, null, null, null, null), false, token).first();

        ObjectMap storageOptions = analysisParams.getPipelineParams().getVariantIndexParams() != null
                ? analysisParams.getPipelineParams().getVariantIndexParams().toObjectMap()
                : new ObjectMap();

        getVariantStorageManager().index(study, vcfFile.getId(), getScratchDir().toAbsolutePath().toString(), storageOptions, token);
    }

    private PipelineSample createPipelineSampleFromString(String sampleString) throws CatalogException, ToolException {
        // Parse the input format: sample_id::file_id1[,file_id2][::somatic::role]
        String[] fields = sampleString.split(SAMPLE_FIELD_SEP);
        if (fields.length < 2) {
            throw new ToolException("Invalid input format. Expected format: sample_id" + SAMPLE_FIELD_SEP + "file_id1["
                    + SAMPLE_FILE_SEP + "file_id2][" + SAMPLE_FIELD_SEP + "somatic" + SAMPLE_FIELD_SEP + "role],"
                    + " but got: " + sampleString);
        }

        PipelineSample pipelineSample = new PipelineSample();

        // Set sample ID
        pipelineSample.setId(fields[0]);

        // Parse and set files
        String inputFiles = fields[1];
        if (StringUtils.isEmpty(inputFiles)) {
            throw new ToolException("Missing input files for sample '" + fields[0] + "': " + sampleString);
        }

        String[] inputFileArray = inputFiles.split(SAMPLE_FILE_SEP);
        List<String> fileList = new ArrayList<>();

        for (String inputFile : inputFileArray) {
            File opencgaFile = getCatalogManager().getFileManager().get(study, inputFile, QueryOptions.empty(), token).first();
            if (opencgaFile.getType() != File.Type.FILE) {
                throw new ToolException("Clinical pipeline input path '" + inputFile + "' is not a file.");
            }
            fileList.add(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());
        }
        pipelineSample.setFiles(fileList);

        // Parse optional somatic field (default: false)
        if (fields.length > 2 && StringUtils.isNotEmpty(fields[2])) {
            pipelineSample.setSomatic("somatic".equalsIgnoreCase(fields[2]));
        } else {
            pipelineSample.setSomatic(false);
        }

        // Parse optional role field (default: null)
        if (fields.length > 3 && StringUtils.isNotEmpty(fields[3])) {
            pipelineSample.setRole(fields[3]);
        }

        return pipelineSample;
    }
}
