package org.opencb.opencga.analysis.wrappers.clinicalpipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineSample;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyPipelineConfig;
import org.opencb.opencga.core.models.file.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ClinicalPipelineUtils {

    public static final String PIPELINE_ANALYSIS_DIRNAME = "ngs-pipeline";

    public static final String REFERENCE_GENOME_INDEX = "reference-genome";
    public static final String BWA_INDEX = "bwa";
    public static final String BWA_MEM2_INDEX = "bwa-mem2";

    public static final String NGS_PIPELINE_SCRIPT = "main.py";
    public static final String PREPARERE_NGS_PIPELINE_SCRIPT_COMMAND = "prepare";
    public static final String GENOMICS_NGS_PIPELINE_SCRIPT_COMMAND = "genomics";
    public static final String AFFY_PIPELINE_SCRIPT_COMMAND = "affy";

    public static final String INDEX_VIRTUAL_PATH = "/index";
    public static final String SAMPLE_VIRTUAL_PATH = "/sample";
    public static final String REFERENCE_VIRTUAL_PATH = "/reference";
    public static final String PIPELINE_PARAMS_FILENAME_PREFIX = "pipeline";

    public static final String SAMPLE_SEP = ";";
    public static final String SAMPLE_FIELD_SEP = "::";
    public static final String SAMPLE_FILE_SEP = ",";

    public static final String QUALITY_CONTROL_PIPELINE_STEP = "quality-control";
    public static final String ALIGNMENT_PIPELINE_STEP = "alignment";
    public static final String VARIANT_CALLING_PIPELINE_STEP = "variant-calling";
    public static final String GENOTYPE_PIPELINE_STEP = "genotype";
    protected static final Set<String> VALID_PIPELINE_STEPS = new HashSet<>(Arrays.asList(QUALITY_CONTROL_PIPELINE_STEP,
            ALIGNMENT_PIPELINE_STEP, VARIANT_CALLING_PIPELINE_STEP));
    protected static final Set<String> VALID_AFFY_PIPELINE_STEPS = new HashSet<>(Arrays.asList(QUALITY_CONTROL_PIPELINE_STEP,
            GENOTYPE_PIPELINE_STEP));


    private static final Logger logger = LoggerFactory.getLogger(ClinicalPipelineUtils.class);

    private ClinicalPipelineUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static boolean isURL(String input) {
        return input.startsWith("http://") || input.startsWith("https://") || input.startsWith("ftp://");
    }

    public static String getBaseFilename(List<String> files) {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("Files list cannot be null or empty");
        }

        if (files.size() > 1) {
            // Get base names for all files
            List<String> basenames = new ArrayList<>();
            for (String file: files) {
                basenames.add(Paths.get(file).getFileName().toString());
            }

            // Find common prefix
            String commonPrefix = findCommonPrefix(basenames);
            commonPrefix = commonPrefix.replaceAll("[._-]+$", ""); // Remove trailing separators

            if (commonPrefix.isEmpty()) {
                commonPrefix = basenames.get(0);
            }

            return commonPrefix;
        } else {
            String fileName = Paths.get(files.get(0)).getFileName().toString();
            return getBaseName(fileName);
        }
    }

    private static String getBaseName(String fileName) {
        String lower = fileName.toLowerCase();
        String[] extensions = {".fastq.gz", ".fq.gz", ".fastq", ".fq"};

        for (String ext : extensions) {
            if (lower.endsWith(ext)) {
                return fileName.substring(0, fileName.length() - ext.length());
            }
        }

        // Fallback: remove extension using Path
        Path path = Paths.get(fileName);
        String name = path.getFileName().toString();
        int dotIndex = name.lastIndexOf('.');
        return dotIndex > 0 ? name.substring(0, dotIndex) : name;
    }

    private static String findCommonPrefix(List<String> strings) {
        if (strings.isEmpty()) {
            return "";
        }

        String first = strings.get(0);
        int prefixLength = first.length();

        for (int i = 1; i < strings.size(); i++) {
            String current = strings.get(i);
            prefixLength = Math.min(prefixLength, current.length());

            for (int j = 0; j < prefixLength; j++) {
                if (first.charAt(j) != current.charAt(j)) {
                    prefixLength = j;
                    break;
                }
            }
        }

        return first.substring(0, prefixLength);
    }

    public static <T extends PipelineConfig> T copyPipelineConfig(T input) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String pipeline = mapper.writeValueAsString(input);
        @SuppressWarnings("unchecked")
        T copy = (T) mapper.readValue(pipeline, input.getClass());
        return copy;
    }

    public static <T extends PipelineConfig> T checkPipelineConfig(String pipelineFile, T pipelineConfig, CatalogManager catalogManager,
                                                                   String study, String token)
            throws ToolException, CatalogException, IOException {
        // Check pipeline configuration, if a pipeline file is provided, check the file exists and then read the file content
        // to add it to the params
        if (StringUtils.isEmpty(pipelineFile) && pipelineConfig == null) {
            throw new ToolException("Missing clinical pipeline configuration. You can either provide a pipeline configuration file or"
                    + " directly the pipeline configuration.");
        }

        T updatedPipelineConfig;

        // Get pipeline config
        if (!StringUtils.isEmpty(pipelineFile)) {
            logger.info("Checking clinical pipeline configuration file {}", pipelineFile);
            File opencgaFile = catalogManager.getFileManager().get(study, pipelineFile, QueryOptions.empty(), token).first();
            if (opencgaFile.getType() != File.Type.FILE) {
                throw new ToolException("Clinical pipeline configuration file '" + pipelineFile + "' is not a file.");
            }
            Path pipelinePath = Paths.get(opencgaFile.getUri()).toAbsolutePath();
            updatedPipelineConfig = JacksonUtils.getDefaultObjectMapper().readerFor(pipelineConfig.getClass())
                    .readValue(pipelinePath.toFile());
        } else {
            logger.info("Getting clinical pipeline configuration provided directly in the parameters");
            T copyPipelineConfig = JacksonUtils.getDefaultObjectMapper().convertValue(analysisParams.getPipelineParams()
                    .getPipeline(), AffyPipelineConfig.class);
            updatedPipelineConfig = copyPipelineConfig(affyPipelineConfig);
        }

        // Check pipeline input (i.e, samples and data dir)
        if (updatedPipelineConfig.getInput() == null && (CollectionUtils.isEmpty(analysisParams.getPipelineParams().getSamples())
                || StringUtils.isEmpty(analysisParams.getPipelineParams().getDataDir()))) {
            throw new ToolException("Missing clinical pipeline configuration input.");
        }
    }

    public static <T extends PipelineConfig> void updatePipelineConfig(T pipelineConfig, String study, CatalogManager catalogManager,
                                                                          String token) throws CatalogException, ToolException {
        // Update sample files (by getting the real paths) in the pipeline configuration
        for (PipelineSample sample : pipelineConfig.getInput().getSamples()) {
            List<String> updatedFiles = new ArrayList<>();
            for (String file : sample.getFiles()) {
                logger.info("Checking sample file {}", file);
                File opencgaFile = catalogManager.getFileManager().get(study, file, QueryOptions.empty(), token).first();
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

        // Update data dir (by getting the real path) in the pipeline configuration
        String inputDir = pipelineConfig.getInput().getDataDir();
        if (!StringUtils.isEmpty(inputDir)) {
            logger.info("Checking data dir {}", inputDir);
            File opencgaFile = catalogManager.getFileManager().get(study, inputDir, QueryOptions.empty(), token).first();
            if (opencgaFile.getType() != File.Type.DIRECTORY) {
                throw new ToolException("Clinical pipeline data dir '" + inputDir + "' is not a folder.");
            }
            // Update the data dir in the pipeline config
            pipelineConfig.getInput().setDataDir(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());
        }

        // Update index dir (by getting the real path) in the pipeline configuration
        inputDir = pipelineConfig.getInput().getIndexDir();
        if (!StringUtils.isEmpty(inputDir)) {
            logger.info("Checking index dir {}", inputDir);
            File opencgaFile = catalogManager.getFileManager().get(study, inputDir, QueryOptions.empty(), token).first();
            if (opencgaFile.getType() != File.Type.DIRECTORY) {
                throw new ToolException("Clinical pipeline index dir '" + inputDir + "' is not a folder.");
            }
            // Update the data dir in the pipeline config
            pipelineConfig.getInput().setIndexDir(Paths.get(opencgaFile.getUri()).toAbsolutePath().toString());
        }
    }

}