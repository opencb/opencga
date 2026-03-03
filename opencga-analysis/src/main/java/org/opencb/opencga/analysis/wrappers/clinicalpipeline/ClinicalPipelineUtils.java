package org.opencb.opencga.analysis.wrappers.clinicalpipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineSample;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineTool;
import org.opencb.opencga.core.models.file.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor.INPUT_VIRTUAL_PATH;
import static org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor.SCRIPT_VIRTUAL_PATH;

public class ClinicalPipelineUtils {

    public static final String PIPELINE_ANALYSIS_DIRNAME = "ngs-pipeline";

    public static final String REFERENCE_GENOME_INDEX = "reference-genome";
    public static final String BWA_INDEX = "bwa";
    public static final String BWA_MEM2_INDEX = "bwa-mem2";

    public static final String NGS_PIPELINE_SCRIPT = "main.py";
    public static final String PREPARERE_NGS_PIPELINE_SCRIPT_COMMAND = "prepare";
    public static final String GENOMICS_NGS_PIPELINE_SCRIPT_COMMAND = "genomics";
    public static final String AFFY_PIPELINE_SCRIPT_COMMAND = "affy";

    public static final String DATA_VIRTUAL_PATH = "/data";
    public static final String INDEX_VIRTUAL_PATH = "/index";
    public static final String REFERENCE_VIRTUAL_PATH = "/reference";
    public static final String PIPELINE_PARAMS_FILENAME_PREFIX = "pipeline";

    public static final String SAMPLE_SEP = ";";
    public static final String SAMPLE_FIELD_SEP = "::";
    public static final String SAMPLE_FILE_SEP = ",";

    // Common pipeline
    public static final String QUALITY_CONTROL_PIPELINE_STEP = "quality-control";

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

    public static Path getPhysicalDirPath(String inputPath, String study, CatalogManager catalogManager, String token)
            throws CatalogException, ToolException {
        return getPhysicalPath(inputPath, File.Type.DIRECTORY, study, catalogManager, token);
    }

    public static Path getPhysicalFilePath(String inputPath, String study, CatalogManager catalogManager, String token)
            throws CatalogException, ToolException {
        return getPhysicalPath(inputPath, File.Type.FILE, study, catalogManager, token);
    }

    public static Path getPhysicalPath(String inputPath, File.Type fileType, String study, CatalogManager catalogManager, String token)
            throws CatalogException, ToolException {
        logger.info("Checking path {} as {}", inputPath, fileType.name());
        File opencgaFile = catalogManager.getFileManager().get(study, inputPath, QueryOptions.empty(), token).first();
        if (opencgaFile.getType() != fileType) {
            throw new ToolException("Clinical pipeline path '" + inputPath + "' is not a " + fileType.name());
        }
        // Update the data dir in the pipeline config
        Path outputPath = Paths.get(opencgaFile.getUri()).toAbsolutePath();
        if (Files.exists(outputPath)) {
            return outputPath;
        } else {
            throw new ToolException("Clinical pipeline path '" + inputPath + "' does not exist in the file system.");
        }
    }

    public static <C extends PipelineConfig> C copyPipelineConfig(C input) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String pipeline = mapper.writeValueAsString(input);
        @SuppressWarnings("unchecked")
        C copy = (C) mapper.readValue(pipeline, input.getClass());
        return copy;
    }

    public static PipelineSample createPipelineSampleFromString(String sampleString) throws ToolException {
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
        pipelineSample.setFiles(Arrays.asList(inputFiles.split(SAMPLE_FILE_SEP)));

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

    public static void validateTool(String step, PipelineTool pipelineTool) throws ToolException {
        if (pipelineTool == null) {
            throw new ToolException("Missing tool for clinical pipeline step: " + step);
        }
        if (StringUtils.isEmpty(pipelineTool.getId())) {
            throw new ToolException("Missing tool ID for clinical pipeline step: " + step);
        }
    }

    public static String buildPipelineFilename(List<String> pipelineSteps) {
        StringBuilder pipelineFilename = new StringBuilder(PIPELINE_PARAMS_FILENAME_PREFIX);
        for (String pipelineStep : pipelineSteps) {
            pipelineFilename.append("_").append(pipelineStep.replace("-", "_"));
        }
        pipelineFilename.append(".json");

        return pipelineFilename.toString();
    }

    public static String buildStepsParam(List<String> pipelineSteps) {
        StringBuilder steps = new StringBuilder();
        for (String pipelineStep : pipelineSteps) {
            if (steps.length() > 0) {
                steps.append(",");
            }
            steps.append(pipelineStep);
        }
        return steps.toString();
    }

    public static void setCommonInputBindings(Path pipelineConfigPath, Path scriptPath,
                                              List<AbstractMap.SimpleEntry<String, String>> inputBindings,
                                              Set<String> readOnlyInputBindings)
            throws IOException {

        // Script binding
        Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH);
        inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.toAbsolutePath().toString(), virtualScriptPath.toString()));
        readOnlyInputBindings.add(virtualScriptPath.toString());

        // Pipeline config binding
        Path virtualPipelineParamsPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(pipelineConfigPath.getFileName());
        inputBindings.add(new AbstractMap.SimpleEntry<>(pipelineConfigPath.toAbsolutePath().toString(),
                virtualPipelineParamsPath.toString()));
        readOnlyInputBindings.add(virtualPipelineParamsPath.toString());
    }

    public static String getVirtualPath(Path path, List<AbstractMap.SimpleEntry<String, String>> inputBindings) throws ToolException {
        for (AbstractMap.SimpleEntry<String, String> inputBinding : inputBindings) {
            if (inputBinding.getKey().equals(path.toString())) {
                return inputBinding.getValue();
            }
        }
        throw new ToolException("Could not find virtual path for input path: " + path);
    }
}