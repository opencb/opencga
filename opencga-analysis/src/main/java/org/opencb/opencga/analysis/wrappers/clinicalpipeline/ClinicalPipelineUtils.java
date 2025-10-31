package org.opencb.opencga.analysis.wrappers.clinicalpipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineConfig;

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

    public static final String INDEX_VIRTUAL_PATH = "/index";
    public static final String REFERENCE_VIRTUAL_PATH = "/reference";
    public static final String PIPELINE_PARAMS_FILENAME_PREFIX = "pipeline";

    public static final String SAMPLE_SEP = ";";
    public static final String SAMPLE_FIELD_SEP = "::";
    public static final String SAMPLE_FILE_SEP = ",";

    public static final String QUALITY_CONTROL_PIPELINE_STEP = "quality-control";
    public static final String ALIGNMENT_PIPELINE_STEP = "alignment";
    public static final String VARIANT_CALLING_PIPELINE_STEP = "variant-calling";
    protected static final Set<String> VALID_PIPELINE_STEPS = new HashSet<>(Arrays.asList(QUALITY_CONTROL_PIPELINE_STEP,
            ALIGNMENT_PIPELINE_STEP, VARIANT_CALLING_PIPELINE_STEP));

    private ClinicalPipelineUtils() {
        throw new IllegalStateException("Utility class");
    }

//    public static PipelineConfig copyPipelineConfig(PipelineConfig input) throws JsonProcessingException {
//        ObjectMapper mapper = new ObjectMapper();
//        String pipeline = mapper.writeValueAsString(input);
//        return mapper.readValue(pipeline, PipelineConfig.class);
//    }

    public static <T extends PipelineConfig> T copyPipelineConfig(T input) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String pipeline = mapper.writeValueAsString(input);
        @SuppressWarnings("unchecked")
        T copy = (T) mapper.readValue(pipeline, input.getClass());
        return copy;
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


}