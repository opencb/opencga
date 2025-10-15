package org.opencb.opencga.analysis.wrappers.clinicalpipeline;

public class ClinicalPipelineUtils {

    public static final String PIPELINE_ANALYSIS_DIR = "ngs-pipeline";

    public static final String REFERENCE_GENOME_INDEX = "reference-genome";
    public static final String BWA_INDEX = "bwa";
    public static final String BWA_MEM2_INDEX = "bwa-mem2";

    public static final String NGS_PIPELINE_SCRIPT = "main.py";
    public static final String INDEX_VIRTUAL_PATH = "/index";
    public static final String PIPELINE_PARAMS_FILENAME = "pipeline.json";


    public static boolean isURL(String input) {
        return input.startsWith("http://") || input.startsWith("https://") || input.startsWith("ftp://");
    }
}