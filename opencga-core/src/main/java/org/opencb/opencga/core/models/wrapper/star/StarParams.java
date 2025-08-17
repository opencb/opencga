package org.opencb.opencga.core.models.wrapper.star;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Arrays;
import java.util.List;

public class StarParams {

    // General params
    public static final String RUN_MODE_PARAM = "--runMode";
    public static final String RUN_THREAD_N_PARAM = "--runThreadN";
    public static final String OUT_FILE_NAME_PREFIX_PARAM = "--outFileNamePrefix";
    public static final String OUT_TMP_KEEP_PARAM = "--outTmpKeep";
    public static final String OUT_STD_PARAM = "--outStd";

    // Run modes
    public static final String ALIGN_READS_VALUE = "alignReads";
    public static final String GENOME_GENERATE_VALUE = "genomeGenerate";
    public static final String INPUT_ALIGNMENTS_FROM_BAM_VALUE = "inputAlignmentsFromBAM";
    public static final String LIFT_OVER_VALUE = "liftOver";
    public static final String SOLO_CELL_FILTERING_VALUE = "soloCellFiltering";

    // File params
    public static final String GENOME_DIR_PARAM = "--genomeDir";
    public static final String GENOME_FASTA_FILES_PARAM = "--genomeFastaFiles";
    public static final String READ_FILES_IN_PARAM = "--readFilesIn";
    public static final String SJDB_GTF_FILE_PARAM = "--sjdbGTFfile";
    public static final String SJDB_FILE_CHR_START_END_PARAM = "--sjdbFileChrStartEnd";
    public static final String READ_FILES_COMMAND_PARAM = "--readFilesCommand";
    public static final String OUT_TMP_DIR_PARAM = "--outTmpDir";
    public static final String PARAMETERS_FILES_PARAM = "--parametersFiles";

    public static List<String> FILE_PARAMS = Arrays.asList(GENOME_DIR_PARAM, GENOME_FASTA_FILES_PARAM, READ_FILES_IN_PARAM,
            SJDB_GTF_FILE_PARAM, SJDB_FILE_CHR_START_END_PARAM, READ_FILES_COMMAND_PARAM, OUT_TMP_DIR_PARAM, PARAMETERS_FILES_PARAM);

    public static List<String> SKIP_PARAMS = Arrays.asList(OUT_TMP_DIR_PARAM, OUT_TMP_KEEP_PARAM, OUT_STD_PARAM, PARAMETERS_FILES_PARAM);

    protected ObjectMap options;

    public StarParams() {
        this(new ObjectMap());
    }

    public StarParams(ObjectMap options) {
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StarParams{");
        sb.append("options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public ObjectMap getOptions() {
        return options;
    }

    public StarParams setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }
}
