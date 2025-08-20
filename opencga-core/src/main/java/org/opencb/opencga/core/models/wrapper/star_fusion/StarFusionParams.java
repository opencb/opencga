package org.opencb.opencga.core.models.wrapper.star_fusion;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Arrays;
import java.util.List;

public class StarFusionParams {

    // General params
    public static final String VERSION_PARAM = "--version";

    // File params
    public static final String LEFT_FQ_PARAM = "--left_fq";
    public static final String RIGHT_FQ_PARAM = "--right_fq";
    public static final String CHIMERIC_JUNCTION_PARAM = "--chimeric_junction";
    public static final String CAPITAL_J_PARAM = "-J";
    public static final String SAMPLES_FILE_PARAM = "--samples_file";
    public static final String TMPDIR_PARAM = "--tmpdir";
    public static final String GENOME_LIB_DIR_PARAM = "--genome_lib_dir";
    public static final String OUTPUT_DIR_PARAM = "--output_dir";
    public static final String CAPITAL_O_PARAM = "-O";

    public static List<String> FILE_PARAMS = Arrays.asList(LEFT_FQ_PARAM, RIGHT_FQ_PARAM, CHIMERIC_JUNCTION_PARAM, CAPITAL_J_PARAM,
            SAMPLES_FILE_PARAM, TMPDIR_PARAM, GENOME_LIB_DIR_PARAM, OUTPUT_DIR_PARAM, CAPITAL_O_PARAM);

    public static List<String> SKIPPED_PARAMS = Arrays.asList(OUTPUT_DIR_PARAM, CAPITAL_O_PARAM);

    protected ObjectMap options;

    public StarFusionParams() {
        this(new ObjectMap());
    }

    public StarFusionParams(ObjectMap options) {
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

    public StarFusionParams setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }
}
