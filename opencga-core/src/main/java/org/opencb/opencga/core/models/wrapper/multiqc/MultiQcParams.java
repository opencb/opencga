package org.opencb.opencga.core.models.wrapper;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultiQcParams {

    // General params
    public static String FILENAME_PARAM = "--filename";
    public static String OUTDIR_PARAM = "--outdir";
    public static String O_PARAM = "-o";

    // File params
    public static String CONFIG_FILE_PARAM = "--config";
    public static String C_FILE_PARAM = "-c";
    public static String REPLACE_NAMES_FILE_PARAM = "--replace-names";
    public static String SAMPLE_NAMES_FILE_PARAM = "--sample-names";
    public static String SAMPLE_FILTERS_FILE_PARAM = "--sample-filters";
    public static String CUSTOM_CSS_FILE_PARAM = "--custom-css-file";
    public static List<String> FILE_PARAMS = Arrays.asList(CONFIG_FILE_PARAM, C_FILE_PARAM, REPLACE_NAMES_FILE_PARAM,
            SAMPLE_NAMES_FILE_PARAM, SAMPLE_FILTERS_FILE_PARAM, CUSTOM_CSS_FILE_PARAM);

    // Skip params
    public static List<String> SKIP_PARAMS = Arrays.asList(OUTDIR_PARAM, O_PARAM);

    protected List<String> input;
    protected ObjectMap options;

    public MultiQcParams() {
        this.input = new ArrayList<>();
        this.options = new ObjectMap();
    }

    public MultiQcParams(List<String> input, ObjectMap options) {
        this.input = input;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MultiQcParams{");
        sb.append("input=").append(input);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getInput() {
        return input;
    }

    public MultiQcParams setInput(List<String> input) {
        this.input = input;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public MultiQcParams setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }
}
