package org.opencb.opencga.core.models.wrapper.hisat2;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.List;

public class Hisat2Params {

    // Main HISAT2 tools (stored in command)
    public static final String HISAT2_TOOL = "hisat2";
    public static final String HISAT2_BUILD_TOOL = "hisat2-build";

    // General options
    public static final String X_PARAM = "-x";
    public static final String ONE_PARAM = "-1";
    public static final String TWO_PARAM = "-2";
    public static final String U_PARAM = "-U";
    public static final String S_PARAM = "-S";
    public static final String KNOWN_SPLICESITE_INFILE_PARAM = "--known-splicesite-infile";
    public static final String NOVEL_SPLICESITE_OUTFILE_PARAM = "--novel-splicesite-outfile";
    public static final String NOVEL_SPLICE_SITE_INFILE_PARAM = "--novel-splicesite-infile";
    public static final String N_CEIL_PARAM = "--n-ceil";
    public static final String BMAXDIVN_PARAM = "--bmaxdivn";
    public static final String DCV_PARAM = "--dcv";

    protected String command;
    protected List<String> input;
    protected ObjectMap options;

    public Hisat2Params() {
        this("hisat2", new ArrayList<>(), new ObjectMap());
    }

    public Hisat2Params(String command, List<String> input, ObjectMap options) {
        this.command = command;
        this.input = input;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Hisat2Params{");
        sb.append("command='").append(command).append('\'');
        sb.append(", input=").append(input);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public static boolean isFileParam(String command, String param) {
        switch (param) {
            case X_PARAM:
            case ONE_PARAM:
            case TWO_PARAM:
            case U_PARAM:
            case KNOWN_SPLICESITE_INFILE_PARAM:
            case NOVEL_SPLICE_SITE_INFILE_PARAM:
                return true;
            default:
                return false;
        }
    }

    public static boolean isSkippedParam(String command, String param) {
        return false;
    }

    public String getCommand() {
        return command;
    }

    public Hisat2Params setCommand(String command) {
        this.command = command;
        return this;
    }

    public List<String> getInput() {
        return input;
    }

    public Hisat2Params setInput(List<String> input) {
        this.input = input;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public Hisat2Params setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }
}
