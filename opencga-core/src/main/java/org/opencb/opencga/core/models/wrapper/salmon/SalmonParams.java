package org.opencb.opencga.core.models.wrapper.salmon;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Arrays;
import java.util.List;

public class SalmonParams {

    // Main Kallisto commands (supported)
    public static final String INDEX_CMD = "index";
    public static final String QUANT_CMD = "quant";
    public static final String ALEVIN_CMD = "alevin";
    public static final String SWIM_CMD = "swim";
    public static final String QUANTMERGE_CMD = "quantmerge";
    // List of valid Salmon commands
    public static final List<String> VALID_COMMANDS = Arrays.asList(INDEX_CMD, QUANT_CMD, ALEVIN_CMD, SWIM_CMD, QUANTMERGE_CMD);

    // File options
    public static final String T_PARAM = "-t";
    public static final String TRANSCRIPTS_PARAM = "--transcripts";
    public static final String I_PARAM = "-i";
    public static final String INDEX_PARAM = "--index";
    public static final String TMPDIR_PARAM = "--tmpdir";
    public static final String D_PARAM = "-d";
    public static final String DECOYS_PARAM = "--decoys";

    // Common options
    public static final String VERSION_PARAM = "--version";
    public static final String CITE_PARAM = "--cite";

    // Quant command options

    private String command;
    private ObjectMap options;

    public SalmonParams() {
        this("", new ObjectMap());
    }

    public SalmonParams(String command, ObjectMap options) {
        this.command = command;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SalmonParams{");
        sb.append("command='").append(command).append('\'');
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public static boolean isFileParam(String command, String param) {
        switch (command) {
            case INDEX_CMD: {
                switch (param) {
                    case T_PARAM:
                    case TRANSCRIPTS_PARAM:
                    case I_PARAM:
                    case INDEX_PARAM:
                    case TMPDIR_PARAM:
                    case D_PARAM:
                    case DECOYS_PARAM:
                        return true;
                    default:
                        return false;
                }
            }

            case QUANT_CMD: {
                switch (param) {
                    case I_PARAM:
                    case INDEX_PARAM:
                        return true;
                    default:
                        return false;
                }
            }

            case SWIM_CMD: {
                switch (param) {
                    case I_PARAM:
                    case INDEX_PARAM:
                        return true;
                    default:
                        return false;
                }
            }

            default:
                return false;
        }
    }

    public static boolean isSkippedParam(String command, String param) {
        switch (command) {
            case INDEX_CMD: {
                switch (param) {
                    case I_PARAM:
                    case INDEX_PARAM:
                    case TMPDIR_PARAM: {
                        return true;
                    }

                    default:
                        return false;
                }
            }

//            case QUANT_CMD:
//            case SWIM_CMD: {
//                switch (param) {
//                        return true;
//                    default:
//                        return false;
//                }
//            }

            default: {
                return false;
            }
        }
    }

    public String getCommand() {
        return command;
    }

    public SalmonParams setCommand(String command) {
        this.command = command;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public SalmonParams setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }
}
