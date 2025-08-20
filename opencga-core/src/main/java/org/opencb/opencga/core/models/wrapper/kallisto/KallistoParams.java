package org.opencb.opencga.core.models.wrapper.kallisto;

import org.opencb.opencga.core.models.wrapper.WrapperParams;

import java.util.Arrays;
import java.util.List;

public class KallistoParams extends WrapperParams {

    // Main Kallisto commands (supported)
    public static final String INDEX_CMD = "index";
    public static final String QUANT_CMD = "quant";
    public static final String QUANT_TCC_CMD = "quant-tcc";
    public static final String BUS_CMD = "bus";
    public static final String H5DUMP_CMD = "h5dump";
    public static final String INSPECT_CMD = "inspect";
    public static final String VERSION_CMD = "version";
    public static final String CITE_CMD = "cite";
    // List of valid Kallisto commands
    public static final List<String> VALID_COMMANDS = Arrays.asList(INDEX_CMD, QUANT_CMD, QUANT_TCC_CMD, BUS_CMD, H5DUMP_CMD, INSPECT_CMD,
            VERSION_CMD, CITE_CMD);

    // File options
    public static final String I_PARAM = "-i";
    public static final String INDEX_PARAM = "--index";
    public static final String D_PARAM = "-d";
    public static final String D_LIST_PARAM = "--d-list";
    public static final String CAPITAL_T_PARAM = "-T";
    public static final String TMP_PARAM = "--tmp";
    public static final String O_PARAM = "-o";
    public static final String OUTPUT_DIR_PARAM = "--output-dir";
    public static final String TXNAMES_PARAM = "--txnames";
    public static final String E_PARAM = "-e";
    public static final String EC_FILE_PARAM = "--ec-file";
    public static final String F_PARAM = "-f";
    public static final String FRAGMENT_FILE_PARAM = "--fragment-file";
    public static final String G_PARAM = "-g";
    public static final String GENEMAP_PARAM = "--genemap";
    public static final String CAPITAL_G_PARAM = "-G";
    public static final String GTF_PARAM = "--gtf";

    // Common options

    // Quant command options
    public static final String SINGLE_PARAM = "--single";
    public static final String L_PARAM = "-l";
    public static final String FRAGMENT_LENGTH_PARAM = "--fragment-length";
    public static final String S_PARAM = "-s";
    public static final String SD_PARAM = "--sd";

    public static boolean isFileParam(String command, String param) {
        switch (command) {
            case INDEX_CMD: {
                switch (param) {
                    case D_PARAM:
                    case D_LIST_PARAM:
                    case CAPITAL_T_PARAM:
                    case TMP_PARAM:
                        return true;
                    default:
                        return false;
                }
            }

            case QUANT_CMD: {
                switch (param) {
                    case I_PARAM:
                    case INDEX_PARAM:
                    case O_PARAM:
                    case OUTPUT_DIR_PARAM:
                        return true;
                    default:
                        return false;
                }
            }

            case QUANT_TCC_CMD: {
                switch (param) {
                    case I_PARAM:
                    case INDEX_PARAM:
                    case O_PARAM:
                    case OUTPUT_DIR_PARAM:
                    case CAPITAL_T_PARAM:
                    case TXNAMES_PARAM:
                    case E_PARAM:
                    case EC_FILE_PARAM:
                    case F_PARAM:
                    case FRAGMENT_FILE_PARAM:
                    case G_PARAM:
                    case GENEMAP_PARAM:
                    case CAPITAL_G_PARAM:
                    case GTF_PARAM:
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
                    case CAPITAL_T_PARAM:
                    case TMP_PARAM: {
                        return true;
                    }

                    default:
                        return false;
                }
            }

            case QUANT_CMD:
            case QUANT_TCC_CMD: {
                switch (param) {
                    case O_PARAM:
                    case OUTPUT_DIR_PARAM:
                        return true;
                    default:
                        return false;
                }
            }

            default: {
                return false;
            }
        }
    }
}
