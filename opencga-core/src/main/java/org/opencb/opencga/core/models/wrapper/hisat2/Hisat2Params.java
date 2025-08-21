package org.opencb.opencga.core.models.wrapper.hisat2;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.models.wrapper.WrapperParams;

import java.util.ArrayList;

public class Hisat2Params extends WrapperParams {

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

    public Hisat2Params() {
        super("hisat2", new ArrayList<>(), new ObjectMap());
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
}
