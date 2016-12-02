package org.opencb.opencga.storage.core.alignment.local;

import htsjdk.samtools.SAMSequenceRecord;
import org.opencb.commons.utils.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jtarraga on 01/12/16.
 */
public class LocalAlignmentGlobals {
    public static final String COVERAGE_WIG_SUFFIX = ".coverage.wig";
    public static final String COVERAGE_BIGWIG_SUFFIX = ".coverage.bw";

    public static final int DEFAULT_WINDOW_SIZE = 100;
    public static final int COVERAGE_REGION_SIZE = 100000;
}
