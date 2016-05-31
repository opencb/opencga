package org.opencb.opencga.analysis.execution.plugins.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.opencga.analysis.beans.Analysis;
import org.opencb.opencga.analysis.beans.Execution;
import org.opencb.opencga.analysis.beans.Option;
import org.opencb.opencga.analysis.execution.plugins.OpenCGAAnalysis;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class TestAnalysis extends OpenCGAAnalysis {

    public static final String OUTDIR = "outdir";
    public static final String PARAM_1 = "param1";
    public static final String ERROR = "error";
    public static final String PLUGIN_ID = "test_plugin";
    private final Analysis manifest;

    public TestAnalysis() {
        List<Option> validParams = Arrays.asList(
                new Option(OUTDIR, "", true),
                new Option(PARAM_1, "", false),
                new Option(ERROR, "", false)
        );
        List<Execution> executions = Collections.singletonList(
                new Execution("default", "default", "", Collections.emptyList(), Collections.emptyList(), OUTDIR, validParams, Collections.emptyList(), null, null)
        );
        manifest = new Analysis(null, "0.1.0", PLUGIN_ID, "Test plugin", "", "", "", null, Collections.emptyList(), executions, null, null);
        try {
            System.out.println(new ObjectMapper().writer().writeValueAsString(manifest));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Analysis getManifest() {
        return manifest;
    }

    @Override
    public String getIdentifier() {
        return PLUGIN_ID;
    }

    @Override
    public int run() throws Exception {
        if (getConfiguration().containsKey(PARAM_1)) {
            getLogger().info(getConfiguration().getString(PARAM_1));
        }
        return getConfiguration().getBoolean(ERROR) ? 1 : 0;
    }

}
