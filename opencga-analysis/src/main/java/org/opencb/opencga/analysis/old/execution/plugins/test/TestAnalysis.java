/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.old.execution.plugins.test;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.old.models.tool.Manifest;
import org.opencb.opencga.catalog.old.models.tool.Execution;
import org.opencb.opencga.catalog.old.models.tool.Option;
import org.opencb.opencga.analysis.old.execution.plugins.OpenCGAAnalysis;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    private final Manifest manifest;

    public TestAnalysis() {
        List<Option> validParams = Arrays.asList(
                new Option(OUTDIR, "", true),
                new Option(PARAM_1, "", false),
                new Option(ERROR, "", false)
        );
        List<Execution> executions = Collections.singletonList(
                new Execution("default", "default", "", Collections.emptyList(), Collections.emptyList(), OUTDIR, validParams, Collections.emptyList(), null, null)
        );
        manifest = new Manifest(null, "0.1.0", PLUGIN_ID, "Test plugin", "", "", "", null, Collections.emptyList(), executions, null, null);
    }

    @Override
    public Manifest getManifest() {
        return manifest;
    }

    @Override
    public String getIdentifier() {
        return PLUGIN_ID;
    }

    @Override
    public int run(Map<String, Path> input, Path output, ObjectMap params) throws Exception {
        if (params.containsKey(PARAM_1)) {
            getLogger().info(params.getString(PARAM_1));
        }
        return params.getBoolean(ERROR) ? 1 : 0;
    }

}
