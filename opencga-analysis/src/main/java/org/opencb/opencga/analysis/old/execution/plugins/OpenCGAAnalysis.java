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

package org.opencb.opencga.analysis.old.execution.plugins;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.old.models.tool.Execution;
import org.opencb.opencga.catalog.old.models.tool.InputParam;
import org.opencb.opencga.catalog.old.models.tool.Manifest;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created on 26/11/15.
 *
 * TODO: Move non abstract methods to a Context class
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class OpenCGAAnalysis {

    private Logger logger;
    private ObjectMap params;
    private CatalogManager catalogManager;
    private String sessionId;
    private boolean initialized;
    private StorageEngineFactory storageEngineFactory;
    private long studyId;
    private String execution;
    private VariantStorageManager variantStorageManager;

    public Manifest getManifest() {
        try {
            return loadManifest(getIdentifier());
        } catch (IOException ignore) {
            logger.error("Unable to load manifest");
            return null;
        }
    }

    public static Manifest loadManifest(String identifier) throws IOException {
        final String file;
        JsonFactory factory;
        if (OpenCGAAnalysis.class.getResource("/" + identifier + "/manifest.yml") != null) {
            file = "/" + identifier + "/manifest.yml";
            factory = new YAMLFactory();
        } else if (OpenCGAAnalysis.class.getResource("/" + identifier + "/manifest.json") != null) {
            file = "/" + identifier + "/manifest.json";
            factory = new JsonFactory();
        } else {
            return null;
        }
        try (InputStream stream = OpenCGAAnalysis.class.getResourceAsStream(file)) {
            return new ObjectMapper(factory).readValue(stream, Manifest.class);
        }
    }

    /**
     * Get analysis identifier.
     *
     * @return Analysis identifier
     */
    public abstract String getIdentifier();

    public int run() throws Exception {
        Execution execution = getManifest().getExecutions().get(0);
        for (Execution e : getManifest().getExecutions()) {
            if (e.getId().equals(this.execution)) {
                execution = e;
                break;
            }
        }
        Map<String, Path> inputParams = new LinkedHashMap<>();
        Path outdir = Paths.get(params.getString(execution.getOutputParam()));
        for (InputParam inputParam : execution.getInputParams()) {
            String inputParamValue = params.getString(inputParam.getName());
            if (StringUtils.isNotEmpty(inputParamValue)) {
                inputParams.put(inputParam.getName(), Paths.get(inputParamValue));
            }
        }
        return run(inputParams, outdir, params);
    }

    public abstract int run(Map<String, Path> input, Path outdir, ObjectMap params) throws Exception;

    /*
     *  Util methods
     */

    final void init(Logger logger, ObjectMap configuration, CatalogManager catalogManager,
                    StorageEngineFactory storageEngineFactory, long studyId, String execution, String sessionId) {
        if (initialized) {
            throw new IllegalStateException("The plugin was already initialized! Can't init twice");
        }
        this.logger = logger;
        this.params = configuration;
        this.catalogManager = catalogManager;
        this.storageEngineFactory = storageEngineFactory;
        this.variantStorageManager = new VariantStorageManager(catalogManager, this.storageEngineFactory);
        this.studyId = studyId;
        this.execution = execution;
        this.sessionId = sessionId;
        initialized = true;
    }

    protected final Logger getLogger() {
        return logger;
    }

    protected final ObjectMap getParams() {
        return params;
    }

    protected final CatalogManager getCatalogManager() {
        return catalogManager;
    }

    protected final String getSessionId() {
        return sessionId;
    }

    protected final VariantStorageManager getVariantStorageManager() {
        return variantStorageManager;
    }

    //TODO: Return an AlignmentDBAdaptor which checks catalog permissions
    protected final AlignmentDBAdaptor getAlignmentDBAdaptor(long studyId) {
        throw new UnsupportedOperationException();
    }

    protected final long getStudyId() {
        return studyId;
    }

    public String getExecution() {
        return execution;
    }
}
