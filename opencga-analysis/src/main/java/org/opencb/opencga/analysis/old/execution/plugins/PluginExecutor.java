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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.old.AnalysisExecutionException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PluginExecutor {

    protected static Logger logger = LoggerFactory.getLogger(PluginExecutor.class);

    private final CatalogManager catalogManager;
    private final String sessionId;

    public PluginExecutor(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    public int execute(String pluginId, String execution, long studyId, Map<String, String> params)
            throws CatalogException, AnalysisExecutionException {
        return execute(PluginFactory.get().getPlugin(pluginId), execution, studyId, params);
    }

    public <T extends OpenCGAAnalysis> int execute(Class<T> clazz, String execution, long studyId, Map<String, ?> params)
            throws CatalogException, AnalysisExecutionException {
        return execute(PluginFactory.get().getPlugin(clazz), execution, studyId, params);
    }

    private int execute(OpenCGAAnalysis plugin, String execution, long studyId, Map<String, ?> params)
            throws CatalogException, AnalysisExecutionException {

        ObjectMap configuration = new ObjectMap();
        configuration.putAll(params);

        //TODO: Add file appender
        Logger logger = LoggerFactory.getLogger(plugin.getClass());

        //TODO: Use CatalogClient?
        CatalogManager catalogManager = this.catalogManager;

        plugin.init(logger, configuration, catalogManager, StorageEngineFactory.get(),
                studyId, execution, sessionId);

        int result;
        try {
            result = plugin.run();
        } catch (Exception e) {
            throw new AnalysisExecutionException(e);    //TODO: Handle this
        }

        return result;
    }

}
