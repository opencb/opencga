package org.opencb.opencga.analysis.execution.plugins;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public int run(Job job) throws Exception {

        OpenCGAAnalysis plugin = PluginFactory.get().getPlugin(job.getToolName());
        ObjectMap configuration = new ObjectMap();
        configuration.putAll(job.getParams());

        //TODO: Add file appender
        Logger logger = LoggerFactory.getLogger(plugin.getClass());

        //TODO: Use CatalogClient?
        CatalogManager catalogManager = this.catalogManager;

        plugin.init(logger, configuration, catalogManager, StorageManagerFactory.get(),
                catalogManager.getStudyIdByJobId(job.getId()), sessionId);

        int result;
        try {
            result = plugin.run();
        } catch (Exception e) {
            throw new Exception(e);    //TODO: Handle this
        }

        return result;
    }

}
