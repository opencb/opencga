package org.opencb.opencga.catalog.monitor.executors;

import org.opencb.opencga.catalog.config.CatalogConfiguration;

/**
 * Created by pfurio on 22/08/16.
 */
public class ExecutorManager {

    // TODO: Change for a map
    private AbstractExecutor executor;

    public ExecutorManager(CatalogConfiguration catalogConfiguration) {
        if (catalogConfiguration != null) {
            if (catalogConfiguration.getExecution().getMode().equalsIgnoreCase("local")) {
                this.executor = new LocalExecutor();
            } else if (catalogConfiguration.getExecution().getMode().equalsIgnoreCase("sge")) {
                // init sge executor
                this.executor = new SGEExecutor(catalogConfiguration);
                System.out.println("SGE not ready");
            }
        }

        if (executor == null) {
            // Load default executor
            this.executor = new LocalExecutor();
        }
    }

    public AbstractExecutor getExecutor() {
        return this.executor;
    }

    // TODO: CreateCommandLine (static method)
}
