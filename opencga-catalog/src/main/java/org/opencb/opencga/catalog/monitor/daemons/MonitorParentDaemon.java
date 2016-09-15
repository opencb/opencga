/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog.monitor.daemons;

import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.monitor.executors.ExecutorManager;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 16/06/16.
 */
public abstract class MonitorParentDaemon implements Runnable {

    protected int interval;
    protected CatalogManager catalogManager;
    protected AbstractExecutor executorManager;

    protected boolean exit = false;

    protected String sessionId;

    protected static Logger logger;

    public MonitorParentDaemon(int interval, String sessionId, CatalogManager catalogManager) {
        this.interval = interval;
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
        logger = LoggerFactory.getLogger(this.getClass());

        ExecutorManager executorFactory = new ExecutorManager(catalogManager.getCatalogConfiguration());
        this.executorManager = executorFactory.getExecutor();
//        if (catalogManager.getCatalogConfiguration().getExecution().getMode().equalsIgnoreCase("local")) {
//            this.executorManager = new LocalExecutorManager(catalogManager, sessionId);
//            logger.info("Jobs will be launched locally");
//        } else {
//            this.executorManager = new SgeExecutorManager(catalogManager, sessionId);
//            logger.info("Jobs will be launched to SGE");
//        }
    }

    public boolean isExit() {
        return exit;
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }


}
