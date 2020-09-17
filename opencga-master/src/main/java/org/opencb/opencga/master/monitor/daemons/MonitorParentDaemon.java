/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.master.monitor.daemons;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.master.monitor.executors.BatchExecutor;
import org.opencb.opencga.master.monitor.executors.ExecutorFactory;

import java.nio.file.Path;

/**
 * Created by imedina on 16/06/16.
 */
public abstract class MonitorParentDaemon implements Runnable {

    protected int interval;
    protected CatalogManager catalogManager;
    // FIXME: This should not be used directly! All the queries MUST go through the CatalogManager
    @Deprecated
    protected DBAdaptorFactory dbAdaptorFactory;
    protected BatchExecutor batchExecutor;

    protected boolean exit = false;

    protected String token;

    protected Logger logger;

    public MonitorParentDaemon(int interval, String token, CatalogManager catalogManager) throws CatalogDBException {
        this.interval = interval;
        this.catalogManager = catalogManager;
        this.token = token;
        logger = LogManager.getLogger(this.getClass());
        dbAdaptorFactory = new MongoDBAdaptorFactory(catalogManager.getConfiguration());
        ExecutorFactory executorFactory = new ExecutorFactory(catalogManager.getConfiguration());
        this.batchExecutor = executorFactory.getExecutor();
    }

    public boolean isExit() {
        return exit;
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }

    static Path getJobTemporaryFolder(long jobId, Path tempJobFolder) {
        return tempJobFolder.resolve(getJobTemporaryFolderName(jobId));
    }

    static String getJobTemporaryFolderName(long jobId) {
        return "J_" + jobId;
    }

}
