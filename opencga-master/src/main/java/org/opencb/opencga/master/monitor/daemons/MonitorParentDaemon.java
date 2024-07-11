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

import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.master.monitor.executors.BatchExecutor;
import org.opencb.opencga.master.monitor.executors.ExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by imedina on 16/06/16.
 */
public abstract class MonitorParentDaemon implements Runnable, Closeable {

    protected final int interval;
    protected final CatalogManager catalogManager;
    protected BatchExecutor batchExecutor;

    private volatile boolean exit = false;

    protected final String token;
    protected final Logger logger;

    public MonitorParentDaemon(int interval, String token, CatalogManager catalogManager) {
        this.interval = interval;
        this.catalogManager = catalogManager;
        this.token = token;
        logger = LoggerFactory.getLogger(this.getClass());
        ExecutorFactory executorFactory = new ExecutorFactory(catalogManager.getConfiguration());
        this.batchExecutor = executorFactory.getExecutor();
    }

    public boolean isExit() {
        return exit;
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }

    public void run() {
        try {
            init();
        } catch (Exception e) {
            logger.error("Error initializing daemon", e);
            throw new RuntimeException(e);
        }

        while (!exit) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                if (!exit) {
                    logger.warn("Interrupted while sleeping", e);
                }
                // If interrupted, stop the daemon
                break;
            }

            try {
                apply();
            } catch (Exception e) {
                logger.error("Catch exception " + e.getMessage(), e);
            }
        }

        try {
            close();
        } catch (IOException e) {
            logger.error("Error closing daemon", e);
        }
    }

    public void init() throws Exception {

    }

    public abstract void apply() throws Exception;
}
