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

package org.opencb.opencga.catalog.monitor.daemons;

import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.opencb.opencga.catalog.monitor.executors.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by imedina on 16/06/16.
 */
public abstract class MonitorParentDaemon implements Runnable {

    protected int interval;
    protected CatalogManager catalogManager;
    protected DBAdaptorFactory dbAdaptorFactory;
    protected AbstractExecutor executorManager;

    protected boolean exit = false;

    protected String sessionId;

    protected Logger logger;

    public MonitorParentDaemon(int interval, String sessionId, CatalogManager catalogManager) throws CatalogDBException {
        this.interval = interval;
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
        logger = LoggerFactory.getLogger(this.getClass());

        configureDBAdaptor(catalogManager.getConfiguration());
        ExecutorManager executorFactory = new ExecutorManager(catalogManager.getConfiguration());
        this.executorManager = executorFactory.getExecutor();

//        if (catalogManager.getCatalogConfiguration().getExecution().getMode().equalsIgnoreCase("local")) {
//            this.executorManager = new LocalExecutorManager(catalogManager, sessionId);
//            logger.info("Jobs will be launched locally");
//        } else {
//            this.executorManager = new SgeExecutorManager(catalogManager, sessionId);
//            logger.info("Jobs will be launched to SGE");
//        }
    }

    private void configureDBAdaptor(Configuration configuration) throws CatalogDBException {

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", configuration.getCatalog().getDatabase().getUser())
                .add("password", configuration.getCatalog().getDatabase().getPassword())
                .add("authenticationDatabase", configuration.getCatalog().getDatabase().getOptions().get("authenticationDatabase"))
                .build();

        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : configuration.getCatalog().getDatabase().getHosts()) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }
        dbAdaptorFactory = new MongoDBAdaptorFactory(dataStoreServerAddresses, mongoDBConfiguration,
                catalogManager.getCatalogDatabase()) {};
    }

    public boolean isExit() {
        return exit;
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }


}
