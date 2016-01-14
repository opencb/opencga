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

package org.opencb.opencga.storage.app.service;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//import org.opencb.opencga.catalog.CatalogException;
//import org.opencb.opencga.catalog.CatalogManager;
//import org.opencb.opencga.catalog.db.CatalogDBException;
//import org.opencb.opencga.catalog.io.CatalogIOManagerException;

/**
 * Created by jacobo on 23/10/14.
 */
public final class OpenCGAStorageService implements Runnable {

    public static final String PORT = "OPENCGA.STORAGE.APP.SERVICE.PORT";
    public static final String SLEEP = "OPENCGA.STORAGE.APP.SERVICE.SLEEP";
    public static final String USER = "OPENCGA.STORAGE.APP.SERVICE.USER";
    public static final String PASSWORD = "OPENCGA.STORAGE.APP.SERVICE.PASSWORD";

    private final Properties properties;
    //    private CatalogManager catalogManager;
    private static OpenCGAStorageService singleton;

    private Server server;
    private Thread thread;
    private boolean exit = false;

    private static Logger logger = LoggerFactory.getLogger(OpenCGAStorageService.class);

    public static OpenCGAStorageService newInstance(Properties properties) {
        singleton = new OpenCGAStorageService(properties);
        return singleton;
    }

    public static OpenCGAStorageService getInstance() {
        return singleton;
    }

    private OpenCGAStorageService(Properties properties) {
        this.properties = properties;
//        this.catalogManager = new CatalogManager(Config.getCatalogProperties());


        int port = Integer.parseInt(properties.getProperty(OpenCGAStorageService.PORT, "8083"));

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages(true, "org.opencb.opencga.storage.server.rest");

        ServletContainer sc = new ServletContainer(resourceConfig);
        ServletHolder sh = new ServletHolder("opencga", sc);

        logger.info("Server in port : {}", port);
        server = new Server(port);
        sh.setInitParameter("sessionManager", "org.opencb.opnecga.catalog.XXX");
        ServletContextHandler context = new ServletContextHandler(server, null, ServletContextHandler.SESSIONS);
        context.addServlet(sh, "/opencga/webservices/rest/*");

        thread = new Thread(this);
    }

    @Override
    public void run() {
        int sleep = Integer.parseInt(properties.getProperty(SLEEP, "60000"));


        while (!exit) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                if (!exit) {
                    e.printStackTrace();
                }
            }
            logger.info("----- WakeUp {} -----", TimeUtils.getTimeMillis());

//            try {
//
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
        }


        try {
            Thread.sleep(200);
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start() throws Exception {
        //Start services
        server.start();
        thread.start();
    }

    public int join() {
        //Join services
        try {
            logger.info("Join to Server");
            server.join();
            logger.info("Join to Thread");
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 2;
        }
        return 0;
    }

    public synchronized void stop() {
        exit = true;
        thread.interrupt();
    }

    public Properties getProperties() {
        return properties;
    }
//    public CatalogManager getCatalogManager() {
//        return catalogManager;
//    }
}
