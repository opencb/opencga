package org.opencb.opencga.storage.app.service;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by jacobo on 23/10/14.
 */
public class OpenCGAStorageService implements Runnable {

    public static final String PORT     = "OPENCGA.STORAGE.APP.SERVICE.PORT";
    public static final String SLEEP    = "OPENCGA.STORAGE.APP.SERVICE.SLEEP";
    public static final String USER     = "OPENCGA.STORAGE.APP.SERVICE.USER";
    public static final String PASSWORD = "OPENCGA.STORAGE.APP.SERVICE.PASSWORD";

    private final Properties properties;

    private Server server;
    private Thread thread;
    private boolean exit = false;

    private static Logger logger = LoggerFactory.getLogger(OpenCGAStorageService.class);

    public OpenCGAStorageService(Properties properties) {
        this.properties = properties;

        int port = Integer.parseInt(properties.getProperty(OpenCGAStorageService.PORT, "8083"));

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages(true, "org.opencb.opencga.app.daemon.rest");
        ServletContainer sc = new ServletContainer(resourceConfig);
        ServletHolder sh = new ServletHolder(sc);

        logger.info("Server in port : {}", port);
        server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(server, null, ServletContextHandler.SESSIONS);
        context.addServlet(sh, "/opencga/rest/*");

        thread = new Thread(this);
    }


    @Override
    public void run() {
        int sleep = Integer.parseInt(properties.getProperty(SLEEP, "4000"));


        while(!exit) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                if(!exit) {
                    e.printStackTrace();
                }
            }
            logger.info("----- WakeUp {} -----", TimeUtils.getTimeMillis());

            try {



            } catch (Exception e) {
                e.printStackTrace();
            }
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

    synchronized public void stop() {
        exit = true;
        thread.interrupt();
    }
}
