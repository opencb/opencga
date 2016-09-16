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

package org.opencb.opencga.app.daemon;

//import org.apache.catalina.Server;


//import org.mortbay.jetty.Server;
//import org.mortbay.jetty.handler.ContextHandler;
//import org.mortbay.jetty.servlet.ServletHolder;
//import org.mortbay.jetty.servlet.ServletHandler;
//import org.mortbay.thread.QueuedThreadPool;
//import net.dpml.http.ServletContextHandler;


import org.opencb.opencga.core.common.Config;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jacobo on 23/10/14.
 */
@Deprecated
public class OpenCGADaemon {


    public static Properties properties;
    private static String opencgaHome;

    private static Logger logger = LoggerFactory.getLogger(OpenCGADaemon.class);
    private static DaemonLoop daemon;

    @Deprecated
    public static void main(String args[]) throws IOException {
        OptionParser optionParser = new OptionParser();

        String command = optionParser.parse(args);

        OptionParser.GeneralOptions opts = optionParser.getGeneralOptions();

        //Get properties
        String propertyAppHome = System.getProperty("app.home");
        if (propertyAppHome != null) {
            opencgaHome = System.getProperty("app.home");
        } else {
            String envAppHome = System.getenv("OPENCGA_HOME");
            if (envAppHome != null) {
                opencgaHome = envAppHome;
            } else {
                opencgaHome = Paths.get("opencga-app", "build").toString(); //If it has not been run from the shell script (debug)
            }
        }
        Config.setGcsaHome(opencgaHome);
        File configFile;
        if(opts.conf.isEmpty()) {
            configFile = Paths.get(opencgaHome, "conf", "daemon.properties").toFile();
        } else {
            configFile = Paths.get(opts.conf).toFile();
        }
        if (!configFile.exists()) {
            throw new FileNotFoundException("File " + configFile.toString() + " not found");
        }
        properties = new Properties();
        properties.load(new FileInputStream(configFile));

        if (opts.port != 0) {
            properties.setProperty(DaemonLoop.PORT, Integer.toString(opts.port));
        }
        if (opts.userId != null) {
            properties.setProperty(DaemonLoop.USER, opts.userId);
        }
        if (opts.password != null) {
            properties.setProperty(DaemonLoop.PASSWORD, opts.password);
        }

        int status;
        try {
            OpenCGADaemon.daemon = new DaemonLoop(properties);
            OpenCGADaemon.daemon.start();
            status = OpenCGADaemon.daemon.join();
        } catch (Exception e) {
            e.printStackTrace();
            status = 3;
        }
        System.exit(status);
    }

    public static DaemonLoop getDaemon() {
        return daemon;
    }


}

