package org.opencb.opencga.storage.app.service;

import org.opencb.opencga.lib.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by jacobo on 23/10/14.
 */
public class OpenCGAStorageServiceMain {


    public static Properties properties;
    private static String opencgaHome;

    private static Logger logger = LoggerFactory.getLogger(OpenCGAStorageServiceMain.class);
    private static OpenCGAStorageService storageService;

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
                opencgaHome = Paths.get("opencga-storage-app", "build").toString(); //If it has not been run from the shell script (debug)
            }
        }
        Config.setGcsaHome(opencgaHome);
        File configFile;
        if(opts.conf.isEmpty()) {
            configFile = Paths.get(opencgaHome, "conf", "storage-service.properties").toFile();
        } else {
            configFile = Paths.get(opts.conf).toFile();
        }
        if (!configFile.exists()) {
            throw new FileNotFoundException("File " + configFile.toString() + " not found");
        }
        properties = new Properties();
        properties.load(new FileInputStream(configFile));

        if (opts.port != 0) {
            properties.setProperty(OpenCGAStorageService.PORT, Integer.toString(opts.port));
        }
        if (opts.userId != null) {
            properties.setProperty(OpenCGAStorageService.USER, opts.userId);
        }
        if (opts.password != null) {
            properties.setProperty(OpenCGAStorageService.PASSWORD, opts.password);
        }

        int status;
        try {
            storageService = new OpenCGAStorageService(properties);
            storageService.start();
            status = storageService.join();
        } catch (Exception e) {
            e.printStackTrace();
            status = 3;
        }
        System.exit(status);
    }

    public static OpenCGAStorageService getStorageService() {
        return storageService;
    }
}

