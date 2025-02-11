package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseFactory;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriInfo;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;

public class EnterpriseOpenCGAWSServer extends OpenCGAWSServer {

    public static AtomicBoolean enterpriseInitialized;

    protected static EnterpriseConfiguration enterpriseConfiguration;

    protected static Logger enterpriseLogger;

    static {
        enterpriseInitialized = new AtomicBoolean(false);
    }

    public EnterpriseOpenCGAWSServer(UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        init();
    }

    public EnterpriseOpenCGAWSServer(String version, UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
        init();
    }

    private void init() {
        enterpriseInitialized.set(true);

        enterpriseLogger = LoggerFactory.getLogger(EnterpriseOpenCGAWSServer.class);
        enterpriseLogger.info("========================================================================");
        enterpriseLogger.info("| Initializing EnterpriseOpenCGAWSServer");
        enterpriseLogger.info("| This message must appear only once.");

        java.nio.file.Path configDirPath = opencgaHome.resolve("conf");
        if (Files.exists(configDirPath) && Files.isDirectory(configDirPath)) {
            enterpriseLogger.info("|  * Configuration folder: '{}'", configDirPath);
            loadEnterpriseConfiguration(configDirPath);
            initEnterpriseObjects();
        } else {
            errorMessage = "No valid configuration directory provided: '" + configDirPath.toString() + "'";
            enterpriseLogger.error(errorMessage);
            throw new IllegalStateException(errorMessage);
        }

        enterpriseLogger.info("| EnterpriseOpenCGAWSServer successfully started!");
        enterpriseLogger.info("| - Version {}", GitRepositoryState.getInstance().getBuildVersion());
        enterpriseLogger.info("| - Git version: {} {}", GitRepositoryState.getInstance().getBranch(), GitRepositoryState.getInstance().getCommitId());
        enterpriseLogger.info("========================================================================\n");

    }
    
    /**
     * This method loads Enterprise OpenCGA configuration files. This must be only executed once.
     *
     * @param configDir directory containing the configuration files
     */
    private static void loadEnterpriseConfiguration(java.nio.file.Path configDir) {
        try {
            enterpriseLogger.info("|  * Enterprise configuration file: '{}'", configDir.toFile().getAbsolutePath()
                    + "/enterprise-configuration.yml");
            enterpriseConfiguration = EnterpriseConfiguration
                    .load(new FileInputStream(new File(configDir.toFile().getAbsolutePath() + "/enterprise-configuration.yml")));
        } catch (Exception e) {
            errorMessage = e.getMessage();
            enterpriseLogger.error("Error while initialising EnterpriseFactory", e);
        }
    }

    /**
     * This method initialize EnterpriseFactory. This must be only executed once.
     */
    private static void initEnterpriseObjects() {
        try {
            EnterpriseFactory.init(catalogManager, enterpriseConfiguration);
        } catch (Exception e) {
            errorMessage = e.getMessage();
            enterpriseLogger.error("Error while initialising EnterpriseFactory", e);
        }
    }

}
