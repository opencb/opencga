package org.opencb.opencga.catalog.migration;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;

public abstract class MigrationTool {

    protected Configuration configuration;
    protected CatalogManager catalogManager;
    protected Path appHome;
    protected String token;

    protected ObjectMap params;

    protected final Logger logger;
    private final Logger privateLogger;

    public MigrationTool() {
        this.logger = LoggerFactory.getLogger(this.getClass());
        // Internal logger
        this.privateLogger = LoggerFactory.getLogger(MigrationTool.class);
    }

    public final String getId() {
        return "";
    }

    public final void setup(Configuration configuration, CatalogManager catalogManager, Path appHome, ObjectMap params, String token) {
        this.configuration = configuration;
        this.catalogManager = catalogManager;
        this.appHome = appHome;
        this.params = params;
        this.token = token;
    }

    public final void execute() throws MigrationException {
        try {
            run();
        } catch (MigrationException e) {
            throw e;
        } catch (Exception e) {
            throw new MigrationException("Error running  migration '" + getId() + "'", e);
        }
    }

    protected abstract void run() throws Exception;

    protected final StorageConfiguration readStorageConfiguration() throws MigrationException {
        try(FileInputStream is = new FileInputStream(appHome.resolve("conf").resolve("storage-configuration.yml").toFile())) {
            return StorageConfiguration.load(is);
        } catch (IOException e) {
            throw new MigrationException("Error reading \"storage-configuration.yml\"", e);
        }
    }

    protected final void runJavascript(Path file) throws MigrationException {
        String authentication = "";
        if (StringUtils.isNotEmpty(configuration.getCatalog().getDatabase().getUser())
                && StringUtils.isNotEmpty(configuration.getCatalog().getDatabase().getPassword())) {
            authentication = "-u " + configuration.getCatalog().getDatabase().getUser() + " -p "
                    + configuration.getCatalog().getDatabase().getPassword() + " --authenticationDatabase "
                    + configuration.getCatalog().getDatabase().getOptions().getOrDefault("authenticationDatabase", "admin") + " ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null
                && configuration.getCatalog().getDatabase().getOptions().containsKey(MongoDBConfiguration.SSL_ENABLED)
                && Boolean.parseBoolean(configuration.getCatalog().getDatabase().getOptions().get(MongoDBConfiguration.SSL_ENABLED))) {
            authentication += "--ssl ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null
                && configuration.getCatalog().getDatabase().getOptions().containsKey(MongoDBConfiguration.SSL_INVALID_CERTIFICATES_ALLOWED)
                && Boolean.parseBoolean(configuration.getCatalog().getDatabase().getOptions()
                .get(MongoDBConfiguration.SSL_INVALID_CERTIFICATES_ALLOWED))) {
            authentication += "--sslAllowInvalidCertificates ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null
                && configuration.getCatalog().getDatabase().getOptions().containsKey(MongoDBConfiguration.SSL_INVALID_HOSTNAME_ALLOWED)
                && Boolean.parseBoolean(configuration.getCatalog().getDatabase().getOptions()
                .get(MongoDBConfiguration.SSL_INVALID_HOSTNAME_ALLOWED))) {
            authentication += "--sslAllowInvalidHostnames ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null && StringUtils.isNotEmpty(
                configuration.getCatalog().getDatabase().getOptions().get(MongoDBConfiguration.AUTHENTICATION_MECHANISM))) {
            authentication += "--authenticationMechanism "
                    + configuration.getCatalog().getDatabase().getOptions().get(MongoDBConfiguration.AUTHENTICATION_MECHANISM) + " ";
        }

        String catalogCli = "mongo " + authentication
                + StringUtils.join(configuration.getCatalog().getDatabase().getHosts(), ",") + "/"
                + catalogManager.getCatalogDatabase() + " " + file.getFileName();

        privateLogger.info("Running Javascript cli {} from {}", catalogCli, file.getParent());
        ProcessBuilder processBuilder = new ProcessBuilder(catalogCli.split(" "));
        processBuilder.directory(file.getParent().toFile());
        Process p;
        try {
            p = processBuilder.start();
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                privateLogger.info(line);
            }
            p.waitFor();
            input.close();
        } catch (IOException | InterruptedException e) {
            throw new MigrationException("Error executing cli: " + e.getMessage(), e);
        }

        if (p.exitValue() == 0) {
            privateLogger.info("Finished Javascript catalog migration");
        } else {
            throw new MigrationException("Error with Javascript catalog migrating!");
        }
    }

}
