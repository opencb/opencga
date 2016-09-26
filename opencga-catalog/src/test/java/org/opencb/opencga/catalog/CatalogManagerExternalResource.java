package org.opencb.opencga.catalog;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Level;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogManagerExternalResource extends ExternalResource {

    private static CatalogManager catalogManager;
    private CatalogConfiguration catalogConfiguration;
    private Path opencgaHome;


    public CatalogManagerExternalResource() {

        org.apache.log4j.Logger.getLogger("org.mongodb.driver.cluster").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.connection").setLevel(Level.WARN);

    }


    @Override
    public void before() throws Exception {
        opencgaHome = Paths.get("target/test-data").resolve("junit_opencga_home_" + TimeUtils.getTimeMillis() + "_" + RandomStringUtils.randomAlphabetic(3));
        Files.createDirectories(opencgaHome);
        catalogConfiguration = CatalogConfiguration.load(getClass().getResource("/catalog-configuration-test.yml").openStream());
        catalogConfiguration.setDataDir(opencgaHome.resolve("sessions").toUri().toString());
        catalogConfiguration.setTempJobsDir(opencgaHome.resolve("jobs").toUri().toString());

        catalogManager = new CatalogManager(catalogConfiguration);
        try {
            catalogManager.deleteCatalogDB(false);
        } catch (Exception ignore) {}
        clearCatalog(catalogConfiguration);
        if (!opencgaHome.toFile().exists()) {
            deleteFolderTree(opencgaHome.toFile());
            Files.createDirectory(opencgaHome);
        }
        catalogManager.installCatalogDB();
    }

    @Override
    public void after() {
        super.after();
        try {
            catalogManager.close();
        } catch (CatalogException e) {
            throw new RuntimeException(e);
        }
    }

    public CatalogConfiguration getCatalogConfiguration() {
        return catalogConfiguration;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public Path getOpencgaHome() {
        return opencgaHome;
    }

    public static void clearCatalog(CatalogConfiguration catalogConfiguration) throws IOException, CatalogException {
        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : catalogConfiguration.getDatabase().getHosts()) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddresses);

        if (catalogManager == null) {
            catalogManager = new CatalogManager(catalogConfiguration);
        }

//        MongoDataStore db = mongoManager.get(catalogConfiguration.getDatabase().getDatabase());
        MongoDataStore db = mongoManager.get(catalogManager.getCatalogDatabase());
        db.getDb().drop();
//        mongoManager.close(catalogConfiguration.getDatabase().getDatabase());
        mongoManager.close(catalogManager.getCatalogDatabase());

        Path rootdir = Paths.get(URI.create(catalogConfiguration.getDataDir()));
        deleteFolderTree(rootdir.toFile());
        if (!catalogConfiguration.getTempJobsDir().isEmpty()) {
            Path jobsDir = Paths.get(URI.create(catalogConfiguration.getTempJobsDir()));
            if (jobsDir.toFile().exists()) {
                deleteFolderTree(jobsDir.toFile());
            }
        }
    }

    public static void deleteFolderTree(java.io.File folder) {
        java.io.File[] files = folder.listFiles();
        if (files != null) {
            for (java.io.File f : files) {
                if (f.isDirectory()) {
                    deleteFolderTree(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}
