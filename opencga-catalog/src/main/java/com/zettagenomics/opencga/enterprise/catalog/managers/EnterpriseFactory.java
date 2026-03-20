package com.zettagenomics.opencga.enterprise.catalog.managers;

import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.exceptions.CatalogRuntimeException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

public class EnterpriseFactory implements AutoCloseable {

    protected static Logger logger = LoggerFactory.getLogger(EnterpriseFactory.class);

    private static final AtomicReference<DBAdaptorFactory> catalogDBAdaptorFactoryRef = new AtomicReference<>();
    private static final AtomicReference<CatalogManager> catalogManagerRef = new AtomicReference<>();
    private static final AtomicReference<Configuration> configurationRef = new AtomicReference<>();

    private static final AtomicReference<EnterpriseProjectManager> projectManagerRef = new AtomicReference<>();
    private static final AtomicReference<EnterpriseAuditManager> auditManagerRef = new AtomicReference<>();

    private EnterpriseFactory() {
    }

    public static synchronized void init(CatalogManager catalogManager, Configuration configuration)
            throws CatalogDBException, CatalogIOException {
        if (catalogManagerRef.get() == null) {
            catalogManagerRef.set(catalogManager);
            configurationRef.set(configuration);

            logger.debug("Configure DBAdaptorFactory");
            CatalogIOManager catalogIOManager = new CatalogIOManager(catalogManager.getConfiguration());
            catalogDBAdaptorFactoryRef.set(new MongoDBAdaptorFactory(catalogManager.getConfiguration(),
                    catalogManager.getIoManagerFactory(), catalogIOManager));

            logger.debug("Configure Managers");
            configureManagers();
        }
    }

    public static synchronized void init(CatalogManager catalogManager, Path opencgaHome) {
        if (catalogManagerRef.get() == null) {
            logger.info("========================================================================");
            logger.info("| Initializing EnterpriseFactory");
            logger.info("| This message must appear only once.");

            try {
                catalogManagerRef.set(catalogManager);
                configurationRef.set(catalogManager.getConfiguration());

                logger.info("|  * Configuring Enterprise DBAdaptorFactory");
                CatalogIOManager catalogIOManager = new CatalogIOManager(catalogManager.getConfiguration());
                catalogDBAdaptorFactoryRef.set(new MongoDBAdaptorFactory(catalogManager.getConfiguration(),
                        catalogManager.getIoManagerFactory(), catalogIOManager));

                logger.info("|  * Configuring Enterprise Managers");
                configureManagers();
                logger.info("========================================================================\n");
            } catch (Exception e) {
                throw new CatalogRuntimeException("Unable to initialize EnterpriseFactory", e);
            }
        }
    }

    private static synchronized void configureManagers() {
        CatalogManager catalogManager = catalogManagerRef.get();

        auditManagerRef.set(new EnterpriseAuditManager(catalogManager.getAuthorizationManager(), catalogManager,
                catalogDBAdaptorFactoryRef.get(), catalogManager.getConfiguration()));
        projectManagerRef.set(new EnterpriseProjectManager(catalogManagerRef.get(), configurationRef.get()));
    }

    public static DBAdaptorFactory getCatalogDBAdaptorFactory() throws CatalogRuntimeException {
        if (catalogDBAdaptorFactoryRef.get() == null) {
            throw new CatalogRuntimeException("DBAdaptorFactor has not been properly initialized."
                    + " Please, call init() method first.");
        }
        return catalogDBAdaptorFactoryRef.get();
    }

    public static EnterpriseProjectManager getEnterpriseProjectManager() throws CatalogRuntimeException {
        if (projectManagerRef.get() == null) {
            throw new CatalogRuntimeException("EnterpriseProjectManager has not been properly initialized."
                    + " Please, call init() method first.");
        }
        return projectManagerRef.get();
    }

    public static EnterpriseAuditManager getEnterpriseAuditManager() throws CatalogRuntimeException {
        if (auditManagerRef.get() == null) {
            throw new CatalogRuntimeException("EnterpriseAuditManager has not been properly initialized."
                    + " Please, call init() method first.");
        }
        return auditManagerRef.get();
    }

    public static Configuration getEnterpriseConfiguration() throws CatalogRuntimeException {
        if (configurationRef.get() == null) {
            throw new CatalogRuntimeException("Configuration has not been properly initialized."
                    + " Please, call init() method first.");
        }
        return configurationRef.get();
    }

    @Override
    public void close() throws Exception {
        if (catalogDBAdaptorFactoryRef.get() != null) {
            catalogDBAdaptorFactoryRef.get().close();
        }
    }
}
