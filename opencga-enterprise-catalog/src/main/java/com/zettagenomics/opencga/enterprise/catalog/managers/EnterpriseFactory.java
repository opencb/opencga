package com.zettagenomics.opencga.enterprise.catalog.managers;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogRuntimeException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

public class EnterpriseFactory implements AutoCloseable {

    protected static Logger logger = LoggerFactory.getLogger(EnterpriseFactory.class);

    private static final AtomicReference<DBAdaptorFactory> catalogDBAdaptorFactoryRef = new AtomicReference<>();
    private static final AtomicReference<CatalogManager> catalogManagerRef = new AtomicReference<>();
    private static final AtomicReference<EnterpriseConfiguration> configurationRef = new AtomicReference<>();

    private static final AtomicReference<EnterpriseFederationManager> federationManagerRef = new AtomicReference<>();
    private static final AtomicReference<EnterpriseProjectManager> projectManagerRef = new AtomicReference<>();
    private static final AtomicReference<EnterpriseUserManager> userManagerRef = new AtomicReference<>();
    private static final AtomicReference<EnterpriseAuditManager> auditManagerRef = new AtomicReference<>();

    private EnterpriseFactory() {
    }

    public static synchronized void init(CatalogManager catalogManager, EnterpriseConfiguration configuration)
            throws CatalogDBException {
        if (catalogManagerRef.get() == null) {
            catalogManagerRef.set(catalogManager);
            configurationRef.set(configuration);

            logger.debug("Configure DBAdaptorFactory");
            catalogDBAdaptorFactoryRef.set(new MongoDBAdaptorFactory(catalogManager.getConfiguration(),
                    catalogManager.getIoManagerFactory()));

            logger.debug("Configure Managers");
            configureManagers();
        }
    }

    private static synchronized void configureManagers() {
        //TODO: Obtain opencga token
        String token = null;
        CatalogManager catalogManager = catalogManagerRef.get();

        auditManagerRef.set(new EnterpriseAuditManager(catalogManager.getAuthorizationManager(), catalogManager,
                catalogDBAdaptorFactoryRef.get(), catalogManager.getConfiguration()));
        userManagerRef.set(new EnterpriseUserManager(catalogManagerRef.get(), configurationRef.get(), token));
        federationManagerRef.set(new EnterpriseFederationManager(catalogManagerRef.get(), configurationRef.get()));
        projectManagerRef.set(new EnterpriseProjectManager(catalogManagerRef.get(), configurationRef.get()));
    }

    public static DBAdaptorFactory getCatalogDBAdaptorFactory() throws CatalogRuntimeException {
        if (catalogDBAdaptorFactoryRef.get() == null) {
            throw new CatalogRuntimeException("DBAdaptorFactor has not been properly initialized."
                    + " Please, call init() method first.");
        }
        return catalogDBAdaptorFactoryRef.get();
    }

    public static EnterpriseUserManager getEnterpriseUserManager() throws CatalogRuntimeException {
        if (userManagerRef.get() == null) {
            throw new CatalogRuntimeException("EnterpriseUserManager has not been properly initialized."
                    + " Please, call init() method first.");
        }
        return userManagerRef.get();
    }

    public static EnterpriseFederationManager getEnterpriseFederationManager() throws CatalogRuntimeException {
        if (federationManagerRef.get() == null) {
            throw new CatalogRuntimeException("EnterpriseFederationManager has not been properly initialized."
                    + " Please, call init() method first.");
        }
        return federationManagerRef.get();
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

    @Override
    public void close() throws Exception {
        if (catalogDBAdaptorFactoryRef.get() != null) {
            catalogDBAdaptorFactoryRef.get().close();
        }
    }
}
