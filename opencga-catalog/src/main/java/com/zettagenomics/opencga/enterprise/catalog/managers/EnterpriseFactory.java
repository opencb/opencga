package com.zettagenomics.opencga.enterprise.catalog.managers;

import org.opencb.opencga.core.config.Configuration;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.exceptions.CatalogRuntimeException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.organizations.TokenConfiguration;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

public class EnterpriseFactory implements AutoCloseable {

    protected static Logger logger = LoggerFactory.getLogger(EnterpriseFactory.class);

    private static final AtomicReference<DBAdaptorFactory> catalogDBAdaptorFactoryRef = new AtomicReference<>();
    private static final AtomicReference<CatalogManager> catalogManagerRef = new AtomicReference<>();
    private static final AtomicReference<Configuration> configurationRef = new AtomicReference<>();

    private static final AtomicReference<EnterpriseFederationManager> federationManagerRef = new AtomicReference<>();
    private static final AtomicReference<EnterpriseProjectManager> projectManagerRef = new AtomicReference<>();
    private static final AtomicReference<EnterpriseUserManager> userManagerRef = new AtomicReference<>();
    private static final AtomicReference<EnterpriseAuditManager> auditManagerRef = new AtomicReference<>();

    private static final AtomicReference<String> opencgaTokenAtomicRef = new AtomicReference<>();

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
        String token = getOpencgaToken();
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

    public static Configuration getEnterpriseConfiguration() throws CatalogRuntimeException {
        if (configurationRef.get() == null) {
            throw new CatalogRuntimeException("Configuration has not been properly initialized."
                    + " Please, call init() method first.");
        }
        return configurationRef.get();
    }

    private static String getOpencgaToken() {
        String opencgaToken = opencgaTokenAtomicRef.get();
        if (opencgaToken == null) {
            synchronized (opencgaTokenAtomicRef) {
                try {
                    OpenCGAResult<Organization> result = getCatalogDBAdaptorFactory()
                            .getCatalogOrganizationDBAdaptor(ParamConstants.ADMIN_ORGANIZATION)
                            .get(OrganizationManager.INCLUDE_ORGANIZATION_CONFIGURATION);

                    if (result.getNumResults() == 0) {
                        throw new CatalogException("Organization '" + ParamConstants.ADMIN_ORGANIZATION + "' not found.");
                    }
                    Organization organization = result.first();
                    if (organization.getConfiguration() == null
                            || CollectionUtils.isEmpty(organization.getConfiguration().getAuthenticationOrigins())) {
                        throw new CatalogException("Missing authentication origin for '" + ParamConstants.ADMIN_ORGANIZATION
                                + "' organization.");
                    }
                    if (organization.getConfiguration().getToken() == null) {
                        throw new CatalogException("Internal error: Missing required information to generate"
                                + " tokens.");
                    }
                    AuthenticationOrigin authOrigin = null;
                    for (AuthenticationOrigin authenticationOrigin : organization.getConfiguration().getAuthenticationOrigins()) {
                        if (AuthenticationOrigin.AuthenticationType.OPENCGA.equals(authenticationOrigin.getType())
                                && CatalogAuthenticationManager.OPENCGA.equals(authenticationOrigin.getId())) {
                            authOrigin = authenticationOrigin;
                            break;
                        }
                    }
                    if (authOrigin == null) {
                        throw new CatalogException("Missing '" + CatalogAuthenticationManager.OPENCGA
                                + "'  authentication origin in '" + ParamConstants.ADMIN_ORGANIZATION
                                + "' organization.");
                    }
                    TokenConfiguration tokenConf = organization.getConfiguration().getToken();
                    CatalogAuthenticationManager authManager = new CatalogAuthenticationManager(getCatalogDBAdaptorFactory(),
                            null, tokenConf.getAlgorithm(), tokenConf.getSecretKey(), tokenConf.getExpiration());
                    opencgaToken = authManager.createNonExpiringToken(ParamConstants.ADMIN_ORGANIZATION,
                            ParamConstants.OPENCGA_USER_ID, null);
                    opencgaTokenAtomicRef.set(opencgaToken);
                } catch (CatalogException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
        return opencgaToken;
    }

    @Override
    public void close() throws Exception {
        if (catalogDBAdaptorFactoryRef.get() != null) {
            catalogDBAdaptorFactoryRef.get().close();
        }
    }
}
