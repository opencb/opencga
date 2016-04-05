package org.opencb.opencga.catalog.managers;

import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by hpccoll1 on 12/05/15.
 */
public abstract class AbstractManager {

    protected static Logger logger = LoggerFactory.getLogger(AbstractManager.class);
    protected final AuthenticationManager authenticationManager;
    protected final AuthorizationManager authorizationManager;
    protected final AuditManager auditManager;
    protected final CatalogIOManagerFactory catalogIOManagerFactory;

    protected CatalogConfiguration catalogConfiguration;
    @Deprecated
    protected Properties catalogProperties;

    protected final CatalogUserDBAdaptor userDBAdaptor;
    protected final CatalogProjectDBAdaptor projectDBAdaptor;
    protected final CatalogStudyDBAdaptor studyDBAdaptor;
    protected final CatalogFileDBAdaptor fileDBAdaptor;
    protected final CatalogIndividualDBAdaptor individualDBAdaptor;
    protected final CatalogSampleDBAdaptor sampleDBAdaptor;
    protected final CatalogCohortDBAdaptor cohortDBAdaptor;
    protected final CatalogJobDBAdaptor jobDBAdaptor;

    public AbstractManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                           AuditManager auditManager, CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory
                                   ioManagerFactory, CatalogConfiguration catalogConfiguration) {
        this.authorizationManager = authorizationManager;
        this.authenticationManager = authenticationManager;
        this.auditManager = auditManager;
        this.catalogConfiguration = catalogConfiguration;
        this.userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        this.individualDBAdaptor = catalogDBAdaptorFactory.getCatalogIndividualDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        this.cohortDBAdaptor = catalogDBAdaptorFactory.getCatalogCohortDBAdaptor();
        this.catalogIOManagerFactory = ioManagerFactory;

        projectDBAdaptor = catalogDBAdaptorFactory.getCatalogProjectDbAdaptor();
    }

    @Deprecated
    public AbstractManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                           AuditManager auditManager, CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory
                                   ioManagerFactory,
                           Properties catalogProperties) {
        this.authorizationManager = authorizationManager;
        this.authenticationManager = authenticationManager;
        this.auditManager = auditManager;
        this.catalogProperties = catalogProperties;
        this.userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        this.individualDBAdaptor = catalogDBAdaptorFactory.getCatalogIndividualDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        this.cohortDBAdaptor = catalogDBAdaptorFactory.getCatalogCohortDBAdaptor();
        this.catalogIOManagerFactory = ioManagerFactory;

        projectDBAdaptor = catalogDBAdaptorFactory.getCatalogProjectDbAdaptor();
    }

}
