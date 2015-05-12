package org.opencb.opencga.catalog.managers;

import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by hpccoll1 on 12/05/15.
 */
public abstract class AbstractManager {

    final protected AuthenticationManager authenticationManager;
    final protected AuthorizationManager authorizationManager;
    final protected CatalogIOManagerFactory catalogIOManagerFactory;
    final protected Properties catalogProperties;

    final protected CatalogUserDBAdaptor userDBAdaptor;
    final protected CatalogStudyDBAdaptor studyDBAdaptor;
    final protected CatalogFileDBAdaptor fileDBAdaptor;
    final protected CatalogSampleDBAdaptor sampleDBAdaptor;
    final protected CatalogJobDBAdaptor jobDBAdaptor;

    protected static Logger logger = LoggerFactory.getLogger(AbstractManager.class);

    public AbstractManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                           CatalogDBAdaptor catalogDBAdaptor, CatalogIOManagerFactory ioManagerFactory,
                           Properties catalogProperties) {
        this.authorizationManager = authorizationManager;
        this.authenticationManager = authenticationManager;
        this.catalogProperties = catalogProperties;
        this.userDBAdaptor = catalogDBAdaptor.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptor.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptor.getCatalogFileDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptor.getCatalogSampleDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptor.getCatalogJobDBAdaptor();
        this.catalogIOManagerFactory = ioManagerFactory;
    }

}
