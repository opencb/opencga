package com.zettagenomics.opencga.enterprise.catalog.managers;

import org.junit.Before;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;

public class EnterpriseEnterpriseAbstractManagerTest extends AbstractManagerTest {

    protected EnterpriseFederationManager enterpriseFederationManager;
    protected EnterpriseProjectManager enterpriseProjectManager;
    protected EnterpriseUserManager enterpriseUserManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        EnterpriseFactory.init(catalogManager, catalogManager.getConfiguration());

        enterpriseFederationManager = EnterpriseFactory.getEnterpriseFederationManager();
        enterpriseProjectManager = EnterpriseFactory.getEnterpriseProjectManager();
        enterpriseUserManager = EnterpriseFactory.getEnterpriseUserManager();
    }

}
