package com.zettagenomics.opencga.enterprise.catalog.managers;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.junit.Before;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;

public class EnterpriseEnterpriseAbstractManagerTest extends AbstractManagerTest {

    private EnterpriseConfiguration enterpriseConfiguration;

    protected EnterpriseFederationManager enterpriseFederationManager;
    protected EnterpriseProjectManager enterpriseProjectManager;
    protected EnterpriseUserManager enterpriseUserManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        enterpriseConfiguration = EnterpriseConfiguration.load(getClass().getResource("/enterprise-configuration-test.yml").openStream());
        EnterpriseFactory.init(catalogManager, enterpriseConfiguration);

        enterpriseFederationManager = EnterpriseFactory.getEnterpriseFederationManager();
        enterpriseProjectManager = EnterpriseFactory.getEnterpriseProjectManager();
        enterpriseUserManager = EnterpriseFactory.getEnterpriseUserManager();
    }

}
