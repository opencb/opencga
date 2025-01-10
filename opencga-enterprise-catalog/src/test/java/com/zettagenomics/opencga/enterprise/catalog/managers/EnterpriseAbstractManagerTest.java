package com.zettagenomics.opencga.enterprise.catalog.managers;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.junit.Before;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;

public class EnterpriseAbstractManagerTest extends AbstractManagerTest {

    private EnterpriseConfiguration enterpriseConfiguration;

    protected FederationManager federationManager;
    protected ProjectManager projectManager;
    protected UserManager userManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        enterpriseConfiguration = EnterpriseConfiguration.load(getClass().getResource("/enterprise-configuration-test.yml").openStream());
        EnterpriseFactory.init(catalogManager, enterpriseConfiguration);

        federationManager = EnterpriseFactory.getEnterpriseFederationManager();
        projectManager = EnterpriseFactory.getEnterpriseProjectManager();
        userManager = EnterpriseFactory.getEnterpriseUserManager();
    }

}
