package com.zettagenomics.opencga.enterprise.app.cli.main.custom;

import com.zettagenomics.opencga.enterprise.app.cli.session.EnterpriseSessionManager;
import com.zettagenomics.opencga.enterprise.client.rest.EnterpriseOpenCGAClient;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.core.configuration.SsoConfiguration;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.config.HostConfig;
import org.opencb.opencga.client.config.RestConfig;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class EnterpriseCustomUsersCommandExecutorTest {

    @Test
    public void loginTest() throws Exception {
        Logger logger = LoggerFactory.getLogger(EnterpriseCustomUsersCommandExecutorTest.class);
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .setRest(new RestConfig(Collections.singletonList(new HostConfig("OpenCGA", "http://localhost:9090")), false, null));
        EnterpriseSessionManager sessionManager = new EnterpriseSessionManager(clientConfiguration, "OpenCGA");
        EnterpriseConfiguration enterpriseConfiguration = new EnterpriseConfiguration()
                .setSso(new SsoConfiguration(true, "https://reports.test.zettagenomics.com:8443/cas", "http://localhost:9090"));
        EnterpriseOpenCGAClient openCGAClient = new EnterpriseOpenCGAClient(clientConfiguration, enterpriseConfiguration);
        EnterpriseCustomUsersCommandExecutor executor = new EnterpriseCustomUsersCommandExecutor(new ObjectMap(), "",
                clientConfiguration, sessionManager, "", logger, openCGAClient);
        RestResponse<AuthenticationResponse> login = executor.login();
        System.out.println(login.firstResult());
    }
}