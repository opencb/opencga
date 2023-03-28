package com.zettagenomics.opencga.enterprise.app.cli.main.custom;

import com.zettagenomics.opencga.enterprise.client.rest.EnterpriseOpenCGAClient;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.session.SessionManager;
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
                .setRest(new RestConfig(Collections.singletonList(new HostConfig("OpenCGA", "http://localhost:9090/opencga")), false, null));
        SessionManager sessionManager = new SessionManager(clientConfiguration, "OpenCGA");
        EnterpriseOpenCGAClient openCGAClient = new EnterpriseOpenCGAClient(clientConfiguration);
        String appHome = getClass().getResource("/").getPath();
        EnterpriseCustomUsersCommandExecutor executor = new EnterpriseCustomUsersCommandExecutor(new ObjectMap(), "",
                clientConfiguration, sessionManager, appHome, logger, openCGAClient);
        RestResponse<ObjectMap> about = openCGAClient.getEnterpriseMetaClient().about();
        if (about.first().getNumResults() > 0) {
            System.out.println(about.firstResult().safeToString());
        } else {
            System.out.println("About error");
        }
        RestResponse<AuthenticationResponse> login = executor.login();
        System.out.println(login.firstResult());

        about = openCGAClient.getEnterpriseMetaClient().about();
        System.out.println(about.firstResult().safeToString());
    }
}