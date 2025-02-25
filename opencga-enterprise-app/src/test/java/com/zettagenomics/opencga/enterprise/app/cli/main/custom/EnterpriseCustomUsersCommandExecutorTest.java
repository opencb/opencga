package com.zettagenomics.opencga.enterprise.app.cli.main.custom;

import org.junit.Ignore;
import org.junit.Test;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.core.config.client.HostConfig;
import org.opencb.opencga.core.config.client.RestConfig;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.user.AuthenticationResponse;

import java.util.Collections;

public class EnterpriseCustomUsersCommandExecutorTest {

    @Ignore
    @Test
    public void loginTest() throws ClientException {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setRest(new RestConfig(Collections.singletonList(new HostConfig("opencga", "https://test.app.zettagenomics.com/task-7102/opencga")), false, null));
        OpenCGAClient openCGAClient = new OpenCGAClient(clientConfiguration);
        try {
            AuthenticationResponse login = openCGAClient.login("test", "test", "Test_P4ss");
            System.out.println(login.getToken());
        } catch (Exception e) {
            throw e;
        }
    }

}