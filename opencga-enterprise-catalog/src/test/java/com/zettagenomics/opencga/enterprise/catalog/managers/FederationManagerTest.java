package com.zettagenomics.opencga.enterprise.catalog.managers;

import org.junit.Test;
import org.opencb.biodata.formats.pubmed.v233jaxb.U;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.federation.FederationClient;
import org.opencb.opencga.core.models.federation.FederationServerCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyCreateParams;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.Collections;

import static com.zettagenomics.opencga.enterprise.catalog.EnterpriseConstants.FEDERATION_AUTH_ORIGIN_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FederationManagerTest extends EnterpriseAbstractManagerTest {

    @Test
    public void federateServerTest() throws CatalogException {
        // Create new organization with owner user
        OrganizationCreateParams organizationCreateParams = new OrganizationCreateParams()
                .setId("org2");
        catalogManager.getOrganizationManager().create(organizationCreateParams, null, opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user").setOrganization("org2"), TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getOrganizationManager().update("org2", new OrganizationUpdateParams().setOwner("user"), null, opencgaToken);

        // Create project and study
        String org2OwnerToken = catalogManager.getUserManager().login("org2", "user", TestParamConstants.PASSWORD).getToken();
        ProjectCreateParams projectCreateParams = new ProjectCreateParams()
                .setId("project")
                .setOrganism(new ProjectOrganism("hsapiens", "GRCh38"));
        catalogManager.getProjectManager().create(projectCreateParams, null, org2OwnerToken);
        Study study = new StudyCreateParams()
                .setId("study")
                .toStudy();
        catalogManager.getStudyManager().create("project", study, null, org2OwnerToken);

        // Federate server
        FederationServerCreateParams serverCreateParams = new FederationServerCreateParams(organizationId, "", "mail@mail.com",
                organizationId);
        FederationClient client = federationManager.exposeFederation(serverCreateParams, org2OwnerToken).first();

        // Check we can log in with that user
        AuthenticationResponse login = catalogManager.getUserManager().login(client.getOrganizationId(), client.getUserId(), client.getPassword());
        System.out.println("login = " + login);

        // Grant access to the federated user to the study
        catalogManager.getStudyManager().updateGroup("study", StudyManager.MEMBERS, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(client.getUserId())), org2OwnerToken);

        User user = catalogManager.getUserManager().get(client.getOrganizationId(), client.getUserId(), null, login.getToken()).first();
        assertTrue(user.getInternal().getAccount().getAuthentication().isFederation());
        assertEquals(AuthenticationOrigin.AuthenticationType.OPENCGA.name(), user.getInternal().getAccount().getAuthentication().getId());

        client.setId("org2");
        client.setUrl("http://localhost:9090/opencga");
        client.setEmail("mail@mail.com");
        federationManager.connect(client, ownerToken);
    }

}