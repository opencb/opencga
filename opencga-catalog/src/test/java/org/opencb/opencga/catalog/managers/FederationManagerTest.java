package org.opencb.opencga.catalog.managers;

import org.junit.Ignore;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.SecureKeyUtils;
import org.opencb.opencga.core.client.GenericClient;
import org.opencb.opencga.core.client.ParentClient;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.utils.FederationUtils;
import org.opencb.opencga.core.models.federation.FederationClientParams;
import org.opencb.opencga.core.models.federation.FederationClientUpdateParams;
import org.opencb.opencga.core.models.federation.FederationServerCreateParams;
import org.opencb.opencga.core.models.federation.FederationServerParams;
import org.opencb.opencga.core.models.federation.FederationServerUpdateParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclParams;
import org.opencb.opencga.core.models.study.StudyCreateParams;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.core.common.JwtUtils;

import javax.crypto.SecretKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FederationManagerTest extends AbstractManagerTest {

    @Ignore
    @Test
    public void federateServerTest() throws CatalogException, ClientException {
        // Create new organization with owner user
        OrganizationCreateParams organizationCreateParams = new OrganizationCreateParams()
                .setId("org2");
        catalogManager.getOrganizationManager().create(organizationCreateParams, null, opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user").setOrganization("org2"), TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getOrganizationManager().update("org2", new OrganizationUpdateParams().setOwner("user"), null, opencgaToken);

        // Create project and study
        String org2OwnerToken = catalogManager.getUserManager().login("org2", "user", TestParamConstants.PASSWORD).first().getToken();
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
//        FederationClientParams client = catalogManager.getFederationManager().createFederation("", serverCreateParams, org2OwnerToken).first();
        catalogManager.getFederationManager().createFederation("", serverCreateParams, org2OwnerToken).first();
        FederationClientParams client = new FederationClientParams();

        catalogManager.getStudyManager().updateAcl(study.getId(), organizationId, new StudyAclParams("", "view_only"),
                ParamUtils.AclAction.ADD, org2OwnerToken);

        // Check we can log in with that user
        AuthenticationResponse login = catalogManager.getUserManager().login(client.getOrganizationId(), client.getUserId(),
                client.getPassword()).first();
        System.out.println("login = " + login);

        // Grant access to the federated user to the study
        catalogManager.getStudyManager().updateGroup("study", StudyManager.MEMBERS, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(client.getUserId())), org2OwnerToken);

        client.setId("org2");
        client.setUrl("http://localhost:9090/opencga");
        client.setEmail("mail@mail.com");
        catalogManager.getFederationManager().connect(client, ownerToken);

        // Check we can access remote data
        ownerToken = catalogManager.getUserManager().refreshToken(ownerToken).first().getToken();
        ClientConfiguration clientConfiguration = new ClientConfiguration("http://localhost:9090/opencga");
        GenericClient genericClient = new GenericClient(ownerToken, clientConfiguration);
        OpenCGAResult<Study> studyOpenCGAResult = genericClient.execute("studies", "org2@project:study", null, null, "info",
                new ObjectMap(), "GET", Study.class).first();
        assertEquals(1, studyOpenCGAResult.getNumResults());
        assertEquals("org2@project:study", studyOpenCGAResult.first().getFqn());
        assertFalse(studyOpenCGAResult.first().getInternal().isFederated());
        assertFalse(studyOpenCGAResult.first().getVariableSets().isEmpty());

        ObjectMap params = new ObjectMap()
                .append("member", orgOwnerUserId);
        RestResponse<Object> execute = genericClient.execute("studies", "org2@project:study", null, null, "acl", params, ParentClient.GET,
                Object.class);
        System.out.println(execute);

        // Reset federation server access
//        client = catalogManager.getFederationManager().reset("", serverCreateParams.getId(), org2OwnerToken).first();
        catalogManager.getFederationManager().reset("", serverCreateParams.getId(), org2OwnerToken).first();

//        // Update federation client creds
        FederationClientUpdateParams updateParams = new FederationClientUpdateParams()
                .setPassword(client.getPassword())
                .setSecurityKey(client.getSecurityKey());
        catalogManager.getFederationManager().update("org2", updateParams, ownerToken);

        studyOpenCGAResult = genericClient.execute("studies", "org2@project:study", null, null, "info",
                new ObjectMap(), "GET", Study.class).first();
        assertEquals(1, studyOpenCGAResult.getNumResults());
        assertEquals("org2@project:study", studyOpenCGAResult.first().getFqn());
        assertFalse(studyOpenCGAResult.first().getInternal().isFederated());
        assertFalse(studyOpenCGAResult.first().getVariableSets().isEmpty());

        // Delete federation server
        catalogManager.getFederationManager().deleteFederationServer(serverCreateParams.getId(), org2OwnerToken);

        // Delete federation client
        catalogManager.getFederationManager().deleteFederationClient("org2", ownerToken);
    }

    @Test
    public void securityKeyCodificationTest() throws CatalogException {
        for (int i = 0; i < 10; i++) {
            String securityKey = SecureKeyUtils.generateNewSecurityKey();
            String encodedKey = SecureKeyUtils.encodeSecureString(securityKey);
            String decodedKey = SecureKeyUtils.decodeSecureString(encodedKey);
            System.out.println("securityKey = " + securityKey);
            System.out.println("encodedKey = " + encodedKey);
            assertEquals(securityKey, decodedKey);
        }

        for (int i = 0; i < 10; i++) {
            String securityKey = PasswordUtils.getStrongRandomPassword();
            String encodedKey = SecureKeyUtils.encodeSecureString(securityKey);
            String decodedKey = SecureKeyUtils.decodeSecureString(encodedKey);
            System.out.println("password = " + securityKey);
            System.out.println("encodedPassword = " + encodedKey);
            assertEquals(securityKey, decodedKey);
        }
    }

    @Test
    public void disableFederationServerTest() throws Exception {
        // Create a federation server
        FederationServerCreateParams serverCreateParams = new FederationServerCreateParams(
                "remote-server-1", "", "admin@remote.com", "remote-server-1");
        catalogManager.getFederationManager().createFederation("", serverCreateParams, ownerToken);

        // Get the federation server params (including securityKey and userId)
        Organization org = catalogManager.getOrganizationManager().get(organizationId,
                new QueryOptions(QueryOptions.INCLUDE, "federation"), ownerToken).first();
        FederationServerParams serverParams = org.getFederation().getServers().stream()
                .filter(s -> s.getId().equals("remote-server-1"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Not found"));
        assertTrue(serverParams.isActive());
        String federationUserId = serverParams.getUserId();
        String securityKey = serverParams.getSecurityKey();

        // Change the federation user's password to a known value and log in
        String knownPassword = "KnownPassword123!";
        catalogManager.getUserManager().getCatalogDBAdaptorFactory()
                .getCatalogUserDBAdaptor(organizationId).changePassword(federationUserId, null, knownPassword);
        String plainToken = catalogManager.getUserManager()
                .login(organizationId, federationUserId, knownPassword).first().getToken();

        // Encrypt the token signature with the security key (mimicking a real federation client)
        JwtUtils.Token tokenParts = JwtUtils.getToken(plainToken);
        SecretKey secretKey = CryptoUtils.stringToSecretKey(securityKey);
        String encryptedSignature = CryptoUtils.encrypt(tokenParts.getVerifySignature(), secretKey);
        String federationToken = JwtUtils.generateToken(
                tokenParts.getHeader(), tokenParts.getPayload(), encryptedSignature);

        // Verify the encrypted token is valid while server is active
        catalogManager.getUserManager().validateToken(federationToken);

        // Disable the federation server
        FederationServerUpdateParams updateParams = new FederationServerUpdateParams().setActive(false);
        catalogManager.getFederationManager().update("remote-server-1", updateParams, ownerToken);

        // Verify that token validation now fails with "currently disabled"
        CatalogException exception = assertThrows(CatalogException.class, () -> {
            catalogManager.getUserManager().validateToken(federationToken);
        });
        assertTrue(exception.getMessage().contains("currently disabled"));

        // Re-enable the server
        updateParams = new FederationServerUpdateParams().setActive(true);
        catalogManager.getFederationManager().update("remote-server-1", updateParams, ownerToken);

        // Verify that token validation works again after re-enabling
        catalogManager.getUserManager().validateToken(federationToken);
    }

    @Test
    public void disableFederationClientActiveCheckTest() throws CatalogException {
        // Create a federation server (which creates a user we can reference)
        FederationServerCreateParams serverCreateParams = new FederationServerCreateParams(
                "remote-client-1", "", "admin@remote.com", "remote-client-1");
        catalogManager.getFederationManager().createFederation("", serverCreateParams, ownerToken);

        // Manually add a federation client to simulate a connected federation
        FederationClientParams clientParams = new FederationClientParams()
                .setId("remote-client-1")
                .setDescription("Test federation client")
                .setUrl("http://remote:9090/opencga")
                .setOrganizationId("remote-org")
                .setUserId("fed-user")
                .setActive(true);

        // Encode the security key and password so findFederationClient can decode them
        clientParams.setSecurityKey(org.opencb.opencga.catalog.utils.SecureKeyUtils.encodeSecureString("test-key"));
        clientParams.setPassword(org.opencb.opencga.catalog.utils.SecureKeyUtils.encodeSecureString("test-pass"));

        ObjectMap parameters = new ObjectMap()
                .append(OrganizationDBAdaptor.QueryParams.FEDERATION_CLIENTS.key(), Collections.singletonList(clientParams));
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(OrganizationDBAdaptor.QueryParams.FEDERATION_CLIENTS.key(), ParamUtils.AddRemoveAction.ADD);
        QueryOptions options = new QueryOptions(org.opencb.opencga.catalog.utils.Constants.ACTIONS, actionMap);

        catalogManager.getUserManager().getCatalogDBAdaptorFactory()
                .getCatalogOrganizationDBAdaptor(organizationId).update(organizationId, parameters, options);

        // Verify findFederationClient works when active
        Organization org = catalogManager.getOrganizationManager().get(organizationId,
                new QueryOptions(QueryOptions.INCLUDE, "federation"), ownerToken).first();
        FederationClientParams found = FederationUtils.findFederationClient(org, "remote-client-1");
        assertEquals("remote-client-1", found.getId());

        // Disable the federation client
        FederationClientUpdateParams updateClientParams = new FederationClientUpdateParams().setActive(false);
        catalogManager.getFederationManager().update("remote-client-1", updateClientParams, ownerToken);

        // Verify findFederationClient throws when inactive
        Organization updatedOrg = catalogManager.getOrganizationManager().get(organizationId,
                new QueryOptions(QueryOptions.INCLUDE, "federation"), ownerToken).first();
        CatalogException exception = assertThrows(CatalogException.class, () -> {
            FederationUtils.findFederationClient(updatedOrg, "remote-client-1");
        });
        assertTrue(exception.getMessage().contains("currently disabled"));

        // Re-enable and verify it works again
        updateClientParams = new FederationClientUpdateParams().setActive(true);
        catalogManager.getFederationManager().update("remote-client-1", updateClientParams, ownerToken);

        Organization reenabledOrg = catalogManager.getOrganizationManager().get(organizationId,
                new QueryOptions(QueryOptions.INCLUDE, "federation"), ownerToken).first();
        found = FederationUtils.findFederationClient(reenabledOrg, "remote-client-1");
        assertEquals("remote-client-1", found.getId());
    }

}
