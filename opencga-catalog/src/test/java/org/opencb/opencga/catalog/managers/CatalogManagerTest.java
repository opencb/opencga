/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.managers;

import com.mongodb.BasicDBObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.common.Status;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.Acl;
import org.opencb.opencga.core.models.AclEntry;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.job.*;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.sample.*;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import javax.naming.NamingException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

@Category(MediumTests.class)
public class CatalogManagerTest extends AbstractManagerTest {

    @Test
    public void createOpencgaUserTest() throws CatalogException {
        thrown.expect(CatalogException.class);
        thrown.expectMessage("forbidden");
        catalogManager.getUserManager().create(new User().setId(ParamConstants.OPENCGA_USER_ID).setName(orgOwnerUserId)
                .setOrganization(organizationId), TestParamConstants.PASSWORD, opencgaToken);
    }

    @Test
    public void createStudyFailMoreThanOneProject() throws CatalogException {
        catalogManager.getProjectManager().incrementRelease(project1, ownerToken);
        catalogManager.getProjectManager().create("1000G2", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(), ownerToken);

        // Create a new study without providing the project. It should raise an error because the user owns more than one project
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing");
        catalogManager.getStudyManager().create(null, "phasexx", null, "Phase 1", "Done", null,
                null, null, null, null, ownerToken);
    }

    @Test
    public void testAdminUserExists() throws Exception {
        String token = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        JwtPayload payload = catalogManager.getUserManager().validateToken(token);
        assertEquals(ParamConstants.OPENCGA_USER_ID, payload.getUserId());
        assertEquals(ParamConstants.ADMIN_ORGANIZATION, payload.getOrganization());
    }

    @Test
    public void testGetToken() throws Exception {
        String token = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        Map<String, Object> claims = new HashMap<>();
        claims.put("a", "hola");
        claims.put("ab", "byw");
        // Create a token valid for 1 second
        String expiringToken = catalogManager.getUserManager().getToken(ParamConstants.ADMIN_ORGANIZATION, "opencga", claims, 1L, token);
        assertEquals("opencga", catalogManager.getUserManager().validateToken(expiringToken).getUserId());
        
        String nonExpiringToken = catalogManager.getUserManager().getNonExpiringToken(ParamConstants.ADMIN_ORGANIZATION, "opencga", claims, token);
        assertEquals("opencga", catalogManager.getUserManager().validateToken(nonExpiringToken).getUserId());

        Thread.sleep(1000);
        thrown.expect(CatalogAuthenticationException.class);
        thrown.expectMessage("expired");
        assertEquals("opencga", catalogManager.getUserManager().validateToken(expiringToken).getUserId());
    }

    @Test
    public void loginWithoutOrganizationId() throws CatalogException {
        String token = catalogManager.getUserManager().login(null, ParamConstants.OPENCGA_USER_ID, TestParamConstants.ADMIN_PASSWORD).getToken();
        assertTrue(StringUtils.isNotEmpty(token));
        JwtPayload jwtPayload = new JwtPayload(token);
        assertEquals(ParamConstants.ADMIN_ORGANIZATION, jwtPayload.getOrganization());

        token = catalogManager.getUserManager().login(null, orgOwnerUserId, TestParamConstants.PASSWORD).getToken();
        assertTrue(StringUtils.isNotEmpty(token));
        jwtPayload = new JwtPayload(token);
        assertEquals(organizationId, jwtPayload.getOrganization());

        // Create a third organization
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId("other").setName("Test"), QueryOptions.empty(), opencgaToken);
        token = catalogManager.getUserManager().login(null, ParamConstants.OPENCGA_USER_ID, TestParamConstants.ADMIN_PASSWORD).getToken();
        assertTrue(StringUtils.isNotEmpty(token));
        jwtPayload = new JwtPayload(token);
        assertEquals(ParamConstants.ADMIN_ORGANIZATION, jwtPayload.getOrganization());

        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage("organization");
        catalogManager.getUserManager().login(null, orgOwnerUserId, TestParamConstants.PASSWORD);
    }

    @Test
    public void testCreateExistingUser() throws Exception {
        thrown.expect(CatalogException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.getUserManager().create(orgOwnerUserId, "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, organizationId,
                null, opencgaToken);
    }

    @Test
    public void testCreateAnonymousUser() throws Exception {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage(containsString("reserved"));
        catalogManager.getUserManager().create(ParamConstants.ANONYMOUS_USER_ID, "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD,
                organizationId, null, opencgaToken);
    }

    @Test
    public void testCreateRegisteredUser() throws Exception {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage(containsString("reserved"));
        catalogManager.getUserManager().create(ParamConstants.REGISTERED_USERS, "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, organizationId, null,
                opencgaToken);
    }

    @Test
    public void testLogin() throws Exception {
        catalogManager.getUserManager().login(organizationId, normalUserId1, TestParamConstants.PASSWORD);

        thrown.expect(CatalogAuthenticationException.class);
        thrown.expectMessage(allOf(containsString("Incorrect"), containsString("password")));
        catalogManager.getUserManager().login(organizationId, normalUserId1, "fakePassword");
    }

    @Test
    public void refreshTokenTest() throws Exception {
        String refreshToken = catalogManager.getUserManager().login(organizationId, normalUserId1, TestParamConstants.PASSWORD).getRefreshToken();
        AuthenticationResponse authenticationResponse = catalogManager.getUserManager().refreshToken(refreshToken);
        assertNotNull(authenticationResponse);
        assertNotNull(authenticationResponse.getToken());
    }

    @Test
    public void anonymousUserLoginTest() throws CatalogException {
        AuthenticationResponse authResponse = catalogManager.getUserManager().loginAnonymous(organizationId);
        assertNotNull(authResponse.getToken());

        String org2 = "otherOrg";
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(org2), QueryOptions.empty(), opencgaToken);
        catalogManager.getUserManager().create(new User().setId("userFromOrg2").setName("name").setOrganization(org2).setAccount(new Account()), TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getOrganizationManager().update(org2, new OrganizationUpdateParams().setOwner("userFromOrg2"), null, opencgaToken);
        String owner2Token = catalogManager.getUserManager().login(org2, "userFromOrg2", TestParamConstants.PASSWORD).getToken();
        Project p = catalogManager.getProjectManager().create(new ProjectCreateParams()
                        .setId("project")
                        .setOrganism(new ProjectOrganism("Homo sapiens", "GRCh38")),
                INCLUDE_RESULT, owner2Token).first();
        Study study = catalogManager.getStudyManager().create(p.getFqn(), new Study().setId("study"), INCLUDE_RESULT, owner2Token).first();

        try {
            catalogManager.getUserManager().loginAnonymous(org2);
            fail("Anonymous user should not get a token for that organization as it has not been granted any kind of access");
        } catch (Exception e) {
            assertEquals(CatalogAuthenticationException.class, e.getClass());
            assertTrue(e.getMessage().contains("not found"));
        }

        catalogManager.getStudyManager().updateGroup(study.getFqn(), ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), owner2Token);
        authResponse = catalogManager.getUserManager().loginAnonymous(org2);
        assertNotNull(authResponse.getToken());


        catalogManager.getStudyManager().updateGroup(study.getFqn(), ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Collections.singletonList("*")), owner2Token);
        thrown.expect(CatalogAuthenticationException.class);
        thrown.expectMessage("not found");
        catalogManager.getUserManager().loginAnonymous(org2);
    }

    @Test
    public void testGetUserInfo() throws CatalogException {
        // OpenCGA administrator
        DataResult<User> user = catalogManager.getUserManager().get(organizationId,
                Arrays.asList(normalUserId1, normalUserId2, normalUserId3), new QueryOptions(), opencgaToken);
        assertEquals(3, user.getNumResults());
        assertEquals(normalUserId1, user.getResults().get(0).getId());
        assertEquals(normalUserId2, user.getResults().get(1).getId());
        assertEquals(normalUserId3, user.getResults().get(2).getId());

        // Organization owner
        user = catalogManager.getUserManager().get(organizationId, Arrays.asList(normalUserId1, normalUserId2, normalUserId3),
                new QueryOptions(), ownerToken);
        assertEquals(3, user.getNumResults());
        assertEquals(normalUserId1, user.getResults().get(0).getId());
        assertEquals(normalUserId2, user.getResults().get(1).getId());
        assertEquals(normalUserId3, user.getResults().get(2).getId());

        // Organization administrator
        user = catalogManager.getUserManager().get(organizationId, Arrays.asList(normalUserId1, normalUserId2, normalUserId3),
                new QueryOptions(), orgAdminToken1);
        assertEquals(3, user.getNumResults());
        assertEquals(normalUserId1, user.getResults().get(0).getId());
        assertEquals(normalUserId2, user.getResults().get(1).getId());
        assertEquals(normalUserId3, user.getResults().get(2).getId());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("organization");
        catalogManager.getUserManager().get(organizationId, Arrays.asList(normalUserId1, normalUserId2, normalUserId3), new QueryOptions(),
                studyAdminToken1);
    }

    @Test
    public void testGetProjectsFromUserInfo() throws CatalogException {
        String userId = organizationId;
        catalogManager.getUserManager().create(userId, "test", "mail@mail.com", TestParamConstants.PASSWORD, organizationId, null,
                opencgaToken);
        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("test")), ownerToken);
        String token = catalogManager.getUserManager().login(organizationId, userId, TestParamConstants.PASSWORD).getToken();

        DataResult<User> user = catalogManager.getUserManager().get(organizationId, userId, new QueryOptions(), token);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(organizationId, normalUserId3, new QueryOptions(), normalToken3);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(organizationId, orgOwnerUserId, new QueryOptions(), ownerToken);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(organizationId, orgAdminUserId1, new QueryOptions(), orgAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(organizationId, studyAdminUserId1, new QueryOptions(), studyAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(organizationId, normalUserId1, new QueryOptions(), orgAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());


        user = catalogManager.getUserManager().get(null, normalUserId1, new QueryOptions(), normalToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(null, normalUserId3, new QueryOptions(), normalToken3);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(null, orgOwnerUserId, new QueryOptions(), ownerToken);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(null, orgAdminUserId1, new QueryOptions(), orgAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(null, studyAdminUserId1, new QueryOptions(), studyAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());

        user = catalogManager.getUserManager().get(null, normalUserId1, new QueryOptions(), orgAdminToken1);
        assertTrue(CollectionUtils.isNotEmpty(user.first().getProjects()));
        System.out.println(user.first().getProjects().size());
    }

    @Test
    public void testModifyUser() throws CatalogException, InterruptedException, IOException {
        ObjectMap params = new ObjectMap();
        String newName = "Changed Name " + RandomStringUtils.randomAlphanumeric(10);
        String newPassword = RandomStringUtils.randomAlphanumeric(10);
        String newEmail = "new@email.ac.uk";

        params.put("name", newName);
        ObjectMap attributes = new ObjectMap("myBoolean", true);
        attributes.put("value", 6);
        attributes.put("object", new BasicDBObject("id", 1234));
        params.put("attributes", attributes);

        Thread.sleep(10);

        catalogManager.getUserManager().update(orgOwnerUserId, params, null, ownerToken);
        catalogManager.getUserManager().update(orgOwnerUserId, new ObjectMap("email", newEmail), null, ownerToken);
        catalogManager.getUserManager().changePassword(organizationId, orgOwnerUserId, TestParamConstants.PASSWORD, newPassword);

        List<User> userList = catalogManager.getUserManager().get(organizationId, orgOwnerUserId, new QueryOptions(QueryOptions
                .INCLUDE, Arrays.asList(UserDBAdaptor.QueryParams.NAME.key(), UserDBAdaptor.QueryParams.EMAIL.key(),
                UserDBAdaptor.QueryParams.ATTRIBUTES.key())), ownerToken).getResults();
        User userPost = userList.get(0);
        System.out.println("userPost = " + userPost);
        assertEquals(userPost.getName(), newName);
        assertEquals(userPost.getEmail(), newEmail);

        catalogManager.getUserManager().login(organizationId, orgOwnerUserId, newPassword);
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(userPost.getAttributes().get(entry.getKey()), entry.getValue());
        }

        catalogManager.getUserManager().changePassword(organizationId, orgOwnerUserId, newPassword, TestParamConstants.PASSWORD);
        catalogManager.getUserManager().login(organizationId, orgOwnerUserId, TestParamConstants.PASSWORD);

        try {
            params = new ObjectMap();
            params.put("password", "1234321");
            catalogManager.getUserManager().update(orgOwnerUserId, params, null, ownerToken);
            fail("Expected exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        try {
            catalogManager.getUserManager().update(orgOwnerUserId, params, null, orgAdminToken1);
            fail("Expected exception");
        } catch (CatalogException e) {
            System.out.println(e);
        }
    }

    @Test
    public void testUpdateUserConfig() throws CatalogException {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        catalogManager.getUserManager().setConfig(normalUserId1, "a", map, normalToken1);

        Map<String, Object> config = (Map<String, Object>) catalogManager.getUserManager().getConfig(normalUserId1, "a", normalToken1).first();
        assertEquals(2, config.size());
        assertEquals("value1", config.get("key1"));
        assertEquals("value2", config.get("key2"));

        map = new HashMap<>();
        map.put("key2", "value3");
        catalogManager.getUserManager().setConfig(normalUserId1, "a", map, normalToken1);
        config = (Map<String, Object>) catalogManager.getUserManager().getConfig(normalUserId1, "a", normalToken1).first();
        assertEquals(1, config.size());
        assertEquals("value3", config.get("key2"));

        catalogManager.getUserManager().deleteConfig(normalUserId1, "a", normalToken1);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getUserManager().getConfig(normalUserId1, "a", normalToken1);
    }

    private String getAdminToken() throws CatalogException, IOException {
        return catalogManager.getUserManager().loginAsAdmin("admin").getToken();
    }

    @Test
    public void getGroupsTest() throws CatalogException {
        Group group = new Group("groupId", Arrays.asList(normalUserId2, normalUserId3)).setSyncedFrom(new Group.Sync("ldap", "bio"));
        catalogManager.getStudyManager().createGroup(studyFqn, group, ownerToken);

        OpenCGAResult<CustomGroup> customGroups = catalogManager.getStudyManager().getCustomGroups(studyFqn, null, ownerToken);
        assertEquals(4, customGroups.getNumResults());

        for (CustomGroup customGroup : customGroups.getResults()) {
            if (!customGroup.getUsers().isEmpty()) {
                assertTrue(StringUtils.isNotEmpty(customGroup.getUsers().get(0).getName()));
            }
        }

        customGroups = catalogManager.getStudyManager().getCustomGroups(studyFqn, group.getId(), ownerToken);
        assertEquals(1, customGroups.getNumResults());
        assertEquals(group.getId(), customGroups.first().getId());
        assertEquals(2, customGroups.first().getUsers().size());
        assertTrue(StringUtils.isNotEmpty(customGroups.first().getUsers().get(0).getName()));

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("study administrators");
        catalogManager.getStudyManager().getCustomGroups(studyFqn, group.getId(), normalToken2);
    }

    @Ignore
    @Test
    public void importLdapUsers() throws CatalogException, NamingException, IOException {
        // Action only for admins
        catalogManager.getUserManager().importRemoteEntities(organizationId, "ldap", Arrays.asList("pfurio", "imedina"), false, null, null,
                getAdminToken());
        // TODO: Validate the users have been imported
    }

    // To make this test work we will need to add a correct user and password to be able to login
    @Ignore
    @Test
    public void loginNotRegisteredUsers() throws CatalogException {
        // Action only for admins
        Group group = new Group("ldap", Collections.emptyList()).setSyncedFrom(new Group.Sync("ldap", "bio"));
        catalogManager.getStudyManager().createGroup(studyFqn, group, ownerToken);
        catalogManager.getStudyManager().updateAcl(studyFqn, "@ldap", new StudyAclParams("", "view_only"),
                ParamUtils.AclAction.SET, ownerToken);
        String token = catalogManager.getUserManager().login(organizationId, orgOwnerUserId, "password").getToken();

        assertEquals(9, catalogManager.getSampleManager().count(studyFqn, new Query(), token).getNumTotalResults());

        // We remove the permissions for group ldap
        catalogManager.getStudyManager().updateAcl(studyFqn, "@ldap", new StudyAclParams("", ""),
                ParamUtils.AclAction.RESET, this.ownerToken);

        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, new Query(), token).getNumTotalResults());
    }

    @Ignore
    @Test
    public void syncUsers() throws CatalogException {
        // Action only for admins
        String token = catalogManager.getUserManager().loginAsAdmin("admin").getToken();

        catalogManager.getUserManager().importRemoteGroupOfUsers(organizationId, "ldap", "bio", "bio", studyFqn, true, token);
        DataResult<Group> bio = catalogManager.getStudyManager().getGroup(studyFqn, "bio", this.ownerToken);

        assertEquals(1, bio.getNumResults());
        assertEquals(0, bio.first().getUserIds().size());

        catalogManager.getUserManager().syncAllUsersOfExternalGroup(organizationId, studyFqn, "ldap", token);
        bio = catalogManager.getStudyManager().getGroup(studyFqn, "bio", this.ownerToken);

        assertEquals(1, bio.getNumResults());
        assertTrue(!bio.first().getUserIds().isEmpty());
    }

    @Ignore
    @Test
    public void importLdapGroups() throws CatalogException, IOException {
        // Action only for admins
        String remoteGroup = "bio";
        String internalGroup = "test";
        catalogManager.getUserManager().importRemoteGroupOfUsers(organizationId, "ldap", remoteGroup, internalGroup, studyFqn, true, getAdminToken());

        DataResult<Group> test = catalogManager.getStudyManager().getGroup(studyFqn, "test", ownerToken);
        assertEquals(1, test.getNumResults());
        assertEquals("@test", test.first().getId());
        assertTrue(test.first().getUserIds().size() > 0);

//        internalGroup = "test1";
//        try {
//            catalogManager.getUserManager().importRemoteGroupOfUsers("ldap", remoteGroup, internalGroup, study, getAdminToken());
//            fail("Should not be possible creating another group containing the same users that belong to a different group");
//        } catch (CatalogException e) {
//            System.out.println(e.getMessage());
//        }

        remoteGroup = "bioo";
        internalGroup = "test2";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getUserManager().importRemoteGroupOfUsers(organizationId, "ldap", remoteGroup, internalGroup, studyFqn, true, getAdminToken());
    }

    @Test
    public void createEmptyGroup() throws CatalogException {
        catalogManager.getUserManager().create("test", "test", "test@mail.com", TestParamConstants.PASSWORD, organizationId, 100L, opencgaToken);
        catalogManager.getStudyManager().createGroup(studyFqn, "group_cancer_some_thing_else", null, ownerToken);
        catalogManager.getStudyManager().updateGroup(studyFqn, "group_cancer_some_thing_else", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("test")), ownerToken);
    }

    @Test
    public void testAssignPermissions() throws CatalogException {
        catalogManager.getUserManager().create("test", "test", "test@mail.com", TestParamConstants.PASSWORD, organizationId, 100L, opencgaToken);

        catalogManager.getStudyManager().createGroup(studyFqn, "group_cancer_some_thing_else", Collections.singletonList("test"), ownerToken);
        DataResult<AclEntryList<StudyPermissions.Permissions>> permissions = catalogManager.getStudyManager().updateAcl(
                studyFqn, "@group_cancer_some_thing_else",
                new StudyAclParams("", "view_only"), ParamUtils.AclAction.SET, ownerToken);
        assertEquals("@group_cancer_some_thing_else", permissions.first().getAcl().get(0).getMember());
        assertFalse(permissions.first().getAcl().get(0).getPermissions().isEmpty());

        String token = catalogManager.getUserManager().login(organizationId, "test", TestParamConstants.PASSWORD).getToken();
        DataResult<Study> studyDataResult = catalogManager.getStudyManager().get(studyFqn, QueryOptions.empty(), token);
        assertEquals(1, studyDataResult.getNumResults());
        assertTrue(studyDataResult.first().getAttributes().isEmpty());

        studyDataResult = catalogManager.getStudyManager().get(studyFqn, new QueryOptions(DBAdaptor.INCLUDE_ACLS, true), ownerToken);
        assertEquals(1, studyDataResult.getNumResults());
        assertTrue(!studyDataResult.first().getAttributes().isEmpty());
        assertTrue(studyDataResult.first().getAttributes().containsKey("OPENCGA_ACL"));
        List<Map<String, Object>> acls = (List<Map<String, Object>>) studyDataResult.first().getAttributes().get("OPENCGA_ACL");
        assertEquals(2, acls.size());
        assertTrue(acls.stream().map(x -> String.valueOf(x.get("member"))).collect(Collectors.toSet()).contains("@group_cancer_some_thing_else"));

        studyDataResult = catalogManager.getStudyManager().get(studyFqn, new QueryOptions(DBAdaptor.INCLUDE_ACLS, true), token);
        assertEquals(1, studyDataResult.getNumResults());
        assertTrue(!studyDataResult.first().getAttributes().isEmpty());
        assertTrue(studyDataResult.first().getAttributes().containsKey("OPENCGA_ACL"));
        acls = (List<Map<String, Object>>) studyDataResult.first().getAttributes().get("OPENCGA_ACL");
        assertEquals(2, acls.size());
        assertTrue(acls.stream().map(x -> String.valueOf(x.get("member"))).collect(Collectors.toSet()).contains("@group_cancer_some_thing_else"));
    }

    @Test
    public void getUserInfoTest() throws CatalogException {
        OpenCGAResult<User> result = catalogManager.getUserManager().get(organizationId, orgOwnerUserId, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first().getProjects());
        assertEquals(2, result.first().getProjects().size());

        result = catalogManager.getUserManager().get(organizationId, orgAdminUserId1, QueryOptions.empty(), orgAdminToken1);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first().getProjects());
        assertEquals(2, result.first().getProjects().size());

        result = catalogManager.getUserManager().get(organizationId, studyAdminUserId1, QueryOptions.empty(), studyAdminToken1);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first().getProjects());
        assertEquals(2, result.first().getProjects().size());

        result = catalogManager.getUserManager().get(organizationId, normalUserId1, QueryOptions.empty(), normalToken1);
        assertEquals(1, result.getNumResults());
        assertNotNull(result.first().getProjects());
        assertEquals(1, result.first().getProjects().size());
    }

    /**
     * Project methods ***************************
     */

    @Test
    public void testGetAllProjects() throws Exception {
        Query query = new Query();
        DataResult<Project> projects = catalogManager.getProjectManager().search(organizationId, query, null, ownerToken);
        assertEquals(3, projects.getNumResults());

        projects = catalogManager.getProjectManager().search(organizationId, query, null, normalToken1);
        assertEquals(1, projects.getNumResults());
    }

    @Test
    public void testCreateProject() throws Exception {

        String projectAlias = "projectAlias_ASDFASDF";

        catalogManager.getProjectManager().create(projectAlias, "Project", "", "Homo sapiens", null, "GRCh38", new
                QueryOptions(), ownerToken);

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.getProjectManager().create(projectAlias, "Project", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(), ownerToken);
    }

    @Test
    public void testModifyProject() throws CatalogException {
        String newProjectName = "ProjectName " + RandomStringUtils.randomAlphanumeric(10);
        ObjectMap options = new ObjectMap();
        options.put("name", newProjectName);
        ObjectMap attributes = new ObjectMap("myBoolean", true);
        attributes.put("value", 6);
        attributes.put("object", new ObjectMap("id", 1234));
        options.put("attributes", attributes);

        catalogManager.getProjectManager().update(project1, options, null, ownerToken);
        DataResult<Project> result = catalogManager.getProjectManager().get(project1, null, ownerToken);
        Project project = result.first();
        System.out.println(result);

        assertNotEquals("20180101120000", project.getCreationDate());
        assertEquals(newProjectName, project.getName());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(project.getAttributes().get(entry.getKey()), entry.getValue());
        }

        options = new ObjectMap();
        options.put(ProjectDBAdaptor.QueryParams.CREATION_DATE.key(), "20180101120000");
        catalogManager.getProjectManager().update(project1, options, null, ownerToken);
        project = catalogManager.getProjectManager().get(project1, null, ownerToken).first();
        assertEquals("20180101120000", project.getCreationDate());

        options = new ObjectMap();
        options.put(ProjectDBAdaptor.QueryParams.ID.key(), "newProjectId");
        catalogManager.getProjectManager().update(project1, options, null, ownerToken);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("found");
        catalogManager.getProjectManager().update(project1, options, null, ownerToken);
    }

    @Test
    public void updatePrivateParamsFromProjectTest() throws CatalogException {
        catalogManager.getProjectManager().setDatastoreVariant(projectFqn1, new DataStore(), opencgaToken);
        catalogManager.getProjectManager().setDatastoreVariant(projectFqn1, new DataStore(), ownerToken);
        catalogManager.getProjectManager().setDatastoreVariant(projectFqn1, new DataStore(), orgAdminToken1);
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("administrators");
        catalogManager.getProjectManager().setDatastoreVariant(projectFqn1, new DataStore(), normalToken1);
    }

    @Test
    public void testLimitProjects() throws CatalogException {
        for (int i = 0; i < 20; i++) {
            catalogManager.getProjectManager().create(new ProjectCreateParams()
                    .setId("project_" + i)
                    .setOrganism(new ProjectOrganism("hsapiens", "grch38")), QueryOptions.empty(), ownerToken);
            for (int j = 0; j < 2; j++) {
                catalogManager.getStudyManager().create("project_" + i, new Study().setId("study_" + i + "_" + j), QueryOptions.empty(),
                        ownerToken);
            }
        }

        OpenCGAResult<Project> results = catalogManager.getProjectManager().search(organizationId, new Query(), new QueryOptions(QueryOptions.LIMIT, 10),
                ownerToken);
        assertEquals(10, results.getNumResults());
    }

    /**
     * Study methods ***************************
     */

    @Test
    public void testModifyStudy() throws Exception {
        String newName = "Phase 1 " + RandomStringUtils.randomAlphanumeric(20);
        String newDescription = RandomStringUtils.randomAlphanumeric(500);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("key", "value");
        StudyUpdateParams updateParams = new StudyUpdateParams()
                .setName(newName)
                .setDescription(newDescription)
                .setAttributes(attributes);
        catalogManager.getStudyManager().update(studyFqn, updateParams, null, ownerToken);

        DataResult<Study> result = catalogManager.getStudyManager().get(studyFqn, null, ownerToken);
        System.out.println(result);
        Study study = result.first();
        assertEquals(study.getName(), newName);
        assertEquals(study.getDescription(), newDescription);
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(study.getAttributes().get(entry.getKey()), entry.getValue());
        }

        assertNotEquals("20180101120000", study.getCreationDate());
        catalogManager.getStudyManager().update(studyFqn, new StudyUpdateParams().setCreationDate("20180101120000"), null, ownerToken);
        study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();
        assertEquals("20180101120000", study.getCreationDate());
    }

    @Test
    public void testGetAllStudies() throws CatalogException {
        Query query = new Query();
        String projectId = catalogManager.getProjectManager().search(organizationId, query, null, ownerToken).first().getFqn();
        Study study_1 = catalogManager.getStudyManager().create(projectId, new Study().setId("study_1").setCreationDate("20150101120000")
                , null, ownerToken).first();
        assertEquals("20150101120000", study_1.getCreationDate());

        catalogManager.getStudyManager().create(projectId, "study_2", null, "study_2", "description", null, null, null, null, null, ownerToken);

        catalogManager.getStudyManager().create(projectId, "study_3", null, "study_3", "description", null, null, null, null, null, ownerToken);

        String study_4 = catalogManager.getStudyManager().create(projectId, "study_4", null, "study_4", "description", null, null, null,
                null, null, ownerToken).first().getId();

        assertEquals(new HashSet<>(Collections.singletonList(studyId)), catalogManager.getStudyManager().searchInOrganization(organizationId,
                        new Query(StudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), normalUserId1), null, ownerToken)
                .getResults().stream().map(Study::getId).collect(Collectors.toSet()));

//        catalogManager.getStudyManager().createGroup(Long.toString(study_4), "admins", normalUserId3, sessionIdUser);
        catalogManager.getStudyManager().updateGroup(study_4, "admins", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(normalUserId1)), ownerToken);
        assertEquals(new HashSet<>(Arrays.asList(studyId, "study_4")), catalogManager.getStudyManager().search(projectId,
                        new Query(StudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), normalUserId1), null, ownerToken).getResults().stream().map(Study::getId)
                .collect(Collectors.toSet()));

        assertEquals(new HashSet<>(Arrays.asList(studyId, studyId2, "study_1", "study_2", "study_3", "study_4")),
                catalogManager.getStudyManager().search(projectId, new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, ownerToken)
                        .getResults().stream().map(Study::getId).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList(studyId, studyId2, "study_1", "study_2", "study_3", "study_4")),
                catalogManager.getStudyManager().search(projectId, new Query(), null, ownerToken).getResults().stream().map(Study::getId)
                        .collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("study_1", "study_2", "study_3", "study_4")), catalogManager.getStudyManager().search(projectId, new
                        Query(StudyDBAdaptor.QueryParams.ID.key(), "~^study"), null, ownerToken).getResults().stream()
                .map(Study::getId).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList(studyId, studyId2)), catalogManager.getStudyManager().search(projectId, new Query(), null,
                        studyAdminToken1).getResults()
                .stream()
                .map(Study::getId).collect(Collectors.toSet()));
    }

    @Test
    public void testGetId() throws CatalogException {
        // Create another study with alias study3
        Study study = catalogManager.getStudyManager().create(project1, "study3", null, "Phase 3", "d", null, null, null, null,
                INCLUDE_RESULT, orgAdminToken1).first();

        List<Long> uids = catalogManager.getStudyManager().resolveIds(Arrays.asList("*"), orgOwnerUserId, organizationId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(study.getUid()));

        uids = catalogManager.getStudyManager().resolveIds(Collections.emptyList(), orgOwnerUserId, organizationId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(study.getUid()));

        uids = catalogManager.getStudyManager().resolveIds(Collections.emptyList(), orgOwnerUserId, organizationId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(study.getUid()));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList("1000G:*"), orgOwnerUserId, organizationId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(study.getUid()));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(organizationId + "@1000G:*"), orgOwnerUserId, organizationId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(study.getUid()));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(organizationId + "@1000G:phase1", organizationId + "@1000G:study3"), orgOwnerUserId, organizationId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(study.getUid()));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(organizationId + "@1000G:phase1", "study3"), orgOwnerUserId, organizationId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(study.getUid()));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(organizationId + "@1000G:study3", studyFqn), orgOwnerUserId, organizationId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(study.getUid()));

        try {
            catalogManager.getStudyManager().resolveId(null, orgOwnerUserId, organizationId);
            fail("This method should fail because it should find several studies");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("More than one study"));
        }
    }

    @Test
    public void testGetOnlyStudyUserAnonymousCanSee() throws CatalogException {
        String otherOrg = "otherOrg";
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(otherOrg).setName("Test"), QueryOptions.empty(),
                opencgaToken);
        catalogManager.getUserManager().create(new User().setId(orgOwnerUserId).setName(orgOwnerUserId).setOrganization(otherOrg),
                TestParamConstants.PASSWORD, opencgaToken);
        ownerToken = catalogManager.getUserManager().login(otherOrg, orgOwnerUserId, TestParamConstants.PASSWORD).getToken();

        catalogManager.getOrganizationManager().update(otherOrg,
                new OrganizationUpdateParams()
                        .setOwner(orgOwnerUserId),
                null, opencgaToken);
        Project project = catalogManager.getProjectManager().create("myProject", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, ownerToken).first();

        StudyManager studyManager = catalogManager.getStudyManager();

        try {
            studyManager.resolveIds(Collections.emptyList(), ParamConstants.ANONYMOUS_USER_ID, otherOrg);
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        // Create another study with alias phase3
        DataResult<Study> study = catalogManager.getStudyManager().create(project.getFqn(), "phase3", null, "Phase 3", "d", null,
                null, null, null, null, ownerToken);
        try {
            studyManager.resolveIds(Collections.emptyList(), ParamConstants.ANONYMOUS_USER_ID, otherOrg);
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        catalogManager.getStudyManager().updateGroup("phase3", ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(ParamConstants.ANONYMOUS_USER_ID)), ownerToken);

        List<Study> studies = studyManager.resolveIds(Collections.emptyList(), ParamConstants.ANONYMOUS_USER_ID, otherOrg);
        assertEquals(1, studies.size());
        assertEquals(study.first().getUid(), studies.get(0).getUid());
    }

    @Test
    public void testGetSelectedStudyUserAnonymousCanSee() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        try {
            studyManager.resolveIds(Collections.singletonList("phase3"), "*", organizationId);
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        // Create another study with alias phase3
        Study study = catalogManager.getStudyManager().create(project2, "phase3", null, "Phase 3", "d", null, null, null,
                null, INCLUDE_RESULT, orgAdminToken1).first();
        catalogManager.getStudyManager().updateGroup(study.getFqn(), ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), orgAdminToken1);

        List<Study> studies = studyManager.resolveIds(Collections.singletonList("phase3"), "*", organizationId);
        assertEquals(1, studies.size());
        assertEquals(study.getUid(), studies.get(0).getUid());
    }

    @Test
    public void testCreatePermissionRules() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList(normalUserId1, normalUserId2),
                Arrays.asList(SamplePermissions.VIEW.name(), SamplePermissions.WRITE.name()));
        DataResult<PermissionRule> permissionRulesDataResult = catalogManager.getStudyManager().createPermissionRule(
                studyFqn, Enums.Entity.SAMPLES, rules, ownerToken);
        assertEquals(1, permissionRulesDataResult.getNumResults());
        assertEquals("rules1", permissionRulesDataResult.first().getId());
        assertEquals(1, permissionRulesDataResult.first().getQuery().size());
        assertEquals(2, permissionRulesDataResult.first().getMembers().size());
        assertEquals(2, permissionRulesDataResult.first().getPermissions().size());

        // Add new permission rules object
        rules.setId("rules2");
        permissionRulesDataResult = catalogManager.getStudyManager().createPermissionRule(studyFqn, Enums.Entity.SAMPLES, rules,
                ownerToken);
        assertEquals(1, permissionRulesDataResult.getNumResults());
        assertEquals(rules, permissionRulesDataResult.first());
    }

    @Test
    public void testUpdatePermissionRulesIncorrectPermission() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList(normalUserId2, normalUserId3),
                Arrays.asList("VV", "UPDATE"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Detected unsupported");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Enums.Entity.SAMPLES, rules, ownerToken);
    }

    @Test
    public void testUpdatePermissionRulesNonExistingUser() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList(normalUserId2, "user20"),
                Arrays.asList(SamplePermissions.VIEW.name(), SamplePermissions.WRITE.name()));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Enums.Entity.SAMPLES, rules, ownerToken);
    }

    @Test
    public void testUpdatePermissionRulesNonExistingGroup() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList(normalUserId1, "@group"),
                Arrays.asList(SamplePermissions.VIEW.name(), SamplePermissions.WRITE.name()));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Enums.Entity.SAMPLES, rules, ownerToken);
    }

    @Test
    public void removeAllPermissionsToMember() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        // Assign permissions to study
        DataResult<Group> groupDataResult = studyManager.updateGroup(studyFqn, ParamConstants.MEMBERS_GROUP,
                ParamUtils.BasicUpdateAction.ADD, new GroupUpdateParams(Arrays.asList(normalUserId2, normalUserId3)), ownerToken);
        assertEquals(8, groupDataResult.first().getUserIds().size());
        assertEquals(ParamConstants.MEMBERS_GROUP, groupDataResult.first().getId());

        // Obtain all samples from study
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, new Query(), QueryOptions.empty(), ownerToken);
        assertTrue(sampleDataResult.getNumResults() > 0);

        // Assign permissions to all the samples
        SampleAclParams sampleAclParams = new SampleAclParams(null, null, null, null,
                SamplePermissions.VIEW.name() + "," + SamplePermissions.WRITE.name());
        List<String> sampleIds = sampleDataResult.getResults().stream()
                .map(Sample::getId)
                .collect(Collectors.toList());
        DataResult<AclEntryList<SamplePermissions>> sampleAclResult = catalogManager.getSampleManager().updateAcl(studyFqn,
                sampleIds, normalUserId2 + "," + normalUserId3, sampleAclParams, ParamUtils.AclAction.SET, ownerToken);
        assertEquals(sampleIds.size(), sampleAclResult.getNumResults());
        for (AclEntryList<SamplePermissions> result : sampleAclResult.getResults()) {
            assertEquals(2, result.getAcl().size());
            assertTrue(result.getAcl().stream().map(AclEntry::getMember).collect(Collectors.toList()).containsAll(Arrays.asList(normalUserId2, normalUserId3)));
            assertEquals(normalUserId2, result.getAcl().get(0).getMember());
            assertTrue(result.getAcl().get(0).getPermissions().containsAll(Arrays.asList(SamplePermissions.VIEW,
                    SamplePermissions.WRITE)));
            assertTrue(result.getAcl().get(1).getPermissions().containsAll(Arrays.asList(SamplePermissions.VIEW,
                    SamplePermissions.WRITE)));
        }

        // Remove all the permissions to both users in the study. That should also remove the permissions they had in all the samples.
        groupDataResult = studyManager.updateGroup(studyFqn, ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Arrays.asList(normalUserId2, normalUserId3)), ownerToken);
        assertEquals(6, groupDataResult.first().getUserIds().size());

        // Get sample permissions for those members
        for (Sample sample : sampleDataResult.getResults()) {
            long sampleUid = sample.getUid();
            OpenCGAResult<AclEntryList<SamplePermissions>> sampleAcl =
                    catalogManager.getAuthorizationManager().getAcl(organizationId, studyUid, sampleUid, Collections.singletonList(normalUserId2), Enums.Resource.SAMPLE, SamplePermissions.class, orgOwnerUserId);
            assertEquals(1, sampleAcl.getNumResults());
            assertEquals(1, sampleAcl.first().getAcl().size());
            assertEquals(normalUserId2, sampleAcl.first().getAcl().get(0).getMember());
            assertNull(sampleAcl.first().getAcl().get(0).getPermissions());
            sampleAcl = catalogManager.getAuthorizationManager().getAcl(organizationId, studyUid, sampleUid, Collections.singletonList(normalUserId3), Enums.Resource.SAMPLE, SamplePermissions.class, orgOwnerUserId);
            assertEquals(1, sampleAcl.getNumResults());
            assertEquals(1, sampleAcl.first().getAcl().size());
            assertEquals(normalUserId3, sampleAcl.first().getAcl().get(0).getMember());
            assertNull(sampleAcl.first().getAcl().get(0).getPermissions());
        }
    }

    @Test
    public void removeUsersFromStudies() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        // Assign permissions to study
        DataResult<Group> groupDataResult = studyManager.updateGroup(studyFqn, ParamConstants.MEMBERS_GROUP,
                ParamUtils.BasicUpdateAction.ADD, new GroupUpdateParams(Arrays.asList(normalUserId2, normalUserId3)), ownerToken);
        assertEquals(8, groupDataResult.first().getUserIds().size());
        assertEquals(ParamConstants.MEMBERS_GROUP, groupDataResult.first().getId());

        // Obtain all samples from study
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, new Query(), QueryOptions
                .empty(), ownerToken);
        assertTrue(sampleDataResult.getNumResults() > 0);

        // Assign permissions to all the samples
        SampleAclParams sampleAclParams = new SampleAclParams(null, null, null, null,
                SamplePermissions.VIEW.name() + "," + SamplePermissions.WRITE.name());
        List<String> sampleIds = sampleDataResult.getResults().stream().map(Sample::getId).collect(Collectors.toList());

        OpenCGAResult<AclEntryList<SamplePermissions>> sampleAclResult = catalogManager.getSampleManager().updateAcl(studyFqn,
                sampleIds, normalUserId2 + "," + normalUserId3, sampleAclParams, ParamUtils.AclAction.SET, ownerToken);
        assertEquals(sampleIds.size(), sampleAclResult.getNumResults());
        for (AclEntryList<SamplePermissions> result : sampleAclResult.getResults()) {
            assertEquals(2, result.getAcl().size());
            assertTrue(result.getAcl().stream().map(AclEntry::getMember).collect(Collectors.toList()).containsAll(Arrays.asList(normalUserId2, normalUserId3)));
            assertEquals(normalUserId2, result.getAcl().get(0).getMember());
            assertTrue(result.getAcl().get(0).getPermissions().containsAll(Arrays.asList(SamplePermissions.VIEW, SamplePermissions.WRITE)));
            assertTrue(result.getAcl().get(1).getPermissions().containsAll(Arrays.asList(SamplePermissions.VIEW, SamplePermissions.WRITE)));
        }

        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.MEMBERS_GROUP, ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Arrays.asList(normalUserId2, normalUserId3)), ownerToken);

        Study study3 = catalogManager.getStudyManager().resolveId(studyFqn, orgOwnerUserId, organizationId);

        OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> studyAcl = catalogManager.getAuthorizationManager()
                .getStudyAcl(organizationId, study3.getUid(), normalUserId2, orgOwnerUserId);
        assertEquals(1, studyAcl.getNumResults());
        assertEquals(1, studyAcl.first().getAcl().size());
        assertEquals(normalUserId2, studyAcl.first().getAcl().get(0).getMember());
        assertNull(studyAcl.first().getAcl().get(0).getPermissions());
        Study study1 = catalogManager.getStudyManager().resolveId(studyFqn, orgOwnerUserId, organizationId);
        studyAcl = catalogManager.getAuthorizationManager().getStudyAcl(organizationId, study1.getUid(), normalUserId3, orgOwnerUserId);
        assertEquals(1, studyAcl.getNumResults());
        assertEquals(1, studyAcl.first().getAcl().size());
        assertEquals(normalUserId3, studyAcl.first().getAcl().get(0).getMember());
        assertNull(studyAcl.first().getAcl().get(0).getPermissions());

        groupDataResult = catalogManager.getStudyManager().getGroup(studyFqn, null, ownerToken);
        for (Group group : groupDataResult.getResults()) {
            assertFalse(group.getUserIds().contains(normalUserId2));
            assertFalse(group.getUserIds().contains(normalUserId3));
        }

        for (Sample sample : sampleDataResult.getResults()) {
            OpenCGAResult<AclEntryList<SamplePermissions>> sampleAcl =
                    catalogManager.getAuthorizationManager().getAcl(organizationId, studyUid, sample.getUid(), Collections.singletonList(normalUserId2), Enums.Resource.SAMPLE, SamplePermissions.class, orgOwnerUserId);
            assertEquals(1, sampleAcl.getNumResults());
            assertEquals(1, sampleAcl.first().getAcl().size());
            assertEquals(normalUserId2, sampleAcl.first().getAcl().get(0).getMember());
            assertNull(sampleAcl.first().getAcl().get(0).getPermissions());
            sampleAcl = catalogManager.getAuthorizationManager().getAcl(organizationId, studyUid, sample.getUid(), Collections.singletonList(normalUserId3), Enums.Resource.SAMPLE, SamplePermissions.class, orgOwnerUserId);
            assertEquals(1, sampleAcl.getNumResults());
            assertEquals(1, sampleAcl.first().getAcl().size());
            assertEquals(normalUserId3, sampleAcl.first().getAcl().get(0).getMember());
            assertNull(sampleAcl.first().getAcl().get(0).getPermissions());
        }
    }

    /**
     * Job methods ***************************
     */

    @Test
    public void testCreateJob() throws CatalogException {
        String studyId = catalogManager.getStudyManager().searchInOrganization(organizationId, new Query(), null, ownerToken).first().getId();

        catalogManager.getJobManager().submit(studyId, "command-subcommand", null, Collections.emptyMap(), ownerToken);
        catalogManager.getJobManager().submit(studyId, "command-subcommand2", null, Collections.emptyMap(), ownerToken);

        catalogManager.getJobManager().create(studyId,
                new Job().setId("job1").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE))),
                QueryOptions.empty(), ownerToken);
        catalogManager.getJobManager().create(studyId,
                new Job().setId("job2").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR))),
                QueryOptions.empty(), ownerToken);
        catalogManager.getJobManager().create(studyId,
                new Job().setId("job3").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.UNREGISTERED))),
                QueryOptions.empty(), ownerToken);

        Query query = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.PENDING);
        DataResult<Job> unfinishedJobs = catalogManager.getJobManager().search(String.valueOf(studyId), query, null, ownerToken);
        assertEquals(2, unfinishedJobs.getNumResults());

        DataResult<Job> allJobs = catalogManager.getJobManager().search(String.valueOf(studyId), (Query) null, null, ownerToken);
        assertEquals(5, allJobs.getNumResults());

        thrown.expectMessage("status different");
        thrown.expect(CatalogException.class);
        catalogManager.getJobManager().create(studyId,
                new Job().setId("job5").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.PENDING))),
                QueryOptions.empty(), ownerToken);
    }

    @Test
    public void testCreateJobAndReuse() throws CatalogException {
        String project1 = catalogManager.getProjectManager().create("testCreateJobAndReuse_project1", "", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, ownerToken).first().getId();
        String project2 = catalogManager.getProjectManager().create("testCreateJobAndReuse_project2", "", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, ownerToken).first().getId();

        String study1 = catalogManager.getStudyManager().create(project1, new Study()
                .setId("studyWithDuplicatedID"), INCLUDE_RESULT, ownerToken).first().getUuid();
        String study2 = catalogManager.getStudyManager().create(project2, new Study()
                .setId("studyWithDuplicatedID"), INCLUDE_RESULT, ownerToken).first().getUuid();

//        catalogManager.getConfiguration().getAnalysis().getExecution().getOptions()
//                .put("jobs.reuse.tools", "command-subcommand");
//        String toolId = "command-subcommand";
        String toolId = "variant-index";
        String job1 = catalogManager.getJobManager().submit(study1, toolId, null, new ObjectMap("key", 1).append("key2", 2), ownerToken).first().getId();

        // Same params, different order, empty jobId
        OpenCGAResult<Job> result = catalogManager.getJobManager().submit(study1, toolId, null, new ObjectMap("key2", 2).append("key", 1),
                "", "", Collections.emptyList(), Collections.emptyList(), null, null, ownerToken);
        assertEquals(job1, result.first().getId());
        assertEquals(1, result.getEvents().size());
        assertEquals("reuse", result.getEvents().get(0).getId());

        // Same params, different values
        result = catalogManager.getJobManager().submit(study1, toolId, null, new ObjectMap("key2", 2).append("key", 2), ownerToken);
        assertNotEquals(job1, result.first().getId());

        // Same params, but with jobId
        result = catalogManager.getJobManager().submit(study1, toolId, null, new ObjectMap("key2", 2).append("key", 2), "MyJobId", "",
                Collections.emptyList(), Collections.emptyList(), null, null, ownerToken);
        assertNotEquals(job1, result.first().getId());
        assertEquals("MyJobId", result.first().getId());

        // Same params, but with dependencies
        result = catalogManager.getJobManager().submit(study1, toolId, null, new ObjectMap("key2", 2).append("key", 2), "", "",
                Collections.singletonList(job1), Collections.emptyList(), null, null, ownerToken);
        assertNotEquals(job1, result.first().getId());
    }

    @Test
    public void submitJobWithDependenciesFromDifferentStudies() throws CatalogException {
        Job first = catalogManager.getJobManager().submit(studyFqn, "command-subcommand", null, Collections.emptyMap(), ownerToken).first();
        Job second = catalogManager.getJobManager().submit(studyFqn2, "command-subcommand2", null, Collections.emptyMap(), null, "",
                Collections.singletonList(first.getUuid()), null, null, null, ownerToken).first();
        assertEquals(first.getId(), second.getDependsOn().get(0).getId());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getJobManager().submit(studyFqn2, "command-subcommand2", null, Collections.emptyMap(), null, "",
                Collections.singletonList(first.getId()), null, null, null, ownerToken);
    }

    @Test
    public void testGetAllJobs() throws CatalogException {
        Query query = new Query();
        String studyId = catalogManager.getStudyManager().searchInOrganization(organizationId, query, null, ownerToken).first().getId();

        catalogManager.getJobManager().create(studyId, new Job().setId("myErrorJob"), null, ownerToken);

        QueryOptions options = new QueryOptions(QueryOptions.COUNT, true);
        DataResult<Job> allJobs = catalogManager.getJobManager().search(studyId, null, options, ownerToken);

        assertEquals(1, allJobs.getNumMatches());
        assertEquals(1, allJobs.getNumResults());
    }

    @Test
    public void testJobsTop() throws CatalogException {
        List<String> studies = Arrays.asList(studyFqn, studyFqn2);

        for (int i = 99; i > 0; i--) {
            String studyId = studies.get(i % studies.size());
            String id = catalogManager.getJobManager().create(studyId, new Job().setId("myJob-" + i), INCLUDE_RESULT, ownerToken).first().getId();
            String status;
            switch (i % 3) {
                case 0:
                    status = Enums.ExecutionStatus.RUNNING;
                    break;
                case 1:
                    status = Enums.ExecutionStatus.DONE;
                    break;
                case 2:
                    status = Enums.ExecutionStatus.ERROR;
                    break;
                default:
                    throw new IllegalArgumentException();
            }
            catalogManager.getJobManager().update(studyId, id, new ObjectMap("internal",
                    new JobInternal(new Enums.ExecutionStatus(status))), new QueryOptions(), ownerToken);
        }

        int limit = 20;
        DataResult<JobTop> top = catalogManager.getJobManager().top(organizationId, new Query(), limit, ownerToken);

        assertEquals(1, top.getNumMatches());
        assertEquals(limit, top.first().getJobs().size());
        assertEquals(studies.size(), top.first().getJobs().stream().map(job -> job.getStudy().getId()).collect(Collectors.toSet()).size());
        assertEquals(new JobTopStats(33, 0, 0, 33, 0, 33), top.first().getStats());
    }

    @Test
    public void submitJobOwner() throws CatalogException {
        OpenCGAResult<Job> job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(),
                ownerToken);

        assertEquals(1, job.getNumResults());
        assertEquals(Enums.ExecutionStatus.PENDING, job.first().getInternal().getStatus().getId());
    }

    @Test
    public void submitJobWithDependencies() throws CatalogException {
        Job job1 = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap("param", "file1"), ownerToken).first();
        Job job2 = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap("param", "file2"), ownerToken).first();

        Job job3 = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap("param", "file3"), null, null,
                Arrays.asList(job1.getId(), job2.getId()), null, null, null, ownerToken).first();
        Job job4 = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap("param", "file4"), null, null,
                Arrays.asList(job1.getUuid(), job2.getUuid()), null, null, null, ownerToken).first();

        assertEquals(2, job3.getDependsOn().size());
        assertEquals(job1.getUuid(), job3.getDependsOn().get(0).getUuid());
        assertEquals(job2.getUuid(), job3.getDependsOn().get(1).getUuid());

        assertEquals(2, job4.getDependsOn().size());
        assertEquals(job1.getId(), job4.getDependsOn().get(0).getId());
        assertEquals(job2.getId(), job4.getDependsOn().get(1).getId());
    }

    @Test
    public void submitJobFromAdminsGroup() throws CatalogException {
        // Add user to admins group
        catalogManager.getStudyManager().updateGroup(studyFqn, "@admins", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(normalUserId3)), ownerToken);

        OpenCGAResult<Job> job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(),
                ownerToken);

        assertEquals(1, job.getNumResults());
        assertEquals(Enums.ExecutionStatus.PENDING, job.first().getInternal().getStatus().getId());
    }

    @Test
    public void submitJobWithoutPermissions() throws CatalogException {
        // Check there are no ABORTED jobs
        Query query = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.ABORTED);
        assertEquals(0, catalogManager.getJobManager().count(studyFqn, query, ownerToken).getNumMatches());

        // Grant view permissions, but no EXECUTION permission
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId3,
                new StudyAclParams("", ""), ParamUtils.AclAction.SET, ownerToken);

        try {
            catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(), normalToken3);
            fail("Submission should have failed with a message saying the user does not have EXECUTION permissions");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Permission denied"));
        }

        // The previous execution should have created an ABORTED job
        OpenCGAResult<Job> search = catalogManager.getJobManager().search(studyFqn, query, null, ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals("variant-index", search.first().getTool().getId());
    }

    @Test
    public void submitJobWithPermissions() throws CatalogException {
        // Check there are no ABORTED jobs
        Query query = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.ABORTED);
        assertEquals(0, catalogManager.getJobManager().count(studyFqn, query, ownerToken).getNumMatches());

        // Grant view permissions, but no EXECUTION permission
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId3,
                new StudyAclParams(StudyPermissions.Permissions.EXECUTE_JOBS.name(), AuthorizationManager.ROLE_VIEW_ONLY), ParamUtils.AclAction.SET, ownerToken);

        OpenCGAResult<Job> search = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(),
                orgAdminToken2);
        assertEquals(1, search.getNumResults());
        assertEquals(Enums.ExecutionStatus.PENDING, search.first().getInternal().getStatus().getId());
    }

    @Test
    public void deleteJobTest() throws CatalogException {
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId1,
                new StudyAclParams(StudyPermissions.Permissions.EXECUTE_JOBS.name(), AuthorizationManager.ROLE_VIEW_ONLY), ParamUtils.AclAction.SET, ownerToken);

        OpenCGAResult<Job> search = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(),
                normalToken1);
        assertEquals(1, search.getNumResults());
        assertEquals(Enums.ExecutionStatus.PENDING, search.first().getInternal().getStatus().getId());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("stop the job");
        catalogManager.getJobManager().delete(studyFqn, Collections.singletonList(search.first().getId()), QueryOptions.empty(), ownerToken);
    }

    @Test
    public void visitJob() throws CatalogException {
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(), ownerToken)
                .first();

        Query query = new Query(JobDBAdaptor.QueryParams.VISITED.key(), false);
        assertEquals(1, catalogManager.getJobManager().count(studyFqn, query, ownerToken).getNumMatches());

        // Now we visit the job
        catalogManager.getJobManager().visit(studyFqn, job.getId(), ownerToken);
        assertEquals(0, catalogManager.getJobManager().count(studyFqn, query, ownerToken).getNumMatches());

        query.put(JobDBAdaptor.QueryParams.VISITED.key(), true);
        assertEquals(1, catalogManager.getJobManager().count(studyFqn, query, ownerToken).getNumMatches());

        // Now we update setting job to visited false again
        JobUpdateParams updateParams = new JobUpdateParams().setVisited(false);
        catalogManager.getJobManager().update(studyFqn, job.getId(), updateParams, QueryOptions.empty(), ownerToken);
        assertEquals(0, catalogManager.getJobManager().count(studyFqn, query, ownerToken).getNumMatches());

        query = new Query(JobDBAdaptor.QueryParams.VISITED.key(), false);
        assertEquals(1, catalogManager.getJobManager().count(studyFqn, query, ownerToken).getNumMatches());
    }

    /**
     * VariableSet methods ***************************
     */

    @Test
    public void testCreateVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();
        long variableSetNum = study.getVariableSets().size();

        List<Variable> variables = new ArrayList<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), null, 0, "", "",
                        null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), null, 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), null, 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), null, 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), null, 4,
                        "", "",
                        null, Collections.<String, Object>emptyMap())
        ));
        DataResult<VariableSet> queryResult = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", true,
                false, "", null, variables, Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), ownerToken);

        assertEquals(1, queryResult.getResults().size());

        study = catalogManager.getStudyManager().get(study.getId(), null, ownerToken).first();
        assertEquals(variableSetNum + 1, study.getVariableSets().size());
    }

    @Test
    public void testCreateRepeatedVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), null, 0, "", "",
                        null,
                        Collections.<String, Object>emptyMap()),
                new Variable("NAME", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), null, 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), null, 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), null, 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), null, 4,
                        "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        thrown.expect(CatalogException.class);
        catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs1", "vs1", true, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), ownerToken);
    }

    @Test
    public void testDeleteVariableSet() throws CatalogException {
        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), null, 0, "", "",
                        null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), null, 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", true, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), ownerToken).first();

        DataResult<VariableSet> result = catalogManager.getStudyManager().deleteVariableSet(studyFqn, vs1.getId(), false, ownerToken);
        assertEquals(0, result.getNumResults());

        thrown.expect(CatalogException.class);    //VariableSet does not exist
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().getVariableSet(studyFqn, vs1.getId(), null, ownerToken);
    }

    @Test
    public void testGetAllVariableSet() throws CatalogException {
        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), null, 0, "", "",
                        null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), null, 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", true, false, "Cancer", null,
                variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), ownerToken).first();
        VariableSet vs2 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs2", "vs2", true, false, "Virgo", null,
                variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), ownerToken).first();
        VariableSet vs3 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs3", "vs3", true, false, "Piscis", null,
                variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), ownerToken).first();
        VariableSet vs4 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs4", "vs4", true, false, "Aries", null,
                variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), ownerToken).first();

        long numResults;
        numResults = catalogManager.getStudyManager().getVariableSet(studyFqn, "vs1", QueryOptions.empty(), ownerToken).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getStudyManager().getVariableSet(studyFqn, "vs2", QueryOptions.empty(), ownerToken).getNumResults();
        assertEquals(1, numResults);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().getVariableSet(studyFqn, "VS1", QueryOptions.empty(), ownerToken);
    }

    @Test
    public void testDeleteVariableSetInUse() throws CatalogException {
        String sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"),
                INCLUDE_RESULT, ownerToken).first().getId();
        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "", null,
                        Collections.emptyMap()),
                new Variable("AGE", "", "", Variable.VariableType.DOUBLE, null, false, false, Collections.singletonList("0:99"), null, 1,
                        "", "",
                        null, Collections.emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", true, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), ownerToken).first();

        Map<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "LINUS");
        catalogManager.getSampleManager().update(studyFqn, sampleId1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotationId", vs1.getId(), annotations))),
                QueryOptions.empty(), ownerToken);

        try {
            catalogManager.getStudyManager().deleteVariableSet(studyFqn, vs1.getId(), false, ownerToken);
        } finally {
            VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyFqn, vs1.getId(), null, ownerToken).first();
            assertEquals(vs1.getUid(), variableSet.getUid());

            thrown.expect(CatalogDBException.class); //Expect the exception from the try
        }
    }

    /**
     * Sample methods ***************************
     */

    /*
     * Cohort methods
     *
     */
    @Test
    public void testCreateCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort")
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3))
                .setStatus(new Status("custom", "custom", "description", TimeUtils.getTime())), INCLUDE_RESULT, ownerToken).first();

        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));
        assertNotNull(myCohort.getStatus());
        assertEquals("custom", myCohort.getStatus().getName());
        assertEquals("description", myCohort.getStatus().getDescription());
    }

    @Test
    public void createSampleCohortTest() throws Exception {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1, sampleId2)), null, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort2")
                .setSamples(Arrays.asList(sampleId2, sampleId3)), null, ownerToken).first();

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, options, ownerToken);
        assertEquals(3, sampleDataResult.getNumResults());
        for (Sample sample : sampleDataResult.getResults()) {
            switch (sample.getId()) {
                case "SAMPLE_1":
                    assertEquals(1, sample.getCohortIds().size());
                    assertEquals("MyCohort1", sample.getCohortIds().get(0));
                    break;
                case "SAMPLE_2":
                    assertEquals(2, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort1", "MyCohort2")));
                    break;
                case "SAMPLE_3":
                    assertEquals(1, sample.getCohortIds().size());
                    assertEquals("MyCohort2", sample.getCohortIds().get(0));
                    break;
                default:
                    fail();
            }
        }
    }

    @Test
    public void updateSampleCohortTest() throws Exception {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1)), null, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort2")
                .setSamples(Arrays.asList(sampleId2, sampleId3)), null, ownerToken).first();

        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(sampleId3.getId()))),
                QueryOptions.empty(), ownerToken);

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions optionsSample = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());
        QueryOptions optionsCohort = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, optionsSample, ownerToken);

        Cohort cohortResult = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", optionsCohort, ownerToken).first();

        assertEquals(2, cohortResult.getSamples().size());
        assertTrue(cohortResult.getSamples().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("SAMPLE_1", "SAMPLE_3")));

        for (Sample sample : sampleDataResult.getResults()) {

            switch (sample.getId()) {
                case "SAMPLE_1":
                    assertEquals(1, sample.getCohortIds().size());
                    assertEquals("MyCohort1", sample.getCohortIds().get(0));
                    break;
                case "SAMPLE_2":
                    assertEquals(1, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort2")));
                    break;
                case "SAMPLE_3":
                    assertEquals(2, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort1", "MyCohort2")));
                    break;
                default:
                    fail();
            }
        }
    }

    @Test
    public void deleteSampleCohortTest() throws Exception {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1)), null, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort2")
                .setSamples(Arrays.asList(sampleId2, sampleId3)), null, ownerToken).first();

        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Arrays.asList(new SampleReferenceParam().setId(sampleId3.getId()))),
                QueryOptions.empty(), ownerToken);

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(Constants.ACTIONS, actionMap);
        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Arrays.asList(new SampleReferenceParam().setId(sampleId1.getId()))),
                queryOptions, ownerToken);

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions optionsSample = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());
        QueryOptions optionsCohort = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, optionsSample, ownerToken);

        Cohort cohortResult = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", optionsCohort, ownerToken).first();

        assertEquals(1, cohortResult.getSamples().size());
        assertTrue(cohortResult.getSamples().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("SAMPLE_3")));

        for (Sample sample : sampleDataResult.getResults()) {

            switch (sample.getId()) {
                case "SAMPLE_1":
                    assertEquals(0, sample.getCohortIds().size());
                    break;
                case "SAMPLE_2":
                    assertEquals(1, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort2")));
                    break;
                case "SAMPLE_3":
                    assertEquals(2, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort1", "MyCohort2")));
                    break;
                default:
                    fail();
            }
        }
    }

    @Test
    public void setSampleCohortTest() throws Exception {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1)), null, ownerToken).first();

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(Constants.ACTIONS, actionMap);
        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Arrays.asList(
                        new SampleReferenceParam().setId(sampleId2.getId()),
                        new SampleReferenceParam().setId(sampleId3.getId()))),
                queryOptions, ownerToken);

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions optionsSample = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());
        QueryOptions optionsCohort = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, optionsSample, ownerToken);

        Cohort cohortResult = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", optionsCohort, ownerToken).first();

        assertEquals(2, cohortResult.getSamples().size());
        assertTrue(cohortResult.getSamples().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("SAMPLE_2", "SAMPLE_3")));

        for (Sample sample : sampleDataResult.getResults()) {

            switch (sample.getId()) {
                case "SAMPLE_1":
                    assertEquals(0, sample.getCohortIds().size());
                    break;
                case "SAMPLE_2":
                case "SAMPLE_3":
                    assertEquals(1, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort1")));
                    break;
                default:
                    fail();
            }
        }
    }
//    @Test
//    public void createIndividualWithSamples() throws CatalogException {
//        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().create(studyFqn, "sample1", "", "", null,
//                null, QueryOptions.empty(), sessionIdUser);
//
//        Sample oldSample = new Sample().setId(sampleDataResult.first().getId());
//        Sample newSample = new Sample().setName("sample2");
//        ServerUtils.IndividualParameters individualParameters = new ServerUtils.IndividualParameters()
//                .setName("individual").setSamples(Arrays.asList(oldSample, newSample));
//
//        long studyUid = catalogManager.getStudyManager().getId(orgOwnerUserId, "1000G:phase1");
//        // We create the individual together with the samples
//        DataResult<Individual> individualDataResult = catalogManager.getIndividualManager().create(studyFqn,
//                individualParameters, QueryOptions.empty(), sessionIdUser);
//
//        assertEquals(1, individualDataResult.getNumResults());
//        assertEquals("individual", individualDataResult.first().getName());
//
//        AbstractManager.MyResourceIds resources = catalogManager.getSampleManager().getIds("sample1,sample2", studyFqn,
//                sessionIdUser);
//
//        assertEquals(2, resources.getResourceIds().size());
//        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), resources.getResourceIds());
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key());
//        sampleDataResult = catalogManager.getSampleManager().get(studyUid, query, options, sessionIdUser);
//
//        assertEquals(2, sampleDataResult.getNumResults());
//        for (Sample sample : sampleDataResult.getResults()) {
//            assertEquals(individualDataResult.first().getId(), sample.getIndividual().getId());
//        }
//    }

    @Test
    public void testGetAllCohorts() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_4"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_5"), INCLUDE_RESULT, ownerToken).first();
        Cohort myCohort1 = catalogManager.getCohortManager().create(studyFqn,
                new Cohort().setId("MyCohort1").setType(Enums.CohortType.FAMILY).setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)),
                INCLUDE_RESULT, ownerToken).first();
        Cohort myCohort2 = catalogManager.getCohortManager().create(studyFqn,
                new Cohort().setId("MyCohort2").setType(Enums.CohortType.FAMILY)
                        .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3, sampleId4)), INCLUDE_RESULT, ownerToken).first();
        Cohort myCohort3 = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort3")
                .setType(Enums.CohortType.CASE_CONTROL).setSamples(Arrays.asList(sampleId3, sampleId4)), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort4").setType(Enums.CohortType.TRIO)
                .setSamples(Arrays.asList(sampleId5, sampleId3)), null, ownerToken).first();

        long numResults;
        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                sampleId1.getId()), new QueryOptions(), ownerToken).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                sampleId1.getId()
                        + "," + sampleId5.getId()), new QueryOptions(), ownerToken).getNumResults();
        assertEquals(3, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.ID.key(), "MyCohort2"), new
                QueryOptions(), ownerToken).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.ID.key(), "~MyCohort."), new
                QueryOptions(), ownerToken).getNumResults();
        assertEquals(4, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.TYPE.key(),
                Enums.CohortType.FAMILY), new QueryOptions(), ownerToken).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.TYPE.key(), "CASE_CONTROL"),
                new QueryOptions(), ownerToken).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.UID.key(), myCohort1.getUid() +
                "," + myCohort2.getUid() + "," + myCohort3.getUid()), new QueryOptions(), ownerToken).getNumResults();
        assertEquals(3, numResults);
    }

    @Test
    public void testCreateCohortFail() throws CatalogException {
        thrown.expect(CatalogException.class);
        List<Sample> sampleList = Arrays.asList(new Sample().setId("a"), new Sample().setId("b"), new Sample().setId("c"));
        catalogManager.getCohortManager().create(studyFqn,
                new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY).setSamples(sampleList),
                null, ownerToken);
    }

    @Test
    public void testCreateCohortAlreadyExisting() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Collections.singletonList(sampleId1)), null, ownerToken).first();

        thrown.expect(CatalogDBException.class);
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Collections.singletonList(sampleId1)), null, ownerToken).first();
    }

    @Test
    public void testUpdateCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_4"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_5"), INCLUDE_RESULT, ownerToken).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3, sampleId1)), INCLUDE_RESULT, ownerToken).first();

        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertEquals(3, myCohort.getNumSamples());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));

        QueryOptions options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.BasicUpdateAction.SET.name()));

        DataResult<Cohort> result = catalogManager.getCohortManager().update(studyFqn, myCohort.getId(),
                new CohortUpdateParams()
                        .setId("myModifiedCohort")
                        .setSamples(Arrays.asList(
                                new SampleReferenceParam().setId(sampleId1.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()),
                                new SampleReferenceParam().setId(sampleId4.getId()),
                                new SampleReferenceParam().setId(sampleId5.getId()))),
                options, ownerToken);
        assertEquals(1, result.getNumUpdated());

        Cohort myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), ownerToken).first();
        assertEquals("myModifiedCohort", myModifiedCohort.getId());
        assertEquals(4, myModifiedCohort.getSamples().size());
        assertEquals(4, myModifiedCohort.getNumSamples());
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId4.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId5.getUid()));

        QueryOptions options1 = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.NUM_SAMPLES.key());
        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", options1, ownerToken).first();
        assertEquals(4, myModifiedCohort.getNumSamples());
        assertNull(myModifiedCohort.getSamples());

        options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.BasicUpdateAction.SET.name()));
        result = catalogManager.getCohortManager().update(studyFqn, myModifiedCohort.getId(),
                new CohortUpdateParams()
                        .setSamples(Collections.emptyList()),
                options, ownerToken);
        assertEquals(1, result.getNumUpdated());

        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), ownerToken).first();
        assertEquals(0, myModifiedCohort.getSamples().size());
        assertEquals(0, myModifiedCohort.getNumSamples());

        options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.BasicUpdateAction.ADD.name()));
        result = catalogManager.getCohortManager().update(studyFqn, myModifiedCohort.getId(),
                new CohortUpdateParams()
                        .setSamples(Arrays.asList(
                                new SampleReferenceParam().setId(sampleId1.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()),
                                new SampleReferenceParam().setId(sampleId1.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()))),
                options, ownerToken);
        assertEquals(1, result.getNumUpdated());
        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), ownerToken).first();
        assertEquals(2, myModifiedCohort.getSamples().size());
        assertEquals(2, myModifiedCohort.getNumSamples());

        options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.BasicUpdateAction.REMOVE.name()));
        result = catalogManager.getCohortManager().update(studyFqn, myModifiedCohort.getId(),
                new CohortUpdateParams()
                        .setSamples(Arrays.asList(
                                new SampleReferenceParam().setId(sampleId3.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()))),
                options, ownerToken);
        assertEquals(1, result.getNumUpdated());
        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), ownerToken).first();
        assertEquals(1, myModifiedCohort.getSamples().size());
        assertEquals(1, myModifiedCohort.getNumSamples());
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
    }

    /*                    */
    /* Test util methods  */
    /*                    */

    @Test
    public void testDeleteCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), INCLUDE_RESULT, ownerToken).first();

        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));

        DataResult deleteResult = catalogManager.getCohortManager().delete(studyFqn,
                new Query(CohortDBAdaptor.QueryParams.UID.key(), myCohort.getUid()), null, ownerToken);
        assertEquals(1, deleteResult.getNumDeleted());

        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.UID.key(), myCohort.getUid())
                .append(CohortDBAdaptor.QueryParams.DELETED.key(), true);
        Cohort cohort = catalogManager.getCohortManager().search(studyFqn, query, null, ownerToken).first();
        assertEquals(InternalStatus.DELETED, cohort.getInternal().getStatus().getId());
    }

    @Test
    public void getSamplesFromCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT,
                ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT,
                ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT,
                ownerToken).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), INCLUDE_RESULT, ownerToken).first();

        DataResult<Sample> myCohort1 = catalogManager.getCohortManager().getSamples(studyFqn, "MyCohort", ownerToken);
        assertEquals(3, myCohort1.getNumResults());

        thrown.expect(CatalogParameterException.class);
        catalogManager.getCohortManager().getSamples(studyFqn, "MyCohort,AnotherCohort", ownerToken);

        thrown.expect(CatalogParameterException.class);
        catalogManager.getCohortManager().getSamples(studyFqn, "MyCohort,MyCohort", ownerToken);
    }

    @Test
    public void testDeleteVariableSetWithAnnotations() throws CatalogException {
        VariableSet variableSet = createVariableSetAndAnnotationSets();
        thrown.expect(CatalogException.class);
        thrown.expectMessage("in use");
        catalogManager.getStudyManager().deleteVariableSet(studyFqn, variableSet.getId(), false, ownerToken);
    }

    private VariableSet createVariableSetAndAnnotationSets() throws CatalogException {
        VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyFqn, "vs", null, ownerToken).first();

        String individualId1 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_1")
                        .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                INCLUDE_RESULT, ownerToken).first().getId();
        String individualId2 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_2")
                        .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                INCLUDE_RESULT, ownerToken).first().getId();
        String individualId3 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_3")
                        .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                INCLUDE_RESULT, ownerToken).first().getId();

        catalogManager.getIndividualManager().update(studyFqn, individualId1, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", variableSet.getId(),
                                new ObjectMap("NAME", "INDIVIDUAL_1").append("AGE", 5).append("PHEN", "CASE").append("ALIVE", true)))),
                QueryOptions.empty(), ownerToken);

        catalogManager.getIndividualManager().update(studyFqn, individualId2, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", variableSet.getId(),
                                new ObjectMap("NAME", "INDIVIDUAL_2").append("AGE", 15).append("PHEN", "CONTROL").append("ALIVE", true)))),
                QueryOptions.empty(), ownerToken);

        catalogManager.getIndividualManager().update(studyFqn, individualId3, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", variableSet.getId(),
                                new ObjectMap("NAME", "INDIVIDUAL_3").append("AGE", 25).append("PHEN", "CASE").append("ALIVE", true)))),
                QueryOptions.empty(), ownerToken);

        QueryOptions options = new QueryOptions()
                .append(QueryOptions.LIMIT, 0)
                .append(QueryOptions.COUNT, true);
        OpenCGAResult<?> result = catalogManager.getIndividualManager().search(studyFqn,
                new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId()), options,
                ownerToken);
        assertEquals(3, result.getNumMatches());

        result = catalogManager.getSampleManager().search(studyFqn,
                new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId()), options,
                ownerToken);
        assertEquals(8, result.getNumMatches());
        return variableSet;
    }

    @Test
    public void testForceDeleteVariableSetWithAnnotations() throws CatalogException {
        VariableSet variableSet = createVariableSetAndAnnotationSets();
        catalogManager.getStudyManager().deleteVariableSet(studyFqn, variableSet.getId(), true, ownerToken);

        QueryOptions options = new QueryOptions()
                .append(QueryOptions.LIMIT, 0)
                .append(QueryOptions.COUNT, true);
        try {
            catalogManager.getIndividualManager().search(studyFqn,
                    new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId()), options,
                    ownerToken);
            fail("It should fail saying Variable set does not exist");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        try {
            catalogManager.getSampleManager().search(studyFqn,
                    new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId()), options,
                    ownerToken);
            fail("It should fail saying Variable set does not exist");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        try {
            catalogManager.getStudyManager().getVariableSet(studyFqn, variableSet.getId(), QueryOptions.empty(), ownerToken);
            fail("It should fail saying Variable set does not exist");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("not found"));
        }
    }

    @Test
    public void generateCohortFromSampleQuery() throws CatalogException, IOException {
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken);

        Query query = new Query();
        Cohort myCohort = catalogManager.getCohortManager().generate(studyFqn, query, new Cohort().setId("MyCohort"), INCLUDE_RESULT, ownerToken).first();
        assertEquals(12, myCohort.getSamples().size());

        query = new Query(SampleDBAdaptor.QueryParams.ID.key(), "~^SAM");
        myCohort = catalogManager.getCohortManager().generate(studyFqn, query, new Cohort()
                .setId("MyCohort2")
                .setStatus(new Status("custom", "custom", "description", TimeUtils.getTime())), INCLUDE_RESULT, ownerToken).first();
        assertEquals(3, myCohort.getSamples().size());
        assertNotNull(myCohort.getStatus());
        assertEquals("custom", myCohort.getStatus().getName());
        assertEquals("description", myCohort.getStatus().getDescription());
    }

    // Effective permissions testing
    @Test
    public void getEffectivePermissionsNoAdmins() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("study administrators");
        catalogManager.getAdminManager().getEffectivePermissions(studyFqn, Arrays.asList(s_1Id, s_2Id), Enums.Resource.SAMPLE.name(),
                normalToken1);
    }

    @Test
    public void getEffectivePermissionsMissingEntries() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage("entry id list");
        catalogManager.getAdminManager().getEffectivePermissions(studyFqn, Collections.emptyList(), "sampl", ownerToken);
    }

    @Test
    public void getEffectivePermissionsWrongCategory() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage("category");
        catalogManager.getAdminManager().getEffectivePermissions(studyFqn, Collections.singletonList(s_1Id), "sampl", ownerToken);
    }

    @Test
    public void getEffectivePermissionsWrongPermission() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage("permission");
        catalogManager.getAdminManager().getEffectivePermissions(studyFqn, Collections.singletonList(s_1Id), Collections.singletonList("VIE"),
                Enums.Resource.SAMPLE.name(), ownerToken);
    }

    @Test
    public void getEffectivePermissions() throws CatalogException {
        OpenCGAResult<Acl> aclList = catalogManager.getAdminManager().getEffectivePermissions(studyFqn, Collections.singletonList(s_1Id),
                Collections.singletonList(SamplePermissions.VIEW.name()), Enums.Resource.SAMPLE.name(), ownerToken);
        assertEquals(1, aclList.getNumResults());
        assertEquals(s_1Id, aclList.first().getId());
        assertEquals(1, aclList.first().getPermissions().size());
        assertEquals(SamplePermissions.VIEW.name(), aclList.first().getPermissions().get(0).getId());
        assertEquals(7, aclList.first().getPermissions().get(0).getUserIds().size());
        assertTrue(Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3)
                .containsAll(aclList.first().getPermissions().get(0).getUserIds()));

        aclList = catalogManager.getAdminManager().getEffectivePermissions(studyFqn, Collections.singletonList(s_1Id),
                Enums.Resource.SAMPLE.name(), ownerToken);
        assertEquals(1, aclList.getNumResults());
        assertEquals(s_1Id, aclList.first().getId());
        assertEquals(8, aclList.first().getPermissions().size());
        for (Acl.Permission permission : aclList.first().getPermissions()) {
            if ("NONE".equals(permission.getId())) {
                assertEquals(1, permission.getUserIds().size());
                assertEquals(ParamConstants.ANONYMOUS_USER_ID, permission.getUserIds().get(0));
            } else {
                if (permission.getId().startsWith("VIEW") || permission.getId().startsWith("WRITE")) {
                    assertEquals(7, permission.getUserIds().size());
                    assertTrue(Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3)
                            .containsAll(permission.getUserIds()));
                } else {
                    // DELETE
                    assertTrue(permission.getId().startsWith("DELETE"));
                    assertEquals(6, permission.getUserIds().size());
                    assertTrue(Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId2, normalUserId3)
                            .containsAll(permission.getUserIds()));
                }
            }
        }

        generatePermissionScenario();
        aclList = catalogManager.getAdminManager().getEffectivePermissions(studyFqn, Arrays.asList(s_7Id, s_8Id, s_9Id),
                Enums.Resource.SAMPLE.name(), ownerToken);
        assertEquals(3, aclList.getNumResults());
        assertPermissions(s_7Id, Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user4", "user5", "user6", "user7"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, "user6", "user7"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6"),
                Collections.singletonList(ParamConstants.ANONYMOUS_USER_ID), aclList.getResults().get(0));
        assertPermissions(s_8Id, Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, "user4", "user5", "user6", "user7"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, "user6", "user7"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6"),
                Arrays.asList(normalUserId3, ParamConstants.ANONYMOUS_USER_ID), aclList.getResults().get(1));
        assertPermissions(s_9Id, Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user5", "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId3, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user5", "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId3, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user5", "user6"),
                Arrays.asList(ParamConstants.ANONYMOUS_USER_ID, "user4", "user7"), aclList.getResults().get(2));

        // Now, we grant view_only access to anonymous user
        catalogManager.getStudyManager().updateAcl(studyFqn, ParamConstants.ANONYMOUS_USER_ID, new StudyAclParams(null, "view_only"),
                ParamUtils.AclAction.SET, ownerToken);

        aclList = catalogManager.getAdminManager().getEffectivePermissions(studyFqn, Arrays.asList(s_7Id, s_8Id, s_9Id), Enums.Resource.SAMPLE.name(), ownerToken);
        assertEquals(3, aclList.getNumResults());
        assertPermissions(s_7Id, Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user4", "user5", "user6", "user7", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, "user6", "user7"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Collections.emptyList(), aclList.getResults().get(0));
        assertPermissions(s_8Id, Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, "user4", "user5", "user6", "user7", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, "user6", "user7"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Collections.singletonList(normalUserId3), aclList.getResults().get(1));
        assertPermissions(s_9Id, Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId3, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId3, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList("user4", "user7"), aclList.getResults().get(2));

        catalogManager.getConfiguration().getOptimizations().setSimplifyPermissions(true);
        aclList = catalogManager.getAdminManager().getEffectivePermissions(studyFqn, Arrays.asList(s_7Id, s_8Id, s_9Id), Enums.Resource.SAMPLE.name(), ownerToken);
        assertEquals(3, aclList.getNumResults());
        assertPermissions(s_7Id, Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user4", "user5", "user6", "user7", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, "user6", "user7"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Collections.emptyList(), aclList.getResults().get(0));
        assertPermissions(s_8Id, Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, "user4", "user5", "user6", "user7", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, "user6", "user7"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, "user5", "user6", ParamConstants.ANONYMOUS_USER_ID),
                Collections.singletonList(normalUserId3), aclList.getResults().get(1));
        assertPermissions(s_9Id, Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user4", "user5", "user6", "user7", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId3, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user4", "user5", "user6", "user7", ParamConstants.ANONYMOUS_USER_ID),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId3, "user6"),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1),
                Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2, studyAdminUserId1, normalUserId1, normalUserId2, normalUserId3, "user4", "user5", "user6", "user7", ParamConstants.ANONYMOUS_USER_ID),
                Collections.emptyList(), aclList.getResults().get(2));

    }

    private void assertPermissions(String id, List<String> view, List<String> write, List<String> delete, List<String> viewAnnots,
                                   List<String> writeAnnots, List<String> deleteAnnots, List<String> viewVariants, List<String> none,
                                   Acl acl) {
        assertEquals(id, acl.getId());
        for (Acl.Permission permission : acl.getPermissions()) {
            SamplePermissions samplePermission = SamplePermissions.valueOf(permission.getId());
            switch (samplePermission) {
                case NONE:
                    assertEquals("Sample " + id + ": " + SamplePermissions.NONE + " permission", none.size(), permission.getUserIds().size());
                    assertTrue("Sample " + id + ": " + SamplePermissions.NONE + " permission", permission.getUserIds().containsAll(none));
                    break;
                case VIEW:
                    assertEquals("Sample " + id + ": " + SamplePermissions.VIEW + " permission", view.size(), permission.getUserIds().size());
                    assertTrue("Sample " + id + ": " + SamplePermissions.VIEW + " permission", permission.getUserIds().containsAll(view));
                    break;
                case WRITE:
                    assertEquals("Sample " + id + ": " + SamplePermissions.WRITE + " permission", write.size(), permission.getUserIds().size());
                    assertTrue("Sample " + id + ": " + SamplePermissions.WRITE + " permission", permission.getUserIds().containsAll(write));
                    break;
                case DELETE:
                    assertEquals("Sample " + id + ": " + SamplePermissions.DELETE + " permission", delete.size(), permission.getUserIds().size());
                    assertTrue("Sample " + id + ": " + SamplePermissions.DELETE + " permission", permission.getUserIds().containsAll(delete));
                    break;
                case VIEW_ANNOTATIONS:
                    assertEquals("Sample " + id + ": " + SamplePermissions.VIEW_ANNOTATIONS + " permission", viewAnnots.size(), permission.getUserIds().size());
                    assertTrue("Sample " + id + ": " + SamplePermissions.VIEW_ANNOTATIONS + " permission", permission.getUserIds().containsAll(viewAnnots));
                    break;
                case WRITE_ANNOTATIONS:
                    assertEquals("Sample " + id + ": " + SamplePermissions.WRITE_ANNOTATIONS + " permission", writeAnnots.size(), permission.getUserIds().size());
                    assertTrue("Sample " + id + ": " + SamplePermissions.WRITE_ANNOTATIONS + " permission", permission.getUserIds().containsAll(writeAnnots));
                    break;
                case DELETE_ANNOTATIONS:
                    assertEquals("Sample " + id + ": " + SamplePermissions.DELETE_ANNOTATIONS + " permission", deleteAnnots.size(), permission.getUserIds().size());
                    assertTrue("Sample " + id + ": " + SamplePermissions.DELETE_ANNOTATIONS + " permission", permission.getUserIds().containsAll(deleteAnnots));
                    break;
                case VIEW_VARIANTS:
                    assertEquals("Sample " + id + ": " + SamplePermissions.VIEW_VARIANTS + " permission", viewVariants.size(), permission.getUserIds().size());
                    assertTrue("Sample " + id + ": " + SamplePermissions.VIEW_VARIANTS + " permission", permission.getUserIds().containsAll(viewVariants));
                    break;
            }
        }
    }

    private void generatePermissionScenario() throws CatalogException {
        /*
        Study groups - permissions - users
            @members -                                - user1
            @view   - VIEW_ONLY  (annots included)    - user2, user7
            @write  - VIEW_WRITE (annots included)    - user3
        Study permissions to users
           user4  - NONE
           user5  - VIEW       (annots included)
           user6  - VIEW_WRITE (annots included)
           user7  - NONE

        Sample s_7 permissions
        ======================
        Group permissions
            @view - WRITE
            @write - NONE
        User permissions
            user3, user4 - VIEW

        Sample s_8 permissions
        ======================
        Group permissions
            @view - WRITE
            @write - NONE
        User permissions
            user4 - VIEW

        Sample s_9 permissions
        ======================
        No permissions assigned
         */
        catalogManager.getUserManager().create("user4", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);
        catalogManager.getUserManager().create("user5", "User2 Name", "mail2@ebi.ac.uk", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);
        catalogManager.getUserManager().create("user6", "User3 Name", "user.2@e.mail", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);
        catalogManager.getUserManager().create("user7", "User3 Name", "user.2@e.mail", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);

        catalogManager.getStudyManager().createGroup(studyFqn, "@view", Arrays.asList(normalUserId2, "user7"), ownerToken);
        catalogManager.getStudyManager().createGroup(studyFqn, "@write", Collections.singletonList(normalUserId3), ownerToken);

        catalogManager.getStudyManager().updateAcl(studyFqn, "@view,user5", new StudyAclParams(null, "view_only"), ParamUtils.AclAction.SET, ownerToken);
        catalogManager.getStudyManager().updateAcl(studyFqn, "@write,user6", new StudyAclParams(null, "analyst"), ParamUtils.AclAction.SET, ownerToken);
        catalogManager.getStudyManager().updateAcl(studyFqn, "user4,user7", new StudyAclParams(null, null), ParamUtils.AclAction.SET, ownerToken);

        catalogManager.getSampleManager().updateAcl(studyFqn, Arrays.asList(s_7Id, s_8Id), "@view", new SampleAclParams(null, null, null, null, "WRITE"), ParamUtils.AclAction.SET, ownerToken);
        catalogManager.getSampleManager().updateAcl(studyFqn, Arrays.asList(s_7Id, s_8Id), "@write", new SampleAclParams(null, null, null, null, null), ParamUtils.AclAction.SET, ownerToken);
        catalogManager.getSampleManager().updateAcl(studyFqn, Arrays.asList(s_7Id, s_8Id), "user4", new SampleAclParams(null, null, null, null, "VIEW"), ParamUtils.AclAction.SET, ownerToken);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(s_7Id), normalUserId3, new SampleAclParams(null, null, null, null, "VIEW"), ParamUtils.AclAction.SET, ownerToken);
    }
}