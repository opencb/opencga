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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.common.Status;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.job.*;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.*;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.naming.NamingException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class CatalogManagerTest extends AbstractManagerTest {

    @Test
    public void createStudyFailMoreThanOneProject() throws CatalogException {
        catalogManager.getProjectManager().incrementRelease(project1, token);
        catalogManager.getProjectManager().create("1000G2", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(), token);

        // Create a new study without providing the project. It should raise an error because the user owns more than one project
        thrown.expect(CatalogException.class);
        thrown.expectMessage("More than one project found");
        catalogManager.getStudyManager().create(null, "phasexx", null, "Phase 1", "Done", null,
                null, null, null, null, token);
    }

    @Test
    public void testAdminUserExists() throws Exception {
        String token = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        assertEquals("opencga", catalogManager.getUserManager().getUserId(token));
    }

    @Test
    public void testCreateExistingUser() throws Exception {
        thrown.expect(CatalogException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, null);
    }

    @Test
    public void testCreateAnonymousUser() throws Exception {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage(containsString("reserved"));
        catalogManager.getUserManager().create(ParamConstants.ANONYMOUS_USER_ID, "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null,
                Account.AccountType.FULL, null);
    }

    @Test
    public void testCreateRegisteredUser() throws Exception {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage(containsString("reserved"));
        catalogManager.getUserManager().create(ParamConstants.REGISTERED_USERS, "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null,
                Account.AccountType.FULL, null);
    }

    @Test
    public void testLogin() throws Exception {
        catalogManager.getUserManager().login("user", TestParamConstants.PASSWORD);

        thrown.expect(CatalogAuthenticationException.class);
        thrown.expectMessage(allOf(containsString("Incorrect"), containsString("password")));
        catalogManager.getUserManager().login("user", "fakePassword");
    }

    @Test
    public void testGetUserInfo() throws CatalogException {
        DataResult<User> user = catalogManager.getUserManager().get("user", new QueryOptions(), token);
        System.out.println("user = " + user);
        OpenCGAResult<User> result = catalogManager.getUserManager().get("user2", new QueryOptions(), token);
        assertEquals(Event.Type.ERROR, result.getEvents().get(0).getType());

        catalogManager.getStudyManager().updateGroup(studyFqn, StudyManager.MEMBERS, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("user2")), token);
        result = catalogManager.getUserManager().get("user2", new QueryOptions(), token);
        assertTrue(result.getEvents().isEmpty());
        assertTrue(StringUtils.isNotEmpty(result.first().getEmail()));
    }

    @Test
    public void testUserInfoProjections() throws CatalogException {
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.PROJECTS_ID.key());
        DataResult<User> user = catalogManager.getUserManager().get("user", options, token);
        assertNotNull(user.first().getProjects());
        assertTrue(StringUtils.isNotEmpty(user.first().getProjects().get(0).getId()));
        assertTrue(StringUtils.isEmpty(user.first().getProjects().get(0).getName()));
        assertNull(user.first().getProjects().get(0).getStudies());

        options = new QueryOptions()
                .append(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.PROJECTS.key() + ".studies."
                        + StudyDBAdaptor.QueryParams.FQN.key());
        user = catalogManager.getUserManager().get("user", options, token);
        assertNotNull(user.first().getProjects());
        assertEquals(2, user.first().getProjects().get(0).getStudies().size());
        assertTrue(StringUtils.isNotEmpty(user.first().getProjects().get(0).getStudies().get(0).getFqn()));
        assertTrue(StringUtils.isEmpty(user.first().getProjects().get(0).getStudies().get(0).getName()));

        options = new QueryOptions()
                .append(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.SHARED_PROJECTS.key() + ".studies."
                        + StudyDBAdaptor.QueryParams.FQN.key());
        user = catalogManager.getUserManager().get("user2", options, sessionIdUser2);
        assertEquals(0, user.first().getSharedProjects().size());

        // Grant permissions to user2 to access study of user1
        catalogManager.getStudyManager().updateGroup(studyFqn, StudyManager.MEMBERS, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("user2")), token);

        options = new QueryOptions()
                .append(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.SHARED_PROJECTS.key() + ".studies."
                        + StudyDBAdaptor.QueryParams.FQN.key());
        user = catalogManager.getUserManager().get("user2", options, sessionIdUser2);
        assertEquals(1, user.first().getSharedProjects().size());
        assertEquals(studyFqn, user.first().getSharedProjects().get(0).getStudies().get(0).getFqn());
        assertNull(user.first().getSharedProjects().get(0).getStudies().get(0).getId());

        options = new QueryOptions()
                .append(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.SHARED_PROJECTS.key() + ".studies."
                        + StudyDBAdaptor.QueryParams.ID.key());
        user = catalogManager.getUserManager().get("user2", options, sessionIdUser2);
        assertEquals(1, user.first().getSharedProjects().size());
        assertEquals(studyFqn, user.first().getSharedProjects().get(0).getStudies().get(0).getFqn());
        assertNotNull(user.first().getSharedProjects().get(0).getStudies().get(0).getId());
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

        catalogManager.getUserManager().update("user", params, null, token);
        catalogManager.getUserManager().update("user", new ObjectMap("email", newEmail), null, token);
        catalogManager.getUserManager().changePassword("user", TestParamConstants.PASSWORD, newPassword);

        List<User> userList = catalogManager.getUserManager().get("user", new QueryOptions(QueryOptions
                .INCLUDE, Arrays.asList(UserDBAdaptor.QueryParams.NAME.key(), UserDBAdaptor.QueryParams.EMAIL.key(),
                UserDBAdaptor.QueryParams.ATTRIBUTES.key())), token).getResults();
        User userPost = userList.get(0);
        System.out.println("userPost = " + userPost);
        assertEquals(userPost.getName(), newName);
        assertEquals(userPost.getEmail(), newEmail);

        catalogManager.getUserManager().login("user", newPassword);
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(userPost.getAttributes().get(entry.getKey()), entry.getValue());
        }

        catalogManager.getUserManager().changePassword("user", newPassword, TestParamConstants.PASSWORD);
        catalogManager.getUserManager().login("user", TestParamConstants.PASSWORD);

        try {
            params = new ObjectMap();
            params.put("password", "1234321");
            catalogManager.getUserManager().update("user", params, null, token);
            fail("Expected exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        try {
            catalogManager.getUserManager().update("user", params, null, sessionIdUser2);
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
        catalogManager.getUserManager().setConfig("user", "a", map, token);

        Map<String, Object> config = (Map<String, Object>) catalogManager.getUserManager().getConfig("user", "a", token).first();
        assertEquals(2, config.size());
        assertEquals("value1", config.get("key1"));
        assertEquals("value2", config.get("key2"));

        map = new HashMap<>();
        map.put("key2", "value3");
        catalogManager.getUserManager().setConfig("user", "a", map, token);
        config = (Map<String, Object>) catalogManager.getUserManager().getConfig("user", "a", token).first();
        assertEquals(1, config.size());
        assertEquals("value3", config.get("key2"));

        catalogManager.getUserManager().deleteConfig("user", "a", token);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getUserManager().getConfig("user", "a", token);
    }

    private String getAdminToken() throws CatalogException, IOException {
        return catalogManager.getUserManager().loginAsAdmin("admin").getToken();
    }

    @Test
    public void getGroupsTest() throws CatalogException {
        Group group = new Group("groupId", Arrays.asList("user2", "user3")).setSyncedFrom(new Group.Sync("ldap", "bio"));
        catalogManager.getStudyManager().createGroup(studyFqn, group, token);

        OpenCGAResult<CustomGroup> customGroups = catalogManager.getStudyManager().getCustomGroups(studyFqn, null, token);
        assertEquals(3, customGroups.getNumResults());

        for (CustomGroup customGroup : customGroups.getResults()) {
            if (!customGroup.getUsers().isEmpty()) {
                assertTrue(StringUtils.isNotEmpty(customGroup.getUsers().get(0).getName()));
            }
        }

        customGroups = catalogManager.getStudyManager().getCustomGroups(studyFqn, group.getId(), token);
        assertEquals(1, customGroups.getNumResults());
        assertEquals(group.getId(), customGroups.first().getId());
        assertEquals(2, customGroups.first().getUsers().size());
        assertTrue(StringUtils.isNotEmpty(customGroups.first().getUsers().get(0).getName()));
        assertNull(customGroups.first().getUsers().get(0).getProjects());
        assertNull(customGroups.first().getUsers().get(0).getSharedProjects());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("Only owners");
        catalogManager.getStudyManager().getCustomGroups(studyFqn, group.getId(), sessionIdUser2);
    }

    @Ignore
    @Test
    public void importLdapUsers() throws CatalogException, NamingException, IOException {
        // Action only for admins
        catalogManager.getUserManager().importRemoteEntities("ldap", Arrays.asList("pfurio", "imedina"), false, null, null,
                getAdminToken());
        // TODO: Validate the users have been imported
    }

    // To make this test work we will need to add a correct user and password to be able to login
    @Ignore
    @Test
    public void loginNotRegisteredUsers() throws CatalogException {
        // Action only for admins
        Group group = new Group("ldap", Collections.emptyList()).setSyncedFrom(new Group.Sync("ldap", "bio"));
        catalogManager.getStudyManager().createGroup(studyFqn, group, token);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), "@ldap", new StudyAclParams("", "view_only"),
                ParamUtils.AclAction.SET, token);
        String token = catalogManager.getUserManager().login("user", "password").getToken();

        assertEquals(9, catalogManager.getSampleManager().count(studyFqn, new Query(), token).getNumTotalResults());

        // We remove the permissions for group ldap
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), "@ldap", new StudyAclParams("", ""),
                ParamUtils.AclAction.RESET, this.token);

        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, new Query(), token).getNumTotalResults());
    }

    @Ignore
    @Test
    public void syncUsers() throws CatalogException {
        // Action only for admins
        String token = catalogManager.getUserManager().loginAsAdmin("admin").getToken();

        catalogManager.getUserManager().importRemoteGroupOfUsers("ldap", "bio", "bio", studyFqn, true, token);
        DataResult<Group> bio = catalogManager.getStudyManager().getGroup(studyFqn, "bio", this.token);

        assertEquals(1, bio.getNumResults());
        assertEquals(0, bio.first().getUserIds().size());

        catalogManager.getUserManager().syncAllUsersOfExternalGroup(studyFqn, "ldap", token);
        bio = catalogManager.getStudyManager().getGroup(studyFqn, "bio", this.token);

        assertEquals(1, bio.getNumResults());
        assertTrue(!bio.first().getUserIds().isEmpty());
    }

    @Ignore
    @Test
    public void importLdapGroups() throws CatalogException, IOException {
        // Action only for admins
        String remoteGroup = "bio";
        String internalGroup = "test";
        String study = "user@1000G:phase1";
        catalogManager.getUserManager().importRemoteGroupOfUsers("ldap", remoteGroup, internalGroup, study, true, getAdminToken());

        DataResult<Group> test = catalogManager.getStudyManager().getGroup("user@1000G:phase1", "test", token);
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
        catalogManager.getUserManager().importRemoteGroupOfUsers("ldap", remoteGroup, internalGroup, study, true, getAdminToken());
    }

    @Test
    public void createEmptyGroup() throws CatalogException {
        catalogManager.getUserManager().create("test", "test", "test@mail.com", TestParamConstants.PASSWORD, null, 100L, Account.AccountType.GUEST, null);
        catalogManager.getStudyManager().createGroup("user@1000G:phase1", "group_cancer_some_thing_else", null, token);
        catalogManager.getStudyManager().updateGroup("user@1000G:phase1", "group_cancer_some_thing_else", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("test")), token);
    }

    @Test
    public void testAssignPermissions() throws CatalogException {
        catalogManager.getUserManager().create("test", "test", "test@mail.com", TestParamConstants.PASSWORD, null, 100L, Account.AccountType.GUEST, null);

        catalogManager.getStudyManager().createGroup("user@1000G:phase1", "group_cancer_some_thing_else",
                Collections.singletonList("test"), token);
        DataResult<Map<String, List<String>>> permissions = catalogManager.getStudyManager().updateAcl(
                Collections.singletonList("user@1000G:phase1"), "@group_cancer_some_thing_else",
                new StudyAclParams("", "view_only"), ParamUtils.AclAction.SET, token);
        assertTrue(permissions.first().containsKey("@group_cancer_some_thing_else"));

        String token = catalogManager.getUserManager().login("test", TestParamConstants.PASSWORD).getToken();
        DataResult<Study> studyDataResult = catalogManager.getStudyManager().get("user@1000G:phase1", QueryOptions.empty(), token);
        assertEquals(1, studyDataResult.getNumResults());
        assertTrue(studyDataResult.first().getAttributes().isEmpty());

        studyDataResult = catalogManager.getStudyManager().get("user@1000G:phase1", new QueryOptions(DBAdaptor.INCLUDE_ACLS, true), token);
        assertEquals(1, studyDataResult.getNumResults());
        assertTrue(!studyDataResult.first().getAttributes().isEmpty());
        assertTrue(studyDataResult.first().getAttributes().containsKey("OPENCGA_ACL"));
        List<Map<String, Object>> acls = (List<Map<String, Object>>) studyDataResult.first().getAttributes().get("OPENCGA_ACL");
        assertEquals(1, acls.size());
        assertEquals("@group_cancer_some_thing_else", acls.get(0).get("member"));
        assertTrue(!((List) acls.get(0).get("permissions")).isEmpty());
    }

    /**
     * Project methods ***************************
     */

    @Test
    public void testGetAllProjects() throws Exception {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        DataResult<Project> projects = catalogManager.getProjectManager().search(query, null, token);
        assertEquals(1, projects.getNumResults());

        projects = catalogManager.getProjectManager().search(query, null, sessionIdUser2);
        assertEquals(0, projects.getNumResults());
    }

    @Test
    public void testCreateProject() throws Exception {

        String projectAlias = "projectAlias_ASDFASDF";

        catalogManager.getProjectManager().create(projectAlias, "Project", "", "Homo sapiens", null, "GRCh38", new
                QueryOptions(), token);

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.getProjectManager().create(projectAlias, "Project", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(), token);
    }

    @Test
    public void testModifyProject() throws CatalogException {
        String newProjectName = "ProjectName " + RandomStringUtils.randomAlphanumeric(10);
        String projectId = catalogManager.getUserManager().get("user", new QueryOptions(), token).first().getProjects().get(0)
                .getId();

        ObjectMap options = new ObjectMap();
        options.put("name", newProjectName);
        ObjectMap attributes = new ObjectMap("myBoolean", true);
        attributes.put("value", 6);
        attributes.put("object", new ObjectMap("id", 1234));
        options.put("attributes", attributes);

        catalogManager.getProjectManager().update(projectId, options, null, token);
        DataResult<Project> result = catalogManager.getProjectManager().get(projectId, null, token);
        Project project = result.first();
        System.out.println(result);

        assertNotEquals("20180101120000", project.getCreationDate());
        assertEquals(newProjectName, project.getName());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(project.getAttributes().get(entry.getKey()), entry.getValue());
        }

        options = new ObjectMap();
        options.put(ProjectDBAdaptor.QueryParams.CREATION_DATE.key(), "20180101120000");
        catalogManager.getProjectManager().update(projectId, options, null, token);
        project = catalogManager.getProjectManager().get(projectId, null, token).first();
        assertEquals("20180101120000", project.getCreationDate());

        options = new ObjectMap();
        options.put(ProjectDBAdaptor.QueryParams.ID.key(), "newProjectId");
        catalogManager.getProjectManager().update(projectId, options, null, token);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getProjectManager().update(projectId, options, null, token);
    }

    /**
     * Study methods ***************************
     */

    @Test
    public void testModifyStudy() throws Exception {
        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user");
        String studyId = catalogManager.getStudyManager().search(query, null, token).first().getId();

        String newName = "Phase 1 " + RandomStringUtils.randomAlphanumeric(20);
        String newDescription = RandomStringUtils.randomAlphanumeric(500);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("key", "value");
        StudyUpdateParams updateParams = new StudyUpdateParams()
                .setName(newName)
                .setDescription(newDescription)
                .setAttributes(attributes);
        catalogManager.getStudyManager().update(studyId, updateParams, null, token);

        DataResult<Study> result = catalogManager.getStudyManager().get(studyId, null, token);
        System.out.println(result);
        Study study = result.first();
        assertEquals(study.getName(), newName);
        assertEquals(study.getDescription(), newDescription);
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(study.getAttributes().get(entry.getKey()), entry.getValue());
        }

        assertNotEquals("20180101120000", study.getCreationDate());
        catalogManager.getStudyManager().update(studyId, new StudyUpdateParams().setCreationDate("20180101120000"), null, token);
        study = catalogManager.getStudyManager().get(studyId, null, token).first();
        assertEquals("20180101120000", study.getCreationDate());
    }

    @Test
    public void testGetAllStudies() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        String projectId = catalogManager.getProjectManager().search(query, null, token).first().getId();
        Study study_1 = catalogManager.getStudyManager().create(projectId, new Study().setId("study_1").setCreationDate("20150101120000")
                , null, token).first();
        assertEquals("20150101120000", study_1.getCreationDate());

        catalogManager.getStudyManager().create(projectId, "study_2", null, "study_2", "description", null, null, null, null, null, token);

        catalogManager.getStudyManager().create(projectId, "study_3", null, "study_3", "description", null, null, null, null, null, token);

        String study_4 = catalogManager.getStudyManager().create(projectId, "study_4", null, "study_4", "description", null, null, null,
                null, null, token).first().getId();

        assertEquals(new HashSet<>(Collections.emptyList()), catalogManager.getStudyManager().search(new Query(StudyDBAdaptor.QueryParams
                        .GROUP_USER_IDS.key(), "user2"), null, token).getResults().stream().map(Study::getId)
                .collect(Collectors.toSet()));

//        catalogManager.getStudyManager().createGroup(Long.toString(study_4), "admins", "user3", sessionIdUser);
        catalogManager.getStudyManager().updateGroup(study_4, "admins", ParamUtils.BasicUpdateAction.SET,
                new GroupUpdateParams(Collections.singletonList("user3")), token);
        assertEquals(new HashSet<>(Arrays.asList("study_4")), catalogManager.getStudyManager().search(new Query(StudyDBAdaptor.QueryParams
                        .GROUP_USER_IDS.key(), "user3"), null, token).getResults().stream().map(Study::getId)
                .collect(Collectors.toSet()));

        assertEquals(new HashSet<>(Arrays.asList("phase1", "phase3", "study_1", "study_2", "study_3", "study_4")),
                catalogManager.getStudyManager().search(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, token)
                        .getResults().stream().map(Study::getId).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("phase1", "phase3", "study_1", "study_2", "study_3", "study_4")),
                catalogManager.getStudyManager().search(new Query(), null, token).getResults().stream().map(Study::getId)
                        .collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("study_1", "study_2", "study_3", "study_4")), catalogManager.getStudyManager().search(new
                        Query(StudyDBAdaptor.QueryParams.ID.key(), "~^study"), null, token).getResults().stream()
                .map(Study::getId).collect(Collectors.toSet()));
        assertEquals(Collections.singleton("s1"), catalogManager.getStudyManager().search(new Query(), null, sessionIdUser2).getResults()
                .stream()
                .map(Study::getId).collect(Collectors.toSet()));
    }

    @Test
    public void testGetId() throws CatalogException {
        // Create another study with alias phase3
        catalogManager.getStudyManager().create(project2, "phase3", null, "Phase 3", "d", null, null, null, null, null, sessionIdUser2);

        String userId = catalogManager.getUserManager().getUserId(token);
        List<Long> uids = catalogManager.getStudyManager().resolveIds(Arrays.asList("*"), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Collections.emptyList(), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Collections.emptyList(), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList("1000G:*"), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(userId + "@1000G:*"), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(userId + "@1000G:phase1", userId + "@1000G:phase3"), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(userId + "@1000G:phase1", "phase3"), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(userId + "@1000G:phase3", studyFqn), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        try {
            catalogManager.getStudyManager().resolveId(null, userId);
            fail("This method should fail because it should find several studies");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("More than one study"));
        }

        Study study = catalogManager.getStudyManager().resolveId("phase3", userId);
        assertEquals(studyUid2, study.getUid());
    }

    @Test
    public void testGetOnlyStudyUserAnonymousCanSee() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        try {
            studyManager.resolveIds(Collections.emptyList(), "*");
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        // Create another study with alias phase3
        DataResult<Study> study = catalogManager.getStudyManager().create(String.valueOf(project2), "phase3", null, "Phase 3", "d", null,
                null, null, null, null, sessionIdUser2);
        try {
            studyManager.resolveIds(Collections.emptyList(), "*");
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        catalogManager.getStudyManager().updateGroup("phase3", "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), sessionIdUser2);

        List<Study> studies = studyManager.resolveIds(Collections.emptyList(), "*");
        assertEquals(1, studies.size());
        assertEquals(study.first().getUid(), studies.get(0).getUid());
    }

    @Test
    public void testGetSelectedStudyUserAnonymousCanSee() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        try {
            studyManager.resolveIds(Collections.singletonList("phase3"), "*");
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        // Create another study with alias phase3
        DataResult<Study> study = catalogManager.getStudyManager().create(project2, "phase3", null, "Phase 3", "d", null, null, null,
                null, null, sessionIdUser2);
        catalogManager.getStudyManager().updateGroup("phase3", "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), sessionIdUser2);

        List<Study> studies = studyManager.resolveIds(Collections.singletonList("phase3"), "*");
        assertEquals(1, studies.size());
        assertEquals(study.first().getUid(), studies.get(0).getUid());
    }

    @Test
    public void testCreatePermissionRules() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList("user2", "user3"),
                Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(), SampleAclEntry.SamplePermissions.WRITE.name()));
        DataResult<PermissionRule> permissionRulesDataResult = catalogManager.getStudyManager().createPermissionRule(
                studyFqn, Enums.Entity.SAMPLES, rules, token);
        assertEquals(1, permissionRulesDataResult.getNumResults());
        assertEquals("rules1", permissionRulesDataResult.first().getId());
        assertEquals(1, permissionRulesDataResult.first().getQuery().size());
        assertEquals(2, permissionRulesDataResult.first().getMembers().size());
        assertEquals(2, permissionRulesDataResult.first().getPermissions().size());

        // Add new permission rules object
        rules.setId("rules2");
        permissionRulesDataResult = catalogManager.getStudyManager().createPermissionRule(studyFqn, Enums.Entity.SAMPLES, rules,
                token);
        assertEquals(1, permissionRulesDataResult.getNumResults());
        assertEquals(rules, permissionRulesDataResult.first());
    }

    @Test
    public void testUpdatePermissionRulesIncorrectPermission() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList("user2", "user3"),
                Arrays.asList("VV", "UPDATE"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Detected unsupported");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Enums.Entity.SAMPLES, rules, token);
    }

    @Test
    public void testUpdatePermissionRulesNonExistingUser() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList("user2", "user20"),
                Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(), SampleAclEntry.SamplePermissions.WRITE.name()));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Enums.Entity.SAMPLES, rules, token);
    }

    @Test
    public void testUpdatePermissionRulesNonExistingGroup() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList("user2", "@group"),
                Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(), SampleAclEntry.SamplePermissions.WRITE.name()));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Enums.Entity.SAMPLES, rules, token);
    }

    @Test
    public void removeAllPermissionsToMember() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        // Assign permissions to study
        DataResult<Group> groupDataResult = studyManager.updateGroup(studyFqn, "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Arrays.asList("user2", "user3")), token);
        assertEquals(3, groupDataResult.first().getUserIds().size());
        assertEquals("@members", groupDataResult.first().getId());

        // Obtain all samples from study
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, new Query(), QueryOptions
                .empty(), token);
        assertTrue(sampleDataResult.getNumResults() > 0);

        // Assign permissions to all the samples
        SampleAclParams sampleAclParams = new SampleAclParams(null, null, null, null,
                SampleAclEntry.SamplePermissions.VIEW.name() + "," + SampleAclEntry.SamplePermissions.WRITE.name());
        List<String> sampleIds = sampleDataResult.getResults().stream()
                .map(Sample::getId)
                .collect(Collectors.toList());
        DataResult<Map<String, List<String>>> sampleAclResult = catalogManager.getSampleManager().updateAcl(studyFqn,
                sampleIds, "user2,user3", sampleAclParams, ParamUtils.AclAction.SET, token);
        assertEquals(sampleIds.size(), sampleAclResult.getNumResults());
        for (Map<String, List<String>> result : sampleAclResult.getResults()) {
            assertEquals(2, result.size());
            assertTrue(result.keySet().containsAll(Arrays.asList("user2", "user3")));
            assertTrue(result.get("user2").containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(),
                    SampleAclEntry.SamplePermissions.WRITE.name())));
            assertTrue(result.get("user3").containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(),
                    SampleAclEntry.SamplePermissions.WRITE.name())));
        }

        // Remove all the permissions to both users in the study. That should also remove the permissions they had in all the samples.
        groupDataResult = studyManager.updateGroup(studyFqn, "@members", ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Arrays.asList("user2", "user3")), token);
        assertEquals(1, groupDataResult.first().getUserIds().size());

        // Get sample permissions for those members
        for (Sample sample : sampleDataResult.getResults()) {
            long sampleUid = sample.getUid();
            DataResult<Map<String, List<String>>> sampleAcl =
                    catalogManager.getAuthorizationManager().getSampleAcl(studyUid, sampleUid, "user", "user2");
            assertEquals(0, sampleAcl.getNumResults());
            sampleAcl = catalogManager.getAuthorizationManager().getSampleAcl(studyUid, sampleUid, "user", "user3");
            assertEquals(0, sampleAcl.getNumResults());
        }
    }

    @Test
    public void removeUsersFromStudies() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        // Assign permissions to study
        DataResult<Group> groupDataResult = studyManager.updateGroup(studyFqn, "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Arrays.asList("user2", "user3")), token);
        assertEquals(3, groupDataResult.first().getUserIds().size());
        assertEquals("@members", groupDataResult.first().getId());

        // Obtain all samples from study
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, new Query(), QueryOptions
                .empty(), token);
        assertTrue(sampleDataResult.getNumResults() > 0);

        // Assign permissions to all the samples
        SampleAclParams sampleAclParams = new SampleAclParams(null, null, null, null,
                SampleAclEntry.SamplePermissions.VIEW.name() + "," + SampleAclEntry.SamplePermissions.WRITE.name());
        List<String> sampleIds = sampleDataResult.getResults().stream().map(Sample::getId).collect(Collectors.toList());

        DataResult<Map<String, List<String>>> sampleAclResult = catalogManager.getSampleManager().updateAcl(studyFqn,
                sampleIds, "user2,user3", sampleAclParams, ParamUtils.AclAction.SET, token);
        assertEquals(sampleIds.size(), sampleAclResult.getNumResults());
        for (Map<String, List<String>> result : sampleAclResult.getResults()) {
            assertEquals(2, result.size());
            assertTrue(result.keySet().containsAll(Arrays.asList("user2", "user3")));
            assertTrue(result.get("user2").containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(),
                    SampleAclEntry.SamplePermissions.WRITE.name())));
            assertTrue(result.get("user3").containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(),
                    SampleAclEntry.SamplePermissions.WRITE.name())));
        }

        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Arrays.asList("user2", "user3")), token);

        String userId1 = catalogManager.getUserManager().getUserId(token);
        Study study3 = catalogManager.getStudyManager().resolveId(studyFqn, userId1);

        DataResult<Map<String, List<String>>> studyAcl = catalogManager.getAuthorizationManager()
                .getStudyAcl(userId1, study3.getUid(), "user2");
        assertEquals(0, studyAcl.getNumResults());
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study1 = catalogManager.getStudyManager().resolveId(studyFqn, userId);
        studyAcl = catalogManager.getAuthorizationManager().getStudyAcl(userId, study1.getUid(), "user3");
        assertEquals(0, studyAcl.getNumResults());

        groupDataResult = catalogManager.getStudyManager().getGroup(studyFqn, null, token);
        for (Group group : groupDataResult.getResults()) {
            assertTrue(!group.getUserIds().contains("user2"));
            assertTrue(!group.getUserIds().contains("user3"));
        }

        for (Sample sample : sampleDataResult.getResults()) {
            DataResult<Map<String, List<String>>> sampleAcl =
                    catalogManager.getAuthorizationManager().getSampleAcl(studyUid, sample.getUid(), "user", "user2");
            assertEquals(0, sampleAcl.getNumResults());
            sampleAcl = catalogManager.getAuthorizationManager().getSampleAcl(studyUid, sample.getUid(), "user", "user3");
            assertEquals(0, sampleAcl.getNumResults());
        }
    }

    /**
     * Job methods ***************************
     */

//    @Test
//    public void testCreateJob() throws CatalogException {
//        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user");
//        String studyId = catalogManager.getStudyManager().search(query, null, token).first().getId();
//
//        catalogManager.getJobManager().submit(studyId, "command-subcommand", null, Collections.emptyMap(), token);
//        catalogManager.getJobManager().submit(studyId, "command-subcommand2", null, Collections.emptyMap(), token);
//
//        catalogManager.getJobManager().create(studyId,
//                new Job().setId("job1").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE))),
//                QueryOptions.empty(), token);
//        catalogManager.getJobManager().create(studyId,
//                new Job().setId("job2").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR))),
//                QueryOptions.empty(), token);
//        catalogManager.getJobManager().create(studyId,
//                new Job().setId("job3").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.UNREGISTERED))),
//                QueryOptions.empty(), token);
//
//        query = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.PENDING);
//        DataResult<Job> unfinishedJobs = catalogManager.getJobManager().search(String.valueOf(studyId), query, null, token);
//        assertEquals(2, unfinishedJobs.getNumResults());
//
//        DataResult<Job> allJobs = catalogManager.getJobManager().search(String.valueOf(studyId), (Query) null, null, token);
//        assertEquals(5, allJobs.getNumResults());
//
//        thrown.expectMessage("status different");
//        thrown.expect(CatalogException.class);
//        catalogManager.getJobManager().create(studyId,
//                new Job().setId("job5").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.PENDING))),
//                QueryOptions.empty(), token);
//    }
    @Test
    public void submitJobWithDependenciesFromDifferentStudies() throws CatalogException {
        Execution first = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", null, Collections.emptyMap(), token)
                .first();
        Execution second = catalogManager.getExecutionManager().submit(studyFqn2, "variant-index", null, Collections.emptyMap(), null,
                "", Collections.singletonList(first.getUuid()), null, token).first();
        assertEquals(first.getId(), second.getDependsOn().get(0).getId());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getExecutionManager().submit(studyFqn2, "variant-index", null, Collections.emptyMap(), null, "",
                Collections.singletonList(first.getId()), null, token);
    }

    @Test
    public void testGetAllExecutions() throws CatalogException {
        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user");
        String studyId = catalogManager.getStudyManager().search(query, null, token).first().getId();

        catalogManager.getExecutionManager().submit(studyId, "variant-index", null, Collections.emptyMap(), null,
                "", Collections.emptyList(), null, token);

        QueryOptions options = new QueryOptions(QueryOptions.COUNT, true);
        DataResult<Execution> allExecutions = catalogManager.getExecutionManager().search(studyId, null, options, token);

        assertEquals(1, allExecutions.getNumMatches());
        assertEquals(1, allExecutions.getNumResults());
    }

    @Test
    public void testJobsTop() throws CatalogException {
        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user");
        List<Study> studies = catalogManager.getStudyManager().search(query, null, token).getResults();
        System.out.println(studies.size());

        for (int i = 99; i > 0; i--) {
            String studyId = studies.get(i % studies.size()).getId();
            String id = catalogManager.getJobManager().create(studyId, new Job().setId("myJob-" + i), INCLUDE_RESULT, token).first().getId();
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
            catalogManager.getJobManager().privateUpdate(studyId, id, new JobInternal(new Enums.ExecutionStatus(status)), adminToken);
        }

        int limit = 20;
        DataResult<JobTop> top = catalogManager.getJobManager().top(new Query(), limit, token);

        assertEquals(1, top.getNumMatches());
        assertEquals(limit, top.first().getJobs().size());
        assertEquals(studies.size(), top.first().getJobs().stream().map(job -> job.getStudy().getId()).collect(Collectors.toSet()).size());
        assertEquals(new JobTopStats(33, 0, 0, 33, 0, 33), top.first().getStats());
    }

    @Test
    public void submitExecutionWithoutPermissions() throws CatalogException {
        // Check there are no ABORTED jobs
        Query query = new Query(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.ABORTED);
        assertEquals(0, catalogManager.getExecutionManager().count(studyFqn, query, token).getNumMatches());

        // Grant view permissions, but no EXECUTION permission
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "user3",
                new StudyAclParams("", AuthorizationManager.ROLE_VIEW_ONLY), ParamUtils.AclAction.SET, token);

        try {
            catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(), sessionIdUser3);
            fail("Sumbmission should have failed with a message saying the user does not have EXECUTION permissions");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Permission denied"));
        }

        // The previous execution should have created an ABORTED job
        OpenCGAResult<Execution> search = catalogManager.getExecutionManager().search(studyFqn, query, null, token);
        assertEquals(1, search.getNumResults());
        assertEquals("variant-index", search.first().getInternal().getToolId());
    }

    @Test
    public void submitExecutionWithPermissions() throws CatalogException {
        // Check there are no ABORTED jobs
        Query query = new Query(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.ABORTED);
        assertEquals(0, catalogManager.getExecutionManager().count(studyFqn, query, token).getNumMatches());

        // Grant view permissions, but no EXECUTION permission
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "user3",
                new StudyAclParams(StudyAclEntry.StudyPermissions.EXECUTE_JOBS.name(), AuthorizationManager.ROLE_VIEW_ONLY),
                ParamUtils.AclAction.SET, token);

        OpenCGAResult<Execution> search = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM,
                new ObjectMap(), sessionIdUser3);
        assertEquals(1, search.getNumResults());
        assertEquals(Enums.ExecutionStatus.PENDING, search.first().getInternal().getStatus().getId());
    }

    @Test
    public void deleteExecutionTest() throws CatalogException {
        // Grant view permissions, but no EXECUTION permission
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "user3",
                new StudyAclParams(StudyAclEntry.StudyPermissions.EXECUTE_JOBS.name(), AuthorizationManager.ROLE_VIEW_ONLY),
                ParamUtils.AclAction.SET, token);

        OpenCGAResult<Execution> search = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM,
                new ObjectMap(), sessionIdUser3);
        assertEquals(1, search.getNumResults());
        assertEquals(Enums.ExecutionStatus.PENDING, search.first().getInternal().getStatus().getId());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("stop the job");
        catalogManager.getExecutionManager().delete(studyFqn, Collections.singletonList(search.first().getId()), QueryOptions.empty(), token);
    }

    @Test
    public void visitExecution() throws CatalogException {
        Execution execution = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(), token)
                .first();

        Query query = new Query(ExecutionDBAdaptor.QueryParams.VISITED.key(), false);
        assertEquals(1, catalogManager.getExecutionManager().count(studyFqn, query, token).getNumMatches());

        // Now we visit the job
        ExecutionUpdateParams updateParams = new ExecutionUpdateParams().setVisited(true);
        catalogManager.getExecutionManager().update(studyFqn, execution.getId(), updateParams, token);
        assertEquals(0, catalogManager.getExecutionManager().count(studyFqn, query, token).getNumMatches());

        query.put(ExecutionDBAdaptor.QueryParams.VISITED.key(), true);
        assertEquals(1, catalogManager.getExecutionManager().count(studyFqn, query, token).getNumMatches());

        // Now we update setting job to visited false again
        updateParams = new ExecutionUpdateParams().setVisited(false);
        catalogManager.getExecutionManager().update(studyFqn, execution.getId(), updateParams, token);
        assertEquals(0, catalogManager.getExecutionManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(ExecutionDBAdaptor.QueryParams.VISITED.key(), false);
        assertEquals(1, catalogManager.getExecutionManager().count(studyFqn, query, token).getNumMatches());
    }

    /**
     * VariableSet methods
     * ***************************
     */

    @Test
    public void testCreateVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().get("user@1000G:phase1", null, token).first();
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
                false, "", null, variables, Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token);

        assertEquals(1, queryResult.getResults().size());

        study = catalogManager.getStudyManager().get(study.getId(), null, token).first();
        assertEquals(variableSetNum + 1, study.getVariableSets().size());
    }

    @Test
    public void testCreateRepeatedVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().get("user@1000G:phase1", null, token).first();

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
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token);
    }

    @Test
    public void testDeleteVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().resolveId("1000G:phase1", "user");

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), null, 0, "", "",
                        null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), null, 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs1", "vs1", true, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        DataResult<VariableSet> result = catalogManager.getStudyManager().deleteVariableSet(studyFqn, vs1.getId(), false, token);
        assertEquals(0, result.getNumResults());

        thrown.expect(CatalogException.class);    //VariableSet does not exist
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().getVariableSet(studyFqn, vs1.getId(), null, token);
    }

    @Test
    public void testGetAllVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().resolveId("1000G:phase1", "user");

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), null, 0, "", "",
                        null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), null, 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs1", "vs1", true, false, "Cancer", null,
                variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();
        VariableSet vs2 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs2", "vs2", true, false, "Virgo", null,
                variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();
        VariableSet vs3 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs3", "vs3", true, false, "Piscis", null,
                variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();
        VariableSet vs4 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs4", "vs4", true, false, "Aries", null,
                variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        long numResults;
        numResults = catalogManager.getStudyManager().searchVariableSets(study.getFqn(),
                new Query(StudyDBAdaptor.VariableSetParams.ID.key(), "vs1"), QueryOptions.empty(), token).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getStudyManager().searchVariableSets(study.getFqn(),
                        new Query(StudyDBAdaptor.VariableSetParams.ID.key(), "vs1,vs2"), QueryOptions.empty(), token)
                .getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getStudyManager().searchVariableSets(study.getFqn(),
                new Query(StudyDBAdaptor.VariableSetParams.ID.key(), "VS1"), QueryOptions.empty(), token).getNumResults();
        assertEquals(0, numResults);

        numResults = catalogManager.getStudyManager().searchVariableSets(study.getFqn(),
                new Query(StudyDBAdaptor.VariableSetParams.UID.key(), vs1.getId()), QueryOptions.empty(), token).getNumResults();
        assertEquals(1, numResults);
//        numResults = catalogManager.getStudyManager().searchVariableSets(studyFqn,
//                new Query(StudyDBAdaptor.VariableSetParams.ID.key(), vs1.getId() + "," + vs3.getId()), QueryOptions.empty(),
//                sessionIdUser).getNumResults();

        numResults = catalogManager.getStudyManager().searchVariableSets(study.getFqn(),
                new Query(StudyDBAdaptor.VariableSetParams.UID.key(), vs3.getId()), QueryOptions.empty(), token).getNumResults();
        assertEquals(1, numResults);
    }

    @Test
    public void testDeleteVariableSetInUse() throws CatalogException {
        String sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"),
                INCLUDE_RESULT, token).first().getId();
        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "", null,
                        Collections.emptyMap()),
                new Variable("AGE", "", "", Variable.VariableType.DOUBLE, null, false, false, Collections.singletonList("0:99"), null, 1,
                        "", "",
                        null, Collections.emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", true, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        Map<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "LINUS");
        catalogManager.getSampleManager().update(studyFqn, sampleId1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotationId", vs1.getId(), annotations))),
                QueryOptions.empty(), token);

        try {
            catalogManager.getStudyManager().deleteVariableSet(studyFqn, vs1.getId(), false, token);
        } finally {
            VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyFqn, vs1.getId(), null, token).first();
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
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, token).first();
        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort")
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3))
                .setStatus(new Status("custom", "custom", "description", TimeUtils.getTime())), INCLUDE_RESULT, token).first();

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
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, token).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1, sampleId2)), null, token).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort2")
                .setSamples(Arrays.asList(sampleId2, sampleId3)), null, token).first();

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, options, token);
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
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, token).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1)), null, token).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort2")
                .setSamples(Arrays.asList(sampleId2, sampleId3)), null, token).first();

        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(sampleId3.getId()))),
                QueryOptions.empty(), token);

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions optionsSample = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());
        QueryOptions optionsCohort = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, optionsSample, token);

        Cohort cohortResult = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", optionsCohort, token).first();

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
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, token).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1)), null, token).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort2")
                .setSamples(Arrays.asList(sampleId2, sampleId3)), null, token).first();

        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Arrays.asList(new SampleReferenceParam().setId(sampleId3.getId()))),
                QueryOptions.empty(), token);

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(Constants.ACTIONS, actionMap);
        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Arrays.asList(new SampleReferenceParam().setId(sampleId1.getId()))),
                queryOptions, token);

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions optionsSample = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());
        QueryOptions optionsCohort = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, optionsSample, token);

        Cohort cohortResult = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", optionsCohort, token).first();

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
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, token).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1)), null, token).first();

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(Constants.ACTIONS, actionMap);
        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Arrays.asList(
                        new SampleReferenceParam().setId(sampleId2.getId()),
                        new SampleReferenceParam().setId(sampleId3.getId()))),
                queryOptions, token);

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions optionsSample = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());
        QueryOptions optionsCohort = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, optionsSample, token);

        Cohort cohortResult = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", optionsCohort, token).first();

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
//        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().create("user@1000G:phase1", "sample1", "", "", null,
//                null, QueryOptions.empty(), sessionIdUser);
//
//        Sample oldSample = new Sample().setId(sampleDataResult.first().getId());
//        Sample newSample = new Sample().setName("sample2");
//        ServerUtils.IndividualParameters individualParameters = new ServerUtils.IndividualParameters()
//                .setName("individual").setSamples(Arrays.asList(oldSample, newSample));
//
//        long studyUid = catalogManager.getStudyManager().getId("user", "1000G:phase1");
//        // We create the individual together with the samples
//        DataResult<Individual> individualDataResult = catalogManager.getIndividualManager().create("user@1000G:phase1",
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
        String studyId = "1000G:phase1";

        Sample sampleId1 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, token).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_4"), INCLUDE_RESULT, token).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_5"), INCLUDE_RESULT, token).first();
        Cohort myCohort1 = catalogManager.getCohortManager().create(studyId,
                new Cohort().setId("MyCohort1").setType(Enums.CohortType.FAMILY).setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)),
                INCLUDE_RESULT, token).first();
        Cohort myCohort2 = catalogManager.getCohortManager().create(studyId,
                new Cohort().setId("MyCohort2").setType(Enums.CohortType.FAMILY)
                        .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3, sampleId4)), INCLUDE_RESULT, token).first();
        Cohort myCohort3 = catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort3")
                .setType(Enums.CohortType.CASE_CONTROL).setSamples(Arrays.asList(sampleId3, sampleId4)), INCLUDE_RESULT, token).first();
        catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort4").setType(Enums.CohortType.TRIO)
                .setSamples(Arrays.asList(sampleId5, sampleId3)), null, token).first();

        long numResults;
        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                sampleId1.getId()), new QueryOptions(), token).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                sampleId1.getId()
                        + "," + sampleId5.getId()), new QueryOptions(), token).getNumResults();
        assertEquals(3, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(), "MyCohort2"), new
                QueryOptions(), token).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(), "~MyCohort."), new
                QueryOptions(), token).getNumResults();
        assertEquals(4, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.TYPE.key(),
                Enums.CohortType.FAMILY), new QueryOptions(), token).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.TYPE.key(), "CASE_CONTROL"),
                new QueryOptions(), token).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.UID.key(), myCohort1.getUid() +
                "," + myCohort2.getUid() + "," + myCohort3.getUid()), new QueryOptions(), token).getNumResults();
        assertEquals(3, numResults);
    }

    @Test
    public void testCreateCohortFail() throws CatalogException {
        thrown.expect(CatalogException.class);
        List<Sample> sampleList = Arrays.asList(new Sample().setId("a"), new Sample().setId("b"), new Sample().setId("c"));
        catalogManager.getCohortManager().create(studyFqn,
                new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY).setSamples(sampleList),
                null, token);
    }

    @Test
    public void testCreateCohortAlreadyExisting() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, token).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Collections.singletonList(sampleId1)), null, token).first();

        thrown.expect(CatalogDBException.class);
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Collections.singletonList(sampleId1)), null, token).first();
    }

    @Test
    public void testUpdateCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, token).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_4"), INCLUDE_RESULT, token).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_5"), INCLUDE_RESULT, token).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3, sampleId1)), INCLUDE_RESULT, token).first();

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
                options, token);
        assertEquals(1, result.getNumUpdated());

        Cohort myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), token).first();
        assertEquals("myModifiedCohort", myModifiedCohort.getId());
        assertEquals(4, myModifiedCohort.getSamples().size());
        assertEquals(4, myModifiedCohort.getNumSamples());
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId4.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId5.getUid()));

        QueryOptions options1 = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.NUM_SAMPLES.key());
        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", options1, token).first();
        assertEquals(4, myModifiedCohort.getNumSamples());
        assertNull(myModifiedCohort.getSamples());

        options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.BasicUpdateAction.SET.name()));
        result = catalogManager.getCohortManager().update(studyFqn, myModifiedCohort.getId(),
                new CohortUpdateParams()
                        .setSamples(Collections.emptyList()),
                options, token);
        assertEquals(1, result.getNumUpdated());

        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), token).first();
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
                options, token);
        assertEquals(1, result.getNumUpdated());
        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), token).first();
        assertEquals(2, myModifiedCohort.getSamples().size());
        assertEquals(2, myModifiedCohort.getNumSamples());

        options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.BasicUpdateAction.REMOVE.name()));
        result = catalogManager.getCohortManager().update(studyFqn, myModifiedCohort.getId(),
                new CohortUpdateParams()
                        .setSamples(Arrays.asList(
                                new SampleReferenceParam().setId(sampleId3.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()))),
                options, token);
        assertEquals(1, result.getNumUpdated());
        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), token).first();
        assertEquals(1, myModifiedCohort.getSamples().size());
        assertEquals(1, myModifiedCohort.getNumSamples());
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
    }

    /*                    */
    /* Test util methods  */
    /*                    */

    @Test
    public void testDeleteCohort() throws CatalogException {
        String studyId = "user@1000G:phase1";

        Sample sampleId1 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, token).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), INCLUDE_RESULT, token).first();

        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));

        DataResult deleteResult = catalogManager.getCohortManager().delete(studyId,
                new Query(CohortDBAdaptor.QueryParams.UID.key(), myCohort.getUid()), null, token);
        assertEquals(1, deleteResult.getNumDeleted());

        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.UID.key(), myCohort.getUid())
                .append(CohortDBAdaptor.QueryParams.DELETED.key(), true);
        Cohort cohort = catalogManager.getCohortManager().search(studyId, query, null, token).first();
        assertEquals(InternalStatus.DELETED, cohort.getInternal().getStatus().getId());
    }

    @Test
    public void getSamplesFromCohort() throws CatalogException, IOException {
        String studyId = "user@1000G:phase1";

        Sample sampleId1 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT,
                token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT,
                token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT,
                token).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), INCLUDE_RESULT, token).first();

        DataResult<Sample> myCohort1 = catalogManager.getCohortManager().getSamples(studyId, "MyCohort", token);
        assertEquals(3, myCohort1.getNumResults());

        thrown.expect(CatalogParameterException.class);
        catalogManager.getCohortManager().getSamples(studyId, "MyCohort,AnotherCohort", token);

        thrown.expect(CatalogParameterException.class);
        catalogManager.getCohortManager().getSamples(studyId, "MyCohort,MyCohort", token);
    }

    @Test
    public void testDeleteVariableSetWithAnnotations() throws CatalogException {
        VariableSet variableSet = createVariableSetAndAnnotationSets();
        thrown.expect(CatalogException.class);
        thrown.expectMessage("in use");
        catalogManager.getStudyManager().deleteVariableSet(studyFqn, variableSet.getId(), false, token);
    }

    private VariableSet createVariableSetAndAnnotationSets() throws CatalogException {
        VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyFqn, "vs", null, token).first();

        String individualId1 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_1")
                        .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                INCLUDE_RESULT, token).first().getId();
        String individualId2 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_2")
                        .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                INCLUDE_RESULT, token).first().getId();
        String individualId3 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_3")
                        .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                INCLUDE_RESULT, token).first().getId();

        catalogManager.getIndividualManager().update(studyFqn, individualId1, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", variableSet.getId(),
                                new ObjectMap("NAME", "INDIVIDUAL_1").append("AGE", 5).append("PHEN", "CASE").append("ALIVE", true)))),
                QueryOptions.empty(), token);

        catalogManager.getIndividualManager().update(studyFqn, individualId2, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", variableSet.getId(),
                                new ObjectMap("NAME", "INDIVIDUAL_2").append("AGE", 15).append("PHEN", "CONTROL").append("ALIVE", true)))),
                QueryOptions.empty(), token);

        catalogManager.getIndividualManager().update(studyFqn, individualId3, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", variableSet.getId(),
                                new ObjectMap("NAME", "INDIVIDUAL_3").append("AGE", 25).append("PHEN", "CASE").append("ALIVE", true)))),
                QueryOptions.empty(), token);

        QueryOptions options = new QueryOptions()
                .append(QueryOptions.LIMIT, 0)
                .append(QueryOptions.COUNT, true);
        OpenCGAResult<?> result = catalogManager.getIndividualManager().search(studyFqn,
                new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId()), options,
                token);
        assertEquals(3, result.getNumMatches());

        result = catalogManager.getSampleManager().search(studyFqn,
                new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId()), options,
                token);
        assertEquals(8, result.getNumMatches());
        return variableSet;
    }

    @Test
    public void testForceDeleteVariableSetWithAnnotations() throws CatalogException {
        VariableSet variableSet = createVariableSetAndAnnotationSets();
        catalogManager.getStudyManager().deleteVariableSet(studyFqn, variableSet.getId(), true, token);

        QueryOptions options = new QueryOptions()
                .append(QueryOptions.LIMIT, 0)
                .append(QueryOptions.COUNT, true);
        try {
            catalogManager.getIndividualManager().search(studyFqn,
                    new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId()), options,
                    token);
            fail("It should fail saying Variable set does not exist");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        try {
            catalogManager.getSampleManager().search(studyFqn,
                    new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId()), options,
                    token);
            fail("It should fail saying Variable set does not exist");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        try {
            catalogManager.getStudyManager().getVariableSet(studyFqn, variableSet.getId(), QueryOptions.empty(), token);
            fail("It should fail saying Variable set does not exist");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("not found"));
        }
    }


    @Test
    public void generateCohortFromSampleQuery() throws CatalogException, IOException {
        String studyId = "user@1000G:phase1";

        catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, token);
        catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, token);
        catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, token);

        Query query = new Query();
        Cohort myCohort = catalogManager.getCohortManager().generate(studyId, query, new Cohort().setId("MyCohort"), INCLUDE_RESULT, token).first();
        assertEquals(12, myCohort.getSamples().size());

        query = new Query(SampleDBAdaptor.QueryParams.ID.key(), "~^SAM");
        myCohort = catalogManager.getCohortManager().generate(studyId, query, new Cohort()
                .setId("MyCohort2")
                .setStatus(new Status("custom", "custom", "description", TimeUtils.getTime())), INCLUDE_RESULT, token).first();
        assertEquals(3, myCohort.getSamples().size());
        assertNotNull(myCohort.getStatus());
        assertEquals("custom", myCohort.getStatus().getName());
        assertEquals("description", myCohort.getStatus().getDescription());
    }

}