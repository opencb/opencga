/*
 * Copyright 2015-2017 OpenCB
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
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobInternal;
import org.opencb.opencga.core.models.job.JobUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.sample.SampleAclParams;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.naming.NamingException;
import java.io.IOException;
import java.nio.file.Paths;
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
        catalogManager.getStudyManager().create(null, "phasexx", null, "Phase 1", "Done", null, null, null, null,
                null, null, null, token);
    }

    @Test
    public void testAdminUserExists() throws Exception {
        String token = catalogManager.getUserManager().loginAsAdmin("admin");
        assertEquals("opencga" ,catalogManager.getUserManager().getUserId(token));
    }

    @Test
    public void testCreateExistingUser() throws Exception {
        thrown.expect(CatalogException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null);
    }

    @Test
    public void testLogin() throws Exception {
        catalogManager.getUserManager().login("user", PASSWORD);

        thrown.expect(CatalogAuthenticationException.class);
        thrown.expectMessage(allOf(containsString("Incorrect"), containsString("password")));
        catalogManager.getUserManager().login("user", "fakePassword");
    }

    @Test
    public void testGetUserInfo() throws CatalogException {
        DataResult<User> user = catalogManager.getUserManager().get("user", new QueryOptions(), token);
        System.out.println("user = " + user);
        try {
            catalogManager.getUserManager().get("user", new QueryOptions(), sessionIdUser2);
            fail();
        } catch (CatalogException e) {
            System.out.println(e);
        }
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

        User userPre = catalogManager.getUserManager().get("user", new QueryOptions(), token).first();
        System.out.println("userPre = " + userPre);
        Thread.sleep(10);

        catalogManager.getUserManager().update("user", params, null, token);
        catalogManager.getUserManager().update("user", new ObjectMap("email", newEmail), null, token);
        catalogManager.getUserManager().changePassword("user", PASSWORD, newPassword);

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

        catalogManager.getUserManager().changePassword("user", newPassword, PASSWORD);
        catalogManager.getUserManager().login("user", PASSWORD);

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

    private String getAdminToken() throws CatalogException, IOException {
        return catalogManager.getUserManager().loginAsAdmin("admin");
    }

    @Ignore
    @Test
    public void importLdapUsers() throws CatalogException, NamingException, IOException {
        // Action only for admins
        catalogManager.getUserManager().importRemoteEntities("ldap", Arrays.asList("pfurio", "imedina"), false, null, null, getAdminToken());
        // TODO: Validate the users have been imported
    }

    // To make this test work we will need to add a correct user and password to be able to login
    @Ignore
    @Test
    public void loginNotRegisteredUsers() throws CatalogException {
        // Action only for admins
        Group group = new Group("ldap", Collections.emptyList()).setSyncedFrom(new Group.Sync("ldap", "bio"));
        catalogManager.getStudyManager().createGroup(studyFqn, group, token);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), "@ldap", new Study.StudyAclParams("", AclParams.Action.SET,
                "view_only"), token);
        String token = catalogManager.getUserManager().login("user", "password");

        assertEquals(9, catalogManager.getSampleManager().count(studyFqn, new Query(), token).getNumTotalResults());

        // We remove the permissions for group ldap
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), "@ldap", new Study.StudyAclParams("", AclParams.Action.RESET,
                ""), this.token);

        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, new Query(), token).getNumTotalResults());
    }

    @Ignore
    @Test
    public void syncUsers() throws CatalogException {
        // Action only for admins
        String token = catalogManager.getUserManager().loginAsAdmin("admin");

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
    public void testAssignPermissions() throws CatalogException {
        catalogManager.getUserManager().create("test", "test", "test@mail.com", "test", null, 100L, Account.Type.GUEST, null);

        catalogManager.getStudyManager().createGroup("user@1000G:phase1", "group_cancer_some_thing_else",
                Collections.singletonList("test"), token);
        DataResult<Map<String, List<String>>> permissions = catalogManager.getStudyManager().updateAcl(
                Collections.singletonList("user@1000G:phase1"), "@group_cancer_some_thing_else",
                new Study.StudyAclParams("", AclParams.Action.SET, "view_only"), token);
        assertTrue(permissions.first().containsKey("@group_cancer_some_thing_else"));

        String token = catalogManager.getUserManager().login("test", "test");
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
     * Project methods
     * ***************************
     */


    @Test
    public void testGetAllProjects() throws Exception {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        DataResult<Project> projects = catalogManager.getProjectManager().get(query, null, token);
        assertEquals(1, projects.getNumResults());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("Permission denied");
        catalogManager.getProjectManager().get(query, null, sessionIdUser2);
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

        assertEquals(newProjectName, project.getName());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(project.getAttributes().get(entry.getKey()), entry.getValue());
        }

        options = new ObjectMap();
        options.put(ProjectDBAdaptor.QueryParams.ID.key(), "newProjectId");
        catalogManager.getProjectManager().update(projectId, options, null, token);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getProjectManager().update(projectId, options, null, token);
    }

    /**
     * Study methods
     * ***************************
     */

    @Test
    public void testModifyStudy() throws Exception {
        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user");
        String studyId =  catalogManager.getStudyManager().get(query, null, token).first().getId();

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
    }

    @Test
    public void testGetAllStudies() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        String projectId = catalogManager.getProjectManager().get(query, null, token).first().getId();
        catalogManager.getStudyManager().create(projectId, "study_1", null, "study_1",
                "description", null, null, null, null, null, null, null, token);

        catalogManager.getStudyManager().create(projectId, "study_2", null, "study_2", "description", null, null, null, null, null, null, null, token);

        catalogManager.getStudyManager().create(projectId, "study_3", null, "study_3", "description", null, null, null, null, null, null, null, token);

        String study_4 = catalogManager.getStudyManager().create(projectId, "study_4", null, "study_4", "description", null, null, null, null, null, null, null, token).first().getId();

        assertEquals(new HashSet<>(Collections.emptyList()), catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams
                .GROUP_USER_IDS.key(), "user2"), null, token).getResults().stream().map(Study::getId)
                .collect(Collectors.toSet()));

//        catalogManager.getStudyManager().createGroup(Long.toString(study_4), "admins", "user3", sessionIdUser);
        catalogManager.getStudyManager().updateGroup(study_4, "admins", ParamUtils.UpdateAction.SET,
                new GroupUpdateParams(Collections.singletonList("user3")), token);
        assertEquals(new HashSet<>(Arrays.asList("study_4")), catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams
                .GROUP_USER_IDS.key(), "user3"), null, token).getResults().stream().map(Study::getId)
                .collect(Collectors.toSet()));

        assertEquals(new HashSet<>(Arrays.asList("phase1", "phase3", "study_1", "study_2", "study_3", "study_4")),
                catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, token)
                        .getResults().stream().map(Study::getId).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("phase1", "phase3", "study_1", "study_2", "study_3", "study_4")),
                catalogManager.getStudyManager().get(new Query(), null, token).getResults().stream().map(Study::getId)
                        .collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("study_1", "study_2", "study_3", "study_4")), catalogManager.getStudyManager().get(new
                Query(StudyDBAdaptor.QueryParams.ID.key(), "~^study"), null, token).getResults().stream()
                .map(Study::getId).collect(Collectors.toSet()));
        assertEquals(Collections.singleton("s1"), catalogManager.getStudyManager().get(new Query(), null, sessionIdUser2).getResults()
                .stream()
                .map(Study::getId).collect(Collectors.toSet()));
    }

    @Test
    public void testGetId() throws CatalogException {
        // Create another study with alias phase3
        catalogManager.getStudyManager().create(project2, "phase3", null, "Phase 3", "d", null, null, null, null, null, null, null, sessionIdUser2);

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
        DataResult<Study> study = catalogManager.getStudyManager().create(String.valueOf(project2), "phase3", null, "Phase 3", "d", null, null, null, null, null, null, null, sessionIdUser2);
        try {
            studyManager.resolveIds(Collections.emptyList(), "*");
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        catalogManager.getStudyManager().updateGroup("phase3", "@members", ParamUtils.UpdateAction.ADD,
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
        DataResult<Study> study = catalogManager.getStudyManager().create(project2, "phase3", null, "Phase 3", "d", null, null, null, null, null, null, null, sessionIdUser2);
        catalogManager.getStudyManager().updateGroup("phase3", "@members", ParamUtils.UpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), sessionIdUser2);

        List<Study> studies = studyManager.resolveIds(Collections.singletonList("phase3"), "*");
        assertEquals(1, studies.size());
        assertEquals(study.first().getUid(), studies.get(0).getUid());
    }

    @Test
    public void testUpdateGroupInfo() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        studyManager.createGroup(studyFqn, "group1", null, token);
        studyManager.createGroup(studyFqn, "group2", null, token);

        Group.Sync syncFrom = new Group.Sync("auth", "aaa");
        studyManager.syncGroupWith(studyFqn, "group2", syncFrom, token);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Cannot modify already existing sync information");
        studyManager.syncGroupWith(studyFqn, "group2", syncFrom, token);
    }

    @Test
    public void testCreatePermissionRules() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList("user2", "user3"),
                Arrays.asList("VIEW", "UPDATE"));
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
                Arrays.asList("VIEW", "UPDATE"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Enums.Entity.SAMPLES, rules, token);
    }

    @Test
    public void testUpdatePermissionRulesNonExistingGroup() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList("user2", "@group"),
                Arrays.asList("VIEW", "UPDATE"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Enums.Entity.SAMPLES, rules, token);
    }

    @Test
    public void removeAllPermissionsToMember() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        // Assign permissions to study
        DataResult<Group> groupDataResult = studyManager.updateGroup(studyFqn, "@members", ParamUtils.UpdateAction.ADD,
                new GroupUpdateParams(Arrays.asList("user2", "user3")), token);
        assertEquals(3, groupDataResult.first().getUserIds().size());
        assertEquals("@members", groupDataResult.first().getId());

        // Obtain all samples from study
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, new Query(), QueryOptions
                .empty(), token);
        assertTrue(sampleDataResult.getNumResults() > 0);

        // Assign permissions to all the samples
        SampleAclParams sampleAclParams = new SampleAclParams("VIEW,UPDATE", AclParams.Action.SET, null, null, null);
        List<String> sampleIds = sampleDataResult.getResults().stream()
                .map(Sample::getId)
                .collect(Collectors.toList());
        DataResult<Map<String, List<String>>> sampleAclResult = catalogManager.getSampleManager().updateAcl(studyFqn,
                sampleIds, "user2,user3", sampleAclParams, token);
        assertEquals(sampleIds.size(), sampleAclResult.getNumResults());
        for (Map<String, List<String>> result : sampleAclResult.getResults()) {
            assertEquals(2, result.size());
            assertTrue(result.keySet().containsAll(Arrays.asList("user2", "user3")));
            assertTrue(result.get("user2").containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(),
                    SampleAclEntry.SamplePermissions.UPDATE.name())));
            assertTrue(result.get("user3").containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(),
                    SampleAclEntry.SamplePermissions.UPDATE.name())));
        }

        // Remove all the permissions to both users in the study. That should also remove the permissions they had in all the samples.
        groupDataResult = studyManager.updateGroup(studyFqn, "@members", ParamUtils.UpdateAction.REMOVE,
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
        DataResult<Group> groupDataResult = studyManager.updateGroup(studyFqn, "@members", ParamUtils.UpdateAction.ADD,
                new GroupUpdateParams(Arrays.asList("user2", "user3")), token);
        assertEquals(3, groupDataResult.first().getUserIds().size());
        assertEquals("@members", groupDataResult.first().getId());

        // Obtain all samples from study
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, new Query(), QueryOptions
                .empty(), token);
        assertTrue(sampleDataResult.getNumResults() > 0);

        // Assign permissions to all the samples
        SampleAclParams sampleAclParams = new SampleAclParams("VIEW,UPDATE", AclParams.Action.SET, null, null, null);
        List<String> sampleIds = sampleDataResult.getResults().stream().map(Sample::getId).collect(Collectors.toList());

        DataResult<Map<String, List<String>>> sampleAclResult = catalogManager.getSampleManager().updateAcl(studyFqn,
                sampleIds, "user2,user3", sampleAclParams, token);
        assertEquals(sampleIds.size(), sampleAclResult.getNumResults());
        for (Map<String, List<String>> result : sampleAclResult.getResults()) {
            assertEquals(2, result.size());
            assertTrue(result.keySet().containsAll(Arrays.asList("user2", "user3")));
            assertTrue(result.get("user2").containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(),
                    SampleAclEntry.SamplePermissions.UPDATE.name())));
            assertTrue(result.get("user3").containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name(),
                    SampleAclEntry.SamplePermissions.UPDATE.name())));
        }

        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.UpdateAction.REMOVE,
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
     * Job methods
     * ***************************
     */

    @Test
    public void testCreateJob() throws CatalogException, IOException {
        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user");
        String studyId = catalogManager.getStudyManager().get(query, null, token).first().getId();

        File outDir = catalogManager.getFileManager().createFolder(studyId, Paths.get("jobs", "myJob").toString(),
                null, true, null, QueryOptions.empty(), token).first();

        catalogManager.getJobManager().submit(studyId, "command-subcommand", null, Collections.emptyMap(), token);
        catalogManager.getJobManager().submit(studyId, "command-subcommand2", null, Collections.emptyMap(), token);

        catalogManager.getJobManager().create(studyId, new Job().setId("job1").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE))),
                QueryOptions.empty(), token);
        catalogManager.getJobManager().create(studyId, new Job().setId("job2").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR))),
                QueryOptions.empty(), token);
        catalogManager.getJobManager().create(studyId, new Job().setId("job3").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.UNREGISTERED))),
                QueryOptions.empty(), token);

        query = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.PENDING);
        DataResult<Job> unfinishedJobs = catalogManager.getJobManager().search(String.valueOf(studyId), query, null, token);
        assertEquals(2, unfinishedJobs.getNumResults());

        DataResult<Job> allJobs = catalogManager.getJobManager().search(String.valueOf(studyId), (Query) null, null, token);
        assertEquals(5, allJobs.getNumResults());

        thrown.expectMessage("status different");
        thrown.expect(CatalogException.class);
        catalogManager.getJobManager().create(studyId, new Job().setId("job5").setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.PENDING))),
                QueryOptions.empty(), token);
    }

    @Test
    public void testGetAllJobs() throws CatalogException {
        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user");
        String studyId = catalogManager.getStudyManager().get(query, null, token).first().getId();

        catalogManager.getJobManager().create(studyId, new Job().setId("myErrorJob"), null, token);

        QueryOptions options = new QueryOptions(QueryOptions.COUNT, true);
        DataResult<Job> allJobs = catalogManager.getJobManager().search(studyId, null, options, token);

        assertEquals(1, allJobs.getNumMatches());
        assertEquals(1, allJobs.getNumResults());
    }

    @Test
    public void submitJobOwner() throws CatalogException {
        OpenCGAResult<Job> job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(),
                token);

        assertEquals(1, job.getNumResults());
        assertEquals(Enums.ExecutionStatus.PENDING, job.first().getInternal().getStatus().getName());
    }

    @Test
    public void submitJobWithDependencies() throws CatalogException {
        Job job1 = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(), token).first();
        Job job2 = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(), token).first();

        Job job3 = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(), null, null,
                Arrays.asList(job1.getId(), job2.getId()), null, token).first();
        Job job4 = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(), null, null,
                Arrays.asList(job1.getUuid(), job2.getUuid()), null, token).first();

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
        catalogManager.getStudyManager().updateGroup(studyFqn, "@admins", ParamUtils.UpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("user3")), token);

        OpenCGAResult<Job> job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(),
                token);

        assertEquals(1, job.getNumResults());
        assertEquals(Enums.ExecutionStatus.PENDING, job.first().getInternal().getStatus().getName());
    }

    @Test
    public void submitJobWithoutPermissions() throws CatalogException {
        // Check there are no ABORTED jobs
        Query query = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.ABORTED);
        assertEquals(0, catalogManager.getJobManager().count(studyFqn, query, token).getNumMatches());

        // Grant view permissions, but no EXECUTION permission
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "user3",
                new Study.StudyAclParams("", AclParams.Action.SET, "view-only"), token);

        try {
            catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(), sessionIdUser3);
            fail("Sumbmission should have failed with a message saying the user does not have EXECUTION permissions");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Permission denied"));
        }

        // The previous execution should have created an ABORTED job
        OpenCGAResult<Job> search = catalogManager.getJobManager().search(studyFqn, query, null, token);
        assertEquals(1, search.getNumResults());
        assertEquals("variant-index", search.first().getTool().getId());
    }

    @Test
    public void submitJobWithPermissions() throws CatalogException {
        // Check there are no ABORTED jobs
        Query query = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.ABORTED);
        assertEquals(0, catalogManager.getJobManager().count(studyFqn, query, token).getNumMatches());

        // Grant view permissions, but no EXECUTION permission
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "user3",
                new Study.StudyAclParams(StudyAclEntry.StudyPermissions.EXECUTION.name(), AclParams.Action.SET, "view-only"), token);

        OpenCGAResult<Job> search = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(),
                sessionIdUser3);
        assertEquals(1, search.getNumResults());
        assertEquals(Enums.ExecutionStatus.PENDING, search.first().getInternal().getStatus().getName());
    }

    @Test
    public void visitJob() throws CatalogException {
        Job job = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM, new ObjectMap(), token)
                .first();

        Query query = new Query(JobDBAdaptor.QueryParams.VISITED.key(), false);
        assertEquals(1, catalogManager.getJobManager().count(studyFqn, query, token).getNumMatches());

        // Now we visit the job
        catalogManager.getJobManager().visit(studyFqn, job.getId(), token);
        assertEquals(0, catalogManager.getJobManager().count(studyFqn, query, token).getNumMatches());

        query.put(JobDBAdaptor.QueryParams.VISITED.key(), true);
        assertEquals(1, catalogManager.getJobManager().count(studyFqn, query, token).getNumMatches());

        // Now we update setting job to visited false again
        JobUpdateParams updateParams = new JobUpdateParams().setVisited(false);
        catalogManager.getJobManager().update(studyFqn, job.getId(), updateParams, QueryOptions.empty(), token);
        assertEquals(0, catalogManager.getJobManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(JobDBAdaptor.QueryParams.VISITED.key(), false);
        assertEquals(1, catalogManager.getJobManager().count(studyFqn, query, token).getNumMatches());
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
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
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
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("NAME", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
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
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs1", "vs1", true, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        DataResult<VariableSet> result = catalogManager.getStudyManager().deleteVariableSet(studyFqn, vs1.getId(), token);
        assertEquals(0, result.getNumResults());

        thrown.expect(CatalogException.class);    //VariableSet does not exist
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().getVariableSet(studyFqn, vs1.getId(), null, token);
    }

    @Test
    public void testGetAllVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().resolveId("1000G:phase1", "user");

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs1", "vs1", true, false, "Cancer", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();
        VariableSet vs2 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs2", "vs2", true, false, "Virgo", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();
        VariableSet vs3 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs3", "vs3", true, false, "Piscis", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();
        VariableSet vs4 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs4", "vs4", true, false, "Aries", null, variables,
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
                new QueryOptions(), token).first().getId();
        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), 0, "", "", null,
                        Collections.emptyMap()),
                new Variable("AGE", "", "", Variable.VariableType.DOUBLE, null, false, false, Collections.singletonList("0:99"), 1, "", "",
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
            catalogManager.getStudyManager().deleteVariableSet(studyFqn, vs1.getId(), token);
        } finally {
            VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyFqn, vs1.getId(), null, token).first();
            assertEquals(vs1.getUid(), variableSet.getUid());

            thrown.expect(CatalogDBException.class); //Expect the exception from the try
        }
    }

    /**
     * Sample methods
     * ***************************
     */

    /*
     * Cohort methods
     *
     */

    @Test
    public void testCreateCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), new QueryOptions(),
                token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), new QueryOptions(),
                token).first();
        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort")
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), null, token).first();

        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));
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

        Sample sampleId1 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_2"), new QueryOptions(),
                token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_3"), new QueryOptions(),
                token).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_4"), new QueryOptions(),
                token).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_5"), new QueryOptions(),
                token).first();
        Cohort myCohort1 = catalogManager.getCohortManager().create(studyId,
                new Cohort().setId("MyCohort1").setType(Enums.CohortType.FAMILY).setSamples(Arrays.asList(sampleId1,sampleId2, sampleId3)),
                null, token).first();
        Cohort myCohort2 = catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort2").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3, sampleId4)), null, token).first();
        Cohort myCohort3 = catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort3")
                .setType(Enums.CohortType.CASE_CONTROL).setSamples(Arrays.asList(sampleId3, sampleId4)), null, token).first();
        catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort4").setType(Enums.CohortType.TRIO)
                .setSamples(Arrays.asList(sampleId5, sampleId3)), null, token).first();

        long numResults;
        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId1.getId()), new QueryOptions(), token).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId1.getId()
                + "," + sampleId5.getId()), new QueryOptions(), token).getNumResults();
        assertEquals(3, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(), "MyCohort2"), new
                QueryOptions(), token).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(), "~MyCohort."), new
                QueryOptions(), token).getNumResults();
        assertEquals(4, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.TYPE.key(), Enums.CohortType.FAMILY), new QueryOptions(), token).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.TYPE.key(), "CASE_CONTROL"), new QueryOptions(), token).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getCohortManager().search(studyId, new Query(CohortDBAdaptor.QueryParams.UID.key(), myCohort1.getUid() +
                "," + myCohort2.getUid() + "," + myCohort3.getUid()), new QueryOptions(), token).getNumResults();
        assertEquals(3, numResults);
    }

    @Test
    public void testCreateCohortFail() throws CatalogException {
        thrown.expect(CatalogException.class);
        List<Sample> sampleList = Arrays.asList(new Sample().setUid(23L), new Sample().setUid(4L), new Sample().setUid(5L));
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY).setSamples(sampleList),
                null, token);
    }

    @Test
    public void testCreateCohortAlreadyExisting() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Collections.singletonList(sampleId1)), null, token).first();


        thrown.expect(CatalogDBException.class);
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Collections.singletonList(sampleId1)), null, token).first();
    }

    @Test
    public void testUpdateCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), new QueryOptions(),
                token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), new QueryOptions(),
                token).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_4"), new QueryOptions(),
                token).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_5"), new QueryOptions(),
                token).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), null, token).first();


        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));

        QueryOptions options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.UpdateAction.SET.name()));

        DataResult<Cohort> result = catalogManager.getCohortManager().update(studyFqn, myCohort.getId(),
                new CohortUpdateParams()
                        .setId("myModifiedCohort")
                        .setSamples(Arrays.asList(sampleId1.getId(), sampleId3.getId(), sampleId4.getId(), sampleId5.getId())),
                options, token);
        assertEquals(1, result.getNumUpdated());

        Cohort myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), token).first();
        assertEquals("myModifiedCohort", myModifiedCohort.getId());
        assertEquals(4, myModifiedCohort.getSamples().size());
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId4.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId5.getUid()));
    }

    /*                    */
    /* Test util methods  */
    /*                    */

    @Test
    public void testDeleteCohort() throws CatalogException {
        String studyId = "user@1000G:phase1";

        Sample sampleId1 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_2"), new QueryOptions(),
                token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_3"), new QueryOptions(),
                token).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), null, token).first();

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
        assertEquals(Status.DELETED, cohort.getInternal().getStatus().getName());
    }

    @Test
    public void getSamplesFromCohort() throws CatalogException, IOException {
        String studyId = "user@1000G:phase1";

        Sample sampleId1 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_2"), new QueryOptions(),
                token).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_3"), new QueryOptions(),
                token).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), null, token).first();

        DataResult<Sample> myCohort1 = catalogManager.getCohortManager().getSamples(studyId, "MyCohort", token);
        assertEquals(3, myCohort1.getNumResults());

        thrown.expect(CatalogParameterException.class);
        catalogManager.getCohortManager().getSamples(studyId, "MyCohort,AnotherCohort", token);

        thrown.expect(CatalogParameterException.class);
        catalogManager.getCohortManager().getSamples(studyId, "MyCohort,MyCohort", token);
    }

    /**
     * Individual methods
     * ***************************
     */

    @Test
    public void testAnnotateIndividual() throws CatalogException {
        VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyFqn, "vs", null, token).first();

        String individualId1 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_1")
                .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                new QueryOptions(), token).first().getId();
        String individualId2 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_2")
                .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                new QueryOptions(), token).first().getId();
        String individualId3 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_3")
                .setKaryotypicSex(IndividualProperty.KaryotypicSex.UNKNOWN).setLifeStatus(IndividualProperty.LifeStatus.UNKNOWN),
                new QueryOptions(), token).first().getId();

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

        List<String> individuals;
        individuals = catalogManager.getIndividualManager().search(studyFqn, new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), variableSet.getId() + ":NAME=~^INDIVIDUAL_"), null, token)
                .getResults().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_1", "INDIVIDUAL_2", "INDIVIDUAL_3")));

        individuals = catalogManager.getIndividualManager().search(studyFqn, new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), variableSet.getId() + ":AGE>10"), null, token)
                .getResults().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_2", "INDIVIDUAL_3")));

        individuals = catalogManager.getIndividualManager().search(studyFqn, new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), variableSet.getId() + ":AGE>10;" + variableSet.getId()
                + ":PHEN=CASE"), null, token)
                .getResults().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_3")));
    }

    @Test
    public void testUpdateIndividualInfo() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        DataResult<Individual> individualDataResult = individualManager.create(studyFqn, new Individual().setId("Test")
                .setDateOfBirth("19870214"), QueryOptions.empty(), token);
        assertEquals(1, individualDataResult.getNumResults());
        assertEquals("Test", individualDataResult.first().getId());
        assertEquals("19870214", individualDataResult.first().getDateOfBirth());

        DataResult<Individual> update = individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setDateOfBirth(""), QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());

        Individual individual = individualManager.get(studyFqn, individualDataResult.first().getId(), QueryOptions.empty(), token)
                .first();
        assertEquals("", individual.getDateOfBirth());

        update = individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setDateOfBirth("19870214"), QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());
        individual = individualManager.get(studyFqn, individualDataResult.first().getId(), QueryOptions.empty(), token)
                .first();
        assertEquals("19870214", individual.getDateOfBirth());

        update = individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setAttributes(Collections.singletonMap("key", "value")), QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());
        individual = individualManager.get(studyFqn, individualDataResult.first().getId(), QueryOptions.empty(), token)
                .first();
        assertEquals("value", individual.getAttributes().get("key"));

        update = individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setAttributes(Collections.singletonMap("key2", "value2")), QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());
        individual = individualManager.get(studyFqn, individualDataResult.first().getId(), QueryOptions.empty(), token)
                .first();
        assertEquals("value", individual.getAttributes().get("key")); // Keep "key"
        assertEquals("value2", individual.getAttributes().get("key2")); // add new "key2"

        // Wrong date of birth format
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Invalid date of birth format");
        individualManager.update(studyFqn, individualDataResult.first().getId(),
                new IndividualUpdateParams().setDateOfBirth("198421"), QueryOptions.empty(), token);
    }

    @Test
    public void testUpdateIndividualParents() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        individualManager.create(studyFqn, new Individual().setId("child"), QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("father"), QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("mother"), QueryOptions.empty(), token);

        DataResult<Individual> individualDataResult = individualManager.update(studyFqn, "child",
                new IndividualUpdateParams().setFather("father").setMother("mother"), QueryOptions.empty(), token);
        assertEquals(1, individualDataResult.getNumUpdated());

        Individual individual = individualManager.get(studyFqn, "child", QueryOptions.empty(), token).first();

        assertEquals("mother", individual.getMother().getId());
        assertEquals(1, individual.getMother().getVersion());

        assertEquals("father", individual.getFather().getId());
        assertEquals(1, individual.getFather().getVersion());
    }

    @Test
    public void testDeleteIndividualWithFamilies() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        Individual child = individualManager.create(studyFqn, new Individual()
                        .setId("child")
                        .setPhenotypes(Collections.singletonList(new Phenotype().setId("phenotype1")))
                        .setDisorders(Collections.singletonList(new Disorder().setId("disorder1"))),
                QueryOptions.empty(), token).first();
        Individual father = new Individual()
                        .setId("father")
                        .setPhenotypes(Collections.singletonList(new Phenotype().setId("phenotype2")))
                        .setDisorders(Collections.singletonList(new Disorder().setId("disorder2")));
        Individual mother = new Individual()
                        .setId("mother")
                        .setPhenotypes(Collections.singletonList(new Phenotype().setId("phenotype3")))
                        .setDisorders(Collections.singletonList(new Disorder().setId("disorder3")));

        FamilyManager familyManager = catalogManager.getFamilyManager();
        familyManager.create(studyFqn, new Family().setId("family1").setMembers(Collections.singletonList(father)),
                Collections.singletonList(child.getId()), QueryOptions.empty(), token);
        familyManager.create(studyFqn, new Family().setId("family2").setMembers(Collections.singletonList(mother)),
                Arrays.asList(father.getId(), child.getId()), QueryOptions.empty(), token);

        try {
            DataResult writeResult = individualManager.delete(studyFqn, new Query(IndividualDBAdaptor.QueryParams.ID.key(), "child"),
                    new ObjectMap(), token);
            fail("Expected fail");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("found in the families"));
        }

        DataResult writeResult = individualManager.delete(studyFqn, new Query(IndividualDBAdaptor.QueryParams.ID.key(), "child"),
                new ObjectMap(Constants.FORCE, true), token);
        assertEquals(1, writeResult.getNumDeleted());

        Family family1 = familyManager.get(studyFqn, "family1", QueryOptions.empty(), token).first();
        Family family2 = familyManager.get(studyFqn, "family2", QueryOptions.empty(), token).first();

        assertEquals(1, family1.getMembers().size());
        assertEquals(0, family1.getMembers().stream().filter(i -> i.getId().equals("child")).count());
        assertEquals(1, family1.getDisorders().size());
        assertEquals(0, family1.getDisorders().stream().filter(d -> d.getId().equals("disorder1")).count());
        assertEquals(1, family1.getPhenotypes().size());
        assertEquals(0, family1.getPhenotypes().stream().filter(d -> d.getId().equals("phenotype1")).count());

        assertEquals(2, family2.getMembers().size());
        assertEquals(0, family2.getMembers().stream().filter(i -> i.getId().equals("child")).count());
        assertEquals(2, family2.getDisorders().size());
        assertEquals(0, family2.getDisorders().stream().filter(d -> d.getId().equals("disorder1")).count());
        assertEquals(2, family2.getPhenotypes().size());
        assertEquals(0, family2.getPhenotypes().stream().filter(d -> d.getId().equals("phenotype1")).count());

        System.out.println(writeResult.getTime());
    }

    @Test
    public void testGetIndividualWithSamples() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        individualManager.create(studyFqn, new Individual().setId("individual1")
                        .setSamples(Arrays.asList(new Sample().setId("sample1"), new Sample().setId("sample2"), new Sample().setId("sample3"))),
                QueryOptions.empty(), token);
        individualManager.create(studyFqn, new Individual().setId("individual2")
                        .setSamples(Arrays.asList(new Sample().setId("sample4"), new Sample().setId("sample5"), new Sample().setId("sample6"))),
                QueryOptions.empty(), token);

        DataResult<Individual> search = individualManager.search(studyFqn, new Query(), QueryOptions.empty(), token);
        assertEquals(2, search.getNumResults());
        search.getResults().forEach(i -> {
            assertEquals(3, i.getSamples().size());
            assertTrue(org.apache.commons.lang3.StringUtils.isNotEmpty(i.getSamples().get(0).getCreationDate()));
            if (i.getId().equals("individual1")) {
                assertTrue(Arrays.asList("sample1", "sample2", "sample3").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            } else {
                assertTrue(Arrays.asList("sample4", "sample5", "sample6").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            }
        });

        search = individualManager.search(studyFqn, new Query(), new QueryOptions(QueryOptions.EXCLUDE, "samples.creationDate"),
                token);
        assertEquals(2, search.getNumResults());
        search.getResults().forEach(i -> {
            assertEquals(3, i.getSamples().size());
            assertTrue(org.apache.commons.lang3.StringUtils.isEmpty(i.getSamples().get(0).getCreationDate()));
            if (i.getId().equals("individual1")) {
                assertTrue(Arrays.asList("sample1", "sample2", "sample3").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            } else {
                assertTrue(Arrays.asList("sample4", "sample5", "sample6").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            }
        });

        search = individualManager.search(studyFqn, new Query(), new QueryOptions(QueryOptions.INCLUDE, "samples.id"),
                token);
        assertEquals(2, search.getNumResults());
        search.getResults().forEach(i -> {
            assertEquals(3, i.getSamples().size());
            assertTrue(org.apache.commons.lang3.StringUtils.isEmpty(i.getSamples().get(0).getCreationDate()));
            if (i.getId().equals("individual1")) {
                assertTrue(Arrays.asList("sample1", "sample2", "sample3").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            } else {
                assertTrue(Arrays.asList("sample4", "sample5", "sample6").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            }
        });


        search = individualManager.search(studyFqn, new Query(), new QueryOptions(QueryOptions.INCLUDE, "id,creationDate,samples.id"),
                token);
        assertEquals(2, search.getNumResults());
        search.getResults().forEach(i -> {
            assertTrue(org.apache.commons.lang3.StringUtils.isNotEmpty(i.getCreationDate()));
            assertTrue(org.apache.commons.lang3.StringUtils.isEmpty(i.getName()));
            assertEquals(3, i.getSamples().size());
            assertTrue(org.apache.commons.lang3.StringUtils.isEmpty(i.getSamples().get(0).getCreationDate()));
            if (i.getId().equals("individual1")) {
                assertTrue(Arrays.asList("sample1", "sample2", "sample3").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            } else {
                assertTrue(Arrays.asList("sample4", "sample5", "sample6").containsAll(
                        i.getSamples().stream().map(Sample::getId).collect(Collectors.toList())
                ));
            }
        });
    }
}