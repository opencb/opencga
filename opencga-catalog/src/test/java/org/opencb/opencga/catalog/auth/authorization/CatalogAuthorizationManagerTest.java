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

package org.opencb.opencga.catalog.auth.authorization;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.file.FileAclEntry;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.GroupParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.join;
import static org.junit.Assert.*;

/**
 * Created on 19/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthorizationManagerTest extends GenericTest {

    private final String ownerUser = "owner";
    private final String studyAdminUser1 = "studyAdmin1";
    private final String studyAdminUser2 = "studyAdmin2";
    private final String memberUser = "member_member";
    private final String externalUser = "external";
    private final String password = "1234";

    private final String groupAdmin = "@admins";
    private final String groupMember = "@analyst";

    private final String ALL_FILE_PERMISSIONS = join(
            EnumSet.allOf(FileAclEntry.FilePermissions.class)
                    .stream()
                    .map(FileAclEntry.FilePermissions::name)
                    .collect(Collectors.toList()),
            ",");
    private final String DENY_FILE_PERMISSIONS = "";

    private final String ALL_SAMPLE_PERMISSIONS = join(
            EnumSet.allOf(SampleAclEntry.SamplePermissions.class)
                    .stream()
                    .map(SampleAclEntry.SamplePermissions::name)
                    .collect(Collectors.toList()),
            ",");
    private final String DENY_SAMPLE_PERMISSIONS = "";

    private final Sample.SampleAclParams allSamplePermissions =
            new Sample.SampleAclParams(ALL_SAMPLE_PERMISSIONS, AclParams.Action.SET, null, null,null);
    private final Sample.SampleAclParams noSamplePermissions =
            new Sample.SampleAclParams(DENY_SAMPLE_PERMISSIONS, AclParams.Action.SET, null, null,null);

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private CatalogManager catalogManager;
    private FileManager fileManager;
    private String ownerSessionId;
    private String studyAdmin1SessionId;
    private String studyAdmin2SessionId;
    private String memberSessionId;
    private String externalSessionId;
    private String p1;
    private String studyFqn;
    private long studyUid;
    private String data = "data/";                                   //
    private String data_d1 = "data/d1/";                             // Shared with member, Forbidden for @admins, Shared with studyAdmin1
    private String data_d1_d2 = "data/d1/d2/";                       // Forbidden for @admins
    private String data_d1_d2_d3 = "data/d1/d2/d3/";                 // Forbidden for member
    private String data_d1_d2_d3_d4 = "data/d1/d2/d3/d4/";           // Shared for @admins
    private String data_d1_d2_d3_d4_txt = "data/d1/d2/d3/d4/my.txt"; // Shared for member
    private Sample smp1;   // Shared with member
    private Sample smp2;   // Shared with studyAdmin1
    private Sample smp3;   // Shared with member
    private Sample smp4;   // Shared with @members
    private Sample smp6;   // Shared with *
    private Sample smp5;   // Shared with @members, forbidden for memberUse
    private String ind1 = "ind1";
    private String ind2 = "ind2";

    @Before
    public void before() throws Exception {
        Configuration configuration = Configuration.load(getClass().getResource("/configuration-test.yml")
                .openStream());
        configuration.getAdmin().setAlgorithm("HS256");
        configuration.getAdmin().setSecretKey("dummy");
        CatalogManagerExternalResource.clearCatalog(configuration);

        catalogManager = new CatalogManager(configuration);
        catalogManager.installCatalogDB("dummy", "admin", "opencga@admin.com", "");
        fileManager = catalogManager.getFileManager();

        catalogManager.getUserManager().create(ownerUser, ownerUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        catalogManager.getUserManager().create(studyAdminUser1, studyAdminUser1, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        catalogManager.getUserManager().create(studyAdminUser2, studyAdminUser2, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        catalogManager.getUserManager().create(memberUser, memberUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        catalogManager.getUserManager().create(externalUser, externalUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);

        ownerSessionId = catalogManager.getUserManager().login(ownerUser, password);
        studyAdmin1SessionId = catalogManager.getUserManager().login(studyAdminUser1, password);
        studyAdmin2SessionId = catalogManager.getUserManager().login(studyAdminUser2, password);
        memberSessionId = catalogManager.getUserManager().login(memberUser, password);
        externalSessionId = catalogManager.getUserManager().login(externalUser, password);

        p1 = catalogManager.getProjectManager().create("p1", "p1", null, null, "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), ownerSessionId).first().getId();
        Study study = catalogManager.getStudyManager().create(p1, "studyFqn", "studyFqn", "studyFqn", Study.Type.CASE_CONTROL, null, null,
                null, null, null, null, null, null, null, null, ownerSessionId).first();
        studyFqn = study.getFqn();
        studyUid = study.getUid();
        data_d1 = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/").toString(), null, true, null,
                QueryOptions.empty(), ownerSessionId).first().getPath();
        data_d1_d2 = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/").toString(), null, false,
                null, QueryOptions.empty(), ownerSessionId).first().getPath();
        data_d1_d2_d3 = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/d3/").toString(), null,
        false, null, QueryOptions.empty(), ownerSessionId).first().getPath();
        data_d1_d2_d3_d4 = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/d3/d4/").toString(),
                null, false, null, QueryOptions.empty(), ownerSessionId).first().getPath();
        catalogManager.getFileManager().create(studyFqn, new File().setPath("data/d1/d2/d3/d4/my.txt"), false, "file content", null, ownerSessionId);

        // Add studyAdminUser1 and studyAdminUser2 to admin group and admin role.
        catalogManager.getStudyManager().updateGroup(studyFqn, groupAdmin, new GroupParams(studyAdminUser1 + "," + studyAdminUser2,
                GroupParams.Action.SET), ownerSessionId);

        Study.StudyAclParams aclParams1 = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), memberUser, aclParams1, studyAdmin1SessionId);
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), externalUser, aclParams, studyAdmin1SessionId);

        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1), externalUser, new File.FileAclParams(ALL_FILE_PERMISSIONS,
                AclParams.Action.SET, null), ownerSessionId);
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1_d2_d3), externalUser,
                new File.FileAclParams(DENY_FILE_PERMISSIONS, AclParams.Action.SET, null), ownerSessionId);
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1_d2_d3_d4_txt), externalUser,
                new File.FileAclParams(ALL_FILE_PERMISSIONS, AclParams.Action.SET, null), ownerSessionId);

        smp1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("smp1"), QueryOptions.empty(), ownerSessionId).first();
        smp2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("smp2"), QueryOptions.empty(), ownerSessionId).first();
        smp3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("smp3"), QueryOptions.empty(), ownerSessionId).first();
        smp4 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("smp4"), QueryOptions.empty(), ownerSessionId).first();
        smp5 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("smp5"), QueryOptions.empty(), ownerSessionId).first();
        smp6 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("smp6"), QueryOptions.empty(), ownerSessionId).first();

        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("all").setSamples(Arrays.asList(smp1, smp2, smp3)),
                QueryOptions.empty(), ownerSessionId);

        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId(ind1), Collections.singletonList(smp1.getId()),
                QueryOptions.empty(), ownerSessionId);
        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId(ind2), Collections.singletonList(smp2.getId()),
                QueryOptions.empty(), ownerSessionId);

        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp1.getId()), externalUser, allSamplePermissions,
                ownerSessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp3.getId()), externalUser, noSamplePermissions,
                ownerSessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp2.getId()), "*", noSamplePermissions,
                ownerSessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp5.getId()), externalUser, noSamplePermissions,
                ownerSessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp6.getId()), "@members", allSamplePermissions,
                ownerSessionId);
    }

    @After
    public void after() throws Exception {
        catalogManager.close();
    }

    /*--------------------------*/
    // Add group members
    /*--------------------------*/

    private DataResult<Group> updateGroup(String studyStr, String groupId, @Nullable String addUsers, @Nullable String removeUsers,
                                          @Nullable String setUsers, String sessionId) throws CatalogException {
        GroupParams groupParams = null;
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(addUsers)) {
            groupParams = new GroupParams(addUsers, GroupParams.Action.ADD);
        } else if (org.apache.commons.lang3.StringUtils.isNotEmpty(removeUsers)) {
            groupParams = new GroupParams(removeUsers, GroupParams.Action.REMOVE);
        } else if (org.apache.commons.lang3.StringUtils.isNotEmpty(setUsers)) {
            groupParams = new GroupParams(setUsers, GroupParams.Action.SET);
        }
        if (groupParams == null) {
            throw new CatalogException("No action");
        }
        return catalogManager.getStudyManager().updateGroup(studyStr, groupId, groupParams, sessionId);
    }

    @Test
    public void addMemberToGroup() throws CatalogException {
        updateGroup(studyFqn, groupAdmin, externalUser, null, null, ownerSessionId);
        Map<String, Group> groups = getGroupMap();
        assertTrue(groups.get(groupAdmin).getUserIds().contains(externalUser));
    }

    @Test
    public void addMemberToGroupExistingNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        updateGroup(studyFqn, groupAdmin, externalUser, null, null, externalSessionId);
    }

    @Test
    public void changeGroupMembership() throws CatalogException {
        updateGroup(studyFqn, groupAdmin, externalUser, null, null, ownerSessionId);
//        catalogManager.addUsersToGroup(studyFqn, groupAdmin, externalUser, ownerSessionId);
        Map<String, Group> groups = getGroupMap();
        assertTrue(groups.get(groupAdmin).getUserIds().contains(externalUser));
//        thrown.expect(CatalogException.class);
//        catalogManager.addUsersToGroup(s1, groupMember, externalUser, ownerSessionId);
        updateGroup(studyFqn, groupAdmin, null, externalUser, null, ownerSessionId);
        catalogManager.getStudyManager().createGroup(studyFqn, new Group(groupMember, Collections.singletonList(externalUser)),
                ownerSessionId);
        //        catalogManager.updateGroup(Long.toString(s1), groupMember, externalUser, null, null, ownerSessionId);
        groups = getGroupMap();
        assertTrue(groups.get(groupMember).getUserIds().contains(externalUser));
        assertTrue(!groups.get(groupAdmin).getUserIds().contains(externalUser));
    }

//    @Test
//    public void addMemberToTheBelongingGroup() throws CatalogException {
//        catalogManager.addUsersToGroup(studyFqn, groupAdmin, externalUser, ownerSessionId);
//        thrown.expect(CatalogException.class);
//        catalogManager.addUsersToGroup(studyFqn, groupAdmin, externalUser, ownerSessionId);
//    }

    /*--------------------------*/
    // Remove group members
    /*--------------------------*/

    @Test
    public void removeMemberFromGroup() throws CatalogException {
        // Create new group
        catalogManager.getStudyManager().createGroup(studyFqn,
                new Group(groupMember, Arrays.asList(studyAdminUser1, studyAdminUser2)), ownerSessionId);

        // Remove one of the users
        updateGroup(studyFqn, groupMember, null, studyAdminUser1, null, ownerSessionId);
        assertFalse(getGroupMap().get(groupMember).getUserIds().contains(studyAdminUser1));

        // Remove the last user in the admin group
        updateGroup(studyFqn, groupMember, null, studyAdminUser2, null, ownerSessionId);
        assertFalse(getGroupMap().get(groupMember).getUserIds().contains(studyAdminUser2));

//        // Cannot remove group with defined ACLs
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("ACL defined");
        catalogManager.getStudyManager().deleteGroup(studyFqn, groupMember, ownerSessionId);
        assertNull(getGroupMap().get(groupMember));

    }

    @Test
    public void removeMemberFromGroupNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        updateGroup(studyFqn, groupAdmin, null, ownerUser, null, externalSessionId);
//        catalogManager.removeUsersFromGroup(studyFqn, groupAdmin, ownerUser, externalSessionId);
    }

    @Test
    public void removeMemberFromNonExistingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
//        catalogManager.removeUsersFromGroup(studyFqn, "NO_GROUP", ownerUser, ownerSessionId);
        updateGroup(studyFqn, "NO_GROUP", null, ownerUser, null, ownerSessionId);
    }

    @Test
    public void removeMemberFromNonBelongingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
//        catalogManager.removeUsersFromGroup(studyFqn, groupMember, ownerUser, ownerSessionId);
        updateGroup(studyFqn, groupMember, null, ownerUser, null, ownerSessionId);
    }

    /*--------------------------*/
    // Add users/groups to roles
    /*--------------------------*/

    // A user with proper permissions adds an existing user to a role
    @Test
    public void addExistingUserToRole() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), newUser, aclParams, ownerSessionId);
    }

    // A user with no permissions tries to add an existing user to a role
    @Test
    public void addExistingUserToRole2() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("Only owners or administrative users");
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), newUser, aclParams, memberSessionId);
    }

    @Test
    public void addExistingGroupToRole() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        String group = "@newGroup";
//        catalogManager.addUsersToGroup(studyFqn, group, newUser, studyAdmin1SessionId);
        catalogManager.getStudyManager().createGroup(studyFqn, group, group, newUser, studyAdmin1SessionId);
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), group, aclParams, studyAdmin1SessionId);
        DataResult<Map<String, List<String>>> studyAcls = catalogManager.getAuthorizationManager()
                .getStudyAcl(studyAdminUser1, studyUid, group);
        assertEquals(1, studyAcls.getNumResults());
        assertTrue(studyAcls.first().containsKey(group));

        assertEquals(AuthorizationManager.getAnalystAcls().size(), studyAcls.first().get(group).size());
        for (StudyAclEntry.StudyPermissions analystAcl : AuthorizationManager.getAnalystAcls()) {
            assertTrue(studyAcls.first().get(group).contains(analystAcl.name()));
        }
    }

    @Test
    public void addNonExistingUserToRole() throws CatalogException {
        String userNotRegistered = "userNotRegistered";
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("does not exist");
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), userNotRegistered, aclParams, studyAdmin1SessionId);
    }

    @Test
    public void addNonExistingGroupToRole() throws CatalogException {
        String groupNotRegistered = "@groupNotRegistered";
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), groupNotRegistered, new Study.StudyAclParams("",
                AclParams.Action.SET, AuthorizationManager.ROLE_ANALYST), studyAdmin1SessionId);
    }

    @Test
    public void changeUserRole() throws CatalogException {
        DataResult<Map<String, List<String>>> studyAcls = catalogManager.getStudyManager().getAcls(Collections.singletonList(studyFqn),
                externalUser, false, studyAdmin1SessionId);

        assertEquals(1, studyAcls.getNumResults());
        assertEquals(1, studyAcls.first().size());
        assertTrue(studyAcls.first().containsKey(externalUser));

        // Change role
        Study.StudyAclParams aclParams1 = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), externalUser, aclParams1, studyAdmin1SessionId);
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), externalUser, aclParams, studyAdmin1SessionId);

        studyAcls = catalogManager.getStudyManager().getAcls(Collections.singletonList(studyFqn), externalUser, false,
                studyAdmin1SessionId);

        assertEquals(1, studyAcls.getNumResults());
        assertEquals(1, studyAcls.first().size());
        assertTrue(studyAcls.first().containsKey(externalUser));
        assertEquals(AuthorizationManager.getAnalystAcls().size(), studyAcls.first().get(externalUser).size());
        for (StudyAclEntry.StudyPermissions analystAcl : AuthorizationManager.getAnalystAcls()) {
            assertTrue(studyAcls.first().get(externalUser).contains(analystAcl.name()));
        }
    }

     /*--------------------------*/
    // Remove users/groups from roles
    /*--------------------------*/

    // A user with proper permissions removes an existing user from a role
    @Test
    public void removeUserFromRole() throws CatalogException {
        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), externalUser, aclParams, studyAdmin1SessionId);

        DataResult<Map<String, List<String>>> studyAcls = catalogManager.getStudyManager().getAcls(Collections.singletonList(studyFqn),
                externalUser, false, studyAdmin1SessionId);
        assertEquals(0, studyAcls.getNumResults());
    }

    // A user with no permissions tries to remove an existing user from a role
    @Test
    public void removeUserFromRole2() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), externalUser, aclParams, memberSessionId);
    }

    @Test
    public void removeGroupFromRole() throws CatalogException {
        String group = "@newGroup";
        catalogManager.getStudyManager().createGroup(studyFqn, new Group(group, Arrays.asList(studyAdminUser1, studyAdminUser2)),
                studyAdmin1SessionId);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), group,
                new Study.StudyAclParams("", AclParams.Action.SET, "admin"), ownerSessionId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, studyAdminUser1);
        DataResult<Map<String, List<String>>> studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(studyAdminUser1,
                study.getUid(), group);
        assertEquals(1, studyAcls.getNumResults());
        assertTrue(studyAcls.first().containsKey(group));

        assertEquals(AuthorizationManager.getAdminAcls().size(), studyAcls.first().get(group).size());
        for (StudyAclEntry.StudyPermissions adminAcl : AuthorizationManager.getAdminAcls()) {
            assertTrue(studyAcls.first().get(group).contains(adminAcl.name()));
        }

        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), group, aclParams, ownerSessionId);
        String userId = catalogManager.getUserManager().getUserId(ownerSessionId);
        studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(userId, study.getUid(), group);
        assertEquals(0, studyAcls.getNumResults());
    }

    @Test
    public void removeNonExistingUserFromRole() throws CatalogException {
        String userNotRegistered = "userNotRegistered";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
//        catalogManager.unshareStudy(studyFqn, userNotRegistered, ownerSessionId);
        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), userNotRegistered, aclParams, ownerSessionId);
    }

    @Test
    public void removeNonExistingGroupFromRole() throws CatalogException {
        String groupNotRegistered = "@groupNotRegistered";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), groupNotRegistered, aclParams, ownerSessionId);
    }

    /*--------------------------*/
    // Read Project
    /*--------------------------*/

    @Test
    public void readProject() throws CatalogException {
        DataResult<Project> project = catalogManager.getProjectManager().get(p1, null, ownerSessionId);
        assertEquals(1, project.getNumResults());
        project = catalogManager.getProjectManager().get(p1, null, memberSessionId);
        assertEquals(1, project.getNumResults());
    }

    @Test
    public void readProjectDeny() throws CatalogException {
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", new GroupParams(externalUser, GroupParams.Action.REMOVE),
                ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getProjectManager().get(p1, null, externalSessionId);
    }

    /*--------------------------*/
    // Read Study
    /*--------------------------*/

    @Test
    public void readStudy() throws CatalogException {
        DataResult<Study> study = catalogManager.getStudyManager().get(studyFqn, null, ownerSessionId);
        assertEquals(1, study.getNumResults());
        study = catalogManager.getStudyManager().get(studyFqn, null, memberSessionId);
        assertEquals(1, study.getNumResults());
    }

    @Test
    public void readStudyDeny() throws CatalogException {
        catalogManager.getStudyManager().updateGroup(String.valueOf(studyFqn), "@members", new GroupParams(externalUser,
                GroupParams.Action.REMOVE), ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudyManager().get(studyFqn, null, externalSessionId);
    }

    /*--------------------------*/
    // Read file
    /*--------------------------*/

    @Test
    public void readFileByOwner() throws CatalogException {
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1, null, ownerSessionId);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readExplicitlySharedFolder() throws CatalogException {
        catalogManager.getFileManager().get(studyFqn, data_d1, null, externalSessionId);
    }

    @Test
    public void readExplicitlyUnsharedFile() throws CatalogException {
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1, null, memberSessionId);
        assertEquals(1, file.getNumResults());
        // Set an ACL with no permissions
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1), memberUser, new File.FileAclParams(null, AclParams.Action.SET, null),
                ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data_d1, null, memberSessionId);
    }

    @Test
    public void readInheritedSharedFile() throws CatalogException {
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1_d2, null, externalSessionId);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data_d1_d2_d3, null, externalSessionId);
    }

    @Test
    public void readInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data_d1_d2_d3_d4, null, externalSessionId);
    }

    @Test
    public void readExplicitlySharedFile() throws CatalogException {
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1_d2_d3_d4_txt, null, externalSessionId);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data, null, externalSessionId);
    }

    @Test
    public void readFileNoStudyMember() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data, null, sessionId);
    }

    @Test
    public void readFileSharedForGroup() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password);
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(studyFqn, "@external", newUser, ownerSessionId);
        catalogManager.getStudyManager().createGroup(studyFqn, newGroup, newGroup, newUser, ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), newGroup, aclParams, ownerSessionId);
        // Specify all file permissions for that concrete file
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1_d2_d3_d4), newGroup, new File.FileAclParams(ALL_FILE_PERMISSIONS,
                AclParams.Action.SET, null), ownerSessionId);
        catalogManager.getFileManager().get(studyFqn, data_d1_d2_d3_d4, null, sessionId);
    }

    @Test
    public void readFileForbiddenForUser() throws CatalogException {
        // Remove all permissions to the admin group in that folder
        catalogManager.getStudyManager().createGroup(studyFqn, groupMember, groupMember, externalUser, ownerSessionId);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), groupMember, new Study.StudyAclParams("", AclParams.Action.SET,
                "admin"), ownerSessionId);
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1_d2), externalUser, new File.FileAclParams(DENY_FILE_PERMISSIONS,
                AclParams.Action.SET, null), ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data_d1_d2, null, externalSessionId);
    }

    @Test
    public void readFileForbiddenForGroupSharedWithUser() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(ownerUser, password);
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(studyFqn, "@external", newUser, ownerSessionId);
        catalogManager.getStudyManager().createGroup(studyFqn, newGroup, newGroup, newUser, ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), newGroup, aclParams, ownerSessionId);
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1_d2), newGroup, new File.FileAclParams(ALL_FILE_PERMISSIONS,
                AclParams.Action.SET, null), ownerSessionId);
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1_d2, null, sessionId);
        assertEquals(1, file.getNumResults());
    }

    /*--------------------------*/
    // Create file
    /*--------------------------*/

    @Test
    public void createFileByOwnerNoGroups() throws CatalogException {
        DataResult<File> folder = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/newFolder").toString(),
                null, true, null, QueryOptions.empty(), ownerSessionId);
        assertEquals(1, folder.getNumResults());
    }

    @Test
    public void createExplicitlySharedFile() throws CatalogException {
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/folder/").toString(), null, false, null,
                QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createInheritedSharedFile() throws CatalogException {
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/folder/").toString(), null, false,
                null, QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/d3/folder/").toString(), null, false,
                null, QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/d3/d4/folder/").toString(), null, false,
                null, QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("folder/").toString(), null, false, null,
                QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createFileNoStudyMember() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/my_folder/").toString(), null, false, null,
                QueryOptions.empty(), sessionId);
    }

    /*--------------------------*/
    // Read Samples
    /*--------------------------*/

    @Test
    public void readSampleOwnerUser() throws CatalogException {
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, smp1.getId(), null, ownerSessionId);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSampleManager().get(studyFqn, smp2.getId(), null, ownerSessionId);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSampleManager().get(studyFqn, smp3.getId(), null, ownerSessionId);
        assertEquals(1, sample.getNumResults());

        // Owner always have access even if he has been removed all the permissions
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, null);
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), ownerUser, aclParams, ownerSessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp1.getId()), ownerUser, noSamplePermissions,
                ownerSessionId);

        sample = catalogManager.getSampleManager().get(studyFqn, smp1.getId(), null, ownerSessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleExplicitShared() throws CatalogException {
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, smp1.getId(), null, externalSessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleExplicitUnshared() throws CatalogException {
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, smp1.getId(), null, externalSessionId);
        assertEquals(1, sample.getNumResults());
        catalogManager.getAuthorizationManager().removeAcls(Collections.singletonList(smp1.getUid()),
                Collections.singletonList(externalUser), null, Enums.Resource.SAMPLE);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, smp1.getId(), null, externalSessionId);
    }

    @Test
    public void readSampleNoShared() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, smp2.getId(), null, externalSessionId);
    }

    @Test
    public void readSampleExplicitForbidden() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, smp3.getId(), null, externalSessionId);
    }

    @Test
    public void readSampleExternalUser() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, smp2.getId(), null, sessionId);
    }

    @Test
    public void readSampleAdminUser() throws CatalogException {
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, smp1.getId(), null, studyAdmin1SessionId);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSampleManager().get(studyFqn, smp3.getId(), null, studyAdmin1SessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleForbiddenForExternalUser() throws CatalogException {
        catalogManager.getSampleManager().updateAcl(String.valueOf(studyFqn), Arrays.asList(smp2.getId()), externalUser, new Sample.SampleAclParams("",
                AclParams.Action.SET, null, null, null), ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, smp2.getId(), null, externalSessionId);
    }

    @Test
    public void readSampleSharedForGroup() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(ownerUser, password);
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(studyFqn, "@external", newUser, ownerSessionId);
        catalogManager.getStudyManager().createGroup(studyFqn, newGroup, newGroup, newUser, ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), newGroup, aclParams, ownerSessionId);

        // Share the sample with the group
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp4.getId()), newGroup, allSamplePermissions,
                ownerSessionId);

        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, smp4.getId(), null, sessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleSharedForOthers() throws CatalogException {
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, smp6.getId(), null, externalSessionId);
        assertEquals(1, sample.getNumResults());
    }

    // Read a sample where the user is registered in OpenCGA. However, the user has not been included in the study.
    @Test
    public void readSampleSharedForOthersNotWithStudyPermissions() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(ownerUser, password);
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", new GroupParams(newUser, GroupParams.Action.ADD),
                ownerSessionId);

        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, smp6.getId(), null, sessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void adminShareSampleWithOtherUser() throws CatalogException {
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp4.getId()), externalUser, allSamplePermissions,
                studyAdmin1SessionId);
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, smp4.getId(), null, externalSessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readAllSamplesOwner() throws CatalogException {
        Map<Long, Sample> sampleMap = catalogManager.getSampleManager().search(studyFqn, new Query(), new QueryOptions(), ownerSessionId)
                .getResults().stream().collect(Collectors.toMap(Sample::getUid, f -> f));

        assertTrue(sampleMap.containsKey(smp1.getUid()));
        assertTrue(sampleMap.containsKey(smp2.getUid()));
        assertTrue(sampleMap.containsKey(smp3.getUid()));
    }

//    @Test
//    public void readAllSamplesAdmin() throws CatalogException {
//        Map<Long, Sample> sampleMap = catalogManager.getSampleManager().get(studyFqn, new Query(), new QueryOptions(), studyAdmin1SessionId)
//                .getResults().stream().collect(Collectors.toMap(Sample::getId, f -> f));
//
//        assertTrue(sampleMap.containsKey(smp1.getId()));
//        assertFalse(sampleMap.containsKey(smp2.getId()));
//        assertTrue(sampleMap.containsKey(smp3.getId()));
//    }

    @Test
    public void readAllSamplesMember() throws CatalogException {
        Map<Long, Sample> sampleMap = catalogManager.getSampleManager().search(studyFqn, new Query(), new QueryOptions(), externalSessionId)
                .getResults().stream().collect(Collectors.toMap(Sample::getUid, f -> f));

        assertTrue(sampleMap.containsKey(smp1.getUid()));
        assertFalse(sampleMap.containsKey(smp2.getUid()));
        assertFalse(sampleMap.containsKey(smp3.getUid()));
    }

    @Test
    public void readCohort() throws CatalogException {
        assertEquals(1, catalogManager.getCohortManager().search(studyFqn, new Query(), null, ownerSessionId).getNumResults());
        assertEquals(0, catalogManager.getCohortManager().search(studyFqn, new Query(), null, externalSessionId).getNumResults());
    }

    /*--------------------------*/
    // Read Individuals
    /*--------------------------*/

    @Test
    public void readIndividualByReadingSomeSample() throws CatalogException {
        catalogManager.getIndividualManager().get(null, ind1, null, memberSessionId);
    }

    @Test
    public void readIndividualForbidden() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getIndividualManager().get(null, ind2, null, externalSessionId);
    }

    @Test
    public void readIndividualStudyManager() throws CatalogException {
        catalogManager.getIndividualManager().get(null, ind2, null, studyAdmin1SessionId);
    }

    @Test
    public void readAllIndividuals() throws CatalogException {
        assertEquals(2, catalogManager.getIndividualManager().search(studyFqn, new Query(), null, ownerSessionId).getNumResults());
        assertEquals(2, catalogManager.getIndividualManager().search(studyFqn, new Query(), null, studyAdmin1SessionId).getNumResults());
        assertEquals(0, catalogManager.getIndividualManager().search(studyFqn, new Query(), null, externalSessionId).getNumResults());
    }



    /*--------------------------*/
    // Read Jobs
    /*--------------------------*/

    @Test
    public void getAllJobs() throws CatalogException {
        Job job = new Job()
                .setId("job1")
                .setOutDir(new File().setPath(data_d1_d2));
        long job1 = catalogManager.getJobManager().create(studyFqn, job, null, ownerSessionId).first().getUid();

        job.setId("job2");
        long job2 = catalogManager.getJobManager().create(studyFqn, job, null, ownerSessionId).first().getUid();

        job.setId("job3");
        long job3 = catalogManager.getJobManager().create(studyFqn, job, null, ownerSessionId).first().getUid();

        job.setId("job4");
        long job4 = catalogManager.getJobManager().create(studyFqn, job, null, ownerSessionId).first().getUid();


        checkGetAllJobs(Arrays.asList(job1, job2, job3, job4), ownerSessionId);    //Owner can see everything
        checkGetAllJobs(Collections.emptyList(), externalSessionId);               //Can't see inside data_d1_d2_d3
    }

    private void checkGetAllJobs(Collection<Long> expectedJobs, String sessionId) throws CatalogException {
        DataResult<Job> allJobs = catalogManager.getJobManager().search(studyFqn, (Query) null, null, sessionId);

        assertEquals(expectedJobs.size(), allJobs.getNumResults());
        allJobs.getResults().forEach(job -> assertTrue(expectedJobs + " does not contain job " + job.getName(), expectedJobs.contains(job
                .getUid())));
    }

    /////////// Aux methods
    private Map<String, Group> getGroupMap() throws CatalogException {
        return catalogManager.getStudyManager().get(studyFqn, null, ownerSessionId).first().getGroups().stream()
                .collect(Collectors.toMap(Group::getId, f -> f));
    }

}