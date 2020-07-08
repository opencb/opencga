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
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.*;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileAclEntry;
import org.opencb.opencga.core.models.file.FileAclParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.sample.SampleAclParams;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.join;
import static org.junit.Assert.*;
import static org.opencb.opencga.catalog.utils.ParamUtils.AclAction.*;

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

    private final SampleAclParams allSamplePermissions = new SampleAclParams(null, null, null, ALL_SAMPLE_PERMISSIONS);
    private final SampleAclParams noSamplePermissions = new SampleAclParams(null, null, null, DENY_SAMPLE_PERMISSIONS);

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
        Configuration configuration = Configuration.load(getClass().getResource("/configuration-test.yml").openStream());
        CatalogManagerExternalResource.clearCatalog(configuration);

        catalogManager = new CatalogManager(configuration);
        catalogManager.installCatalogDB("dummy", "admin", "opencga@admin.com", "");
        fileManager = catalogManager.getFileManager();

        catalogManager.getUserManager().create(ownerUser, ownerUser, "email@ccc.ccc", password, "ASDF", null, Account.AccountType.FULL, null);
        catalogManager.getUserManager().create(studyAdminUser1, studyAdminUser1, "email@ccc.ccc", password, "ASDF", null, Account.AccountType.FULL, null);
        catalogManager.getUserManager().create(studyAdminUser2, studyAdminUser2, "email@ccc.ccc", password, "ASDF", null, Account.AccountType.FULL, null);
        catalogManager.getUserManager().create(memberUser, memberUser, "email@ccc.ccc", password, "ASDF", null, Account.AccountType.FULL, null);
        catalogManager.getUserManager().create(externalUser, externalUser, "email@ccc.ccc", password, "ASDF", null, Account.AccountType.FULL, null);

        ownerSessionId = catalogManager.getUserManager().login(ownerUser, password).getToken();
        studyAdmin1SessionId = catalogManager.getUserManager().login(studyAdminUser1, password).getToken();
        studyAdmin2SessionId = catalogManager.getUserManager().login(studyAdminUser2, password).getToken();
        memberSessionId = catalogManager.getUserManager().login(memberUser, password).getToken();
        externalSessionId = catalogManager.getUserManager().login(externalUser, password).getToken();

        p1 = catalogManager.getProjectManager().create("p1", "p1", null, "Homo sapiens",
                null, "GRCh38", new QueryOptions(), ownerSessionId).first().getId();
        Study study = catalogManager.getStudyManager().create(p1, "studyFqn", "studyFqn", "studyFqn", null, null, null,
                null, null, null, ownerSessionId).first();
        studyFqn = study.getFqn();
        studyUid = study.getUid();
        data_d1 = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/").toString(), true, null,
                QueryOptions.empty(), ownerSessionId).first().getPath();
        data_d1_d2 = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/").toString(), false,
                null, QueryOptions.empty(), ownerSessionId).first().getPath();
        data_d1_d2_d3 = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/d3/").toString(),
                false, null, QueryOptions.empty(), ownerSessionId).first().getPath();
        data_d1_d2_d3_d4 = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/d3/d4/").toString(),
                false, null, QueryOptions.empty(), ownerSessionId).first().getPath();
        catalogManager.getFileManager().create(studyFqn, new File().setPath("data/d1/d2/d3/d4/my.txt"), false, "file content", null, ownerSessionId);

        // Add studyAdminUser1 and studyAdminUser2 to admin group and admin role.
        catalogManager.getStudyManager().updateGroup(studyFqn, groupAdmin, ParamUtils.UpdateAction.SET,
                new GroupUpdateParams(Arrays.asList(studyAdminUser1, studyAdminUser2)), ownerSessionId);

        StudyAclParams aclParams1 = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), memberUser, aclParams1, ParamUtils.AclAction.ADD,
                studyAdmin1SessionId);
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), externalUser, aclParams, ParamUtils.AclAction.ADD,
                studyAdmin1SessionId);

        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1), externalUser, new FileAclParams(null, ALL_FILE_PERMISSIONS),
                ParamUtils.AclAction.SET, ownerSessionId);
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1_d2_d3), externalUser,
                new FileAclParams(null, DENY_FILE_PERMISSIONS), ParamUtils.AclAction.SET, ownerSessionId);
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1_d2_d3_d4_txt), externalUser,
                new FileAclParams(null, ALL_FILE_PERMISSIONS), ParamUtils.AclAction.SET, ownerSessionId);

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
                ParamUtils.AclAction.SET, false, ownerSessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp3.getId()), externalUser, noSamplePermissions,
                ParamUtils.AclAction.SET, false, ownerSessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp2.getId()), "*", noSamplePermissions,
                ParamUtils.AclAction.SET, false, ownerSessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp5.getId()), externalUser, noSamplePermissions,
                ParamUtils.AclAction.SET, false, ownerSessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp6.getId()), "@members", allSamplePermissions,
                ParamUtils.AclAction.SET, false, ownerSessionId);
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
        GroupUpdateParams groupParams = null;
        ParamUtils.UpdateAction action = null;
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(addUsers)) {
            groupParams = new GroupUpdateParams(Arrays.asList(addUsers.split(",")));
            action = ParamUtils.UpdateAction.ADD;
        } else if (org.apache.commons.lang3.StringUtils.isNotEmpty(removeUsers)) {
            groupParams = new GroupUpdateParams(Arrays.asList(removeUsers.split(",")));
            action = ParamUtils.UpdateAction.REMOVE;
        } else if (org.apache.commons.lang3.StringUtils.isNotEmpty(setUsers)) {
            groupParams = new GroupUpdateParams(Arrays.asList(setUsers.split(",")));
            action = ParamUtils.UpdateAction.SET;
        }
        if (groupParams == null) {
            throw new CatalogException("No action");
        }
        return catalogManager.getStudyManager().updateGroup(studyStr, groupId, action, groupParams, sessionId);
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
        catalogManager.getUserManager().create(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, Account.AccountType.FULL, null);
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), newUser, aclParams, ParamUtils.AclAction.ADD, ownerSessionId);
    }

    // A user with no permissions tries to add an existing user to a role
    @Test
    public void addExistingUserToRole2() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, Account.AccountType.FULL, null);
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("Only owners or administrative users");
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), newUser, aclParams, ParamUtils.AclAction.ADD, memberSessionId);
    }

    @Test
    public void addExistingGroupToRole() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, Account.AccountType.FULL, null);
        String group = "@newGroup";
//        catalogManager.addUsersToGroup(studyFqn, group, newUser, studyAdmin1SessionId);
        catalogManager.getStudyManager().createGroup(studyFqn, group, Collections.singletonList(newUser), studyAdmin1SessionId);
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), group, aclParams, ParamUtils.AclAction.ADD, studyAdmin1SessionId);
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
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), userNotRegistered, aclParams, ParamUtils.AclAction.ADD,
                studyAdmin1SessionId);
    }

    @Test
    public void addNonExistingGroupToRole() throws CatalogException {
        String groupNotRegistered = "@groupNotRegistered";
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), groupNotRegistered, new StudyAclParams("",
                AuthorizationManager.ROLE_ANALYST), ParamUtils.AclAction.SET, studyAdmin1SessionId);
    }

    @Test
    public void changeUserRole() throws CatalogException {
        DataResult<Map<String, List<String>>> studyAcls = catalogManager.getStudyManager().getAcls(Collections.singletonList(studyFqn),
                externalUser, false, studyAdmin1SessionId);

        assertEquals(1, studyAcls.getNumResults());
        assertEquals(1, studyAcls.first().size());
        assertTrue(studyAcls.first().containsKey(externalUser));

        // Change role
        StudyAclParams aclParams1 = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), externalUser, aclParams1, RESET,
                studyAdmin1SessionId);
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), externalUser, aclParams, ParamUtils.AclAction.ADD,
                studyAdmin1SessionId);

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
        StudyAclParams aclParams = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), externalUser, aclParams, RESET,
                studyAdmin1SessionId);

        DataResult<Map<String, List<String>>> studyAcls = catalogManager.getStudyManager().getAcls(Collections.singletonList(studyFqn),
                externalUser, false, studyAdmin1SessionId);
        assertEquals(0, studyAcls.getNumResults());
    }

    // A user with no permissions tries to remove an existing user from a role
    @Test
    public void removeUserFromRole2() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        StudyAclParams aclParams = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), externalUser, aclParams, RESET, memberSessionId);
    }

    @Test
    public void removeGroupFromRole() throws CatalogException {
        String group = "@newGroup";
        catalogManager.getStudyManager().createGroup(studyFqn, new Group(group, Arrays.asList(studyAdminUser1, studyAdminUser2)),
                studyAdmin1SessionId);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), group, new StudyAclParams("", "admin"), SET, ownerSessionId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, studyAdminUser1);
        DataResult<Map<String, List<String>>> studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(studyAdminUser1,
                study.getUid(), group);
        assertEquals(1, studyAcls.getNumResults());
        assertTrue(studyAcls.first().containsKey(group));

        assertEquals(AuthorizationManager.getAdminAcls().size(), studyAcls.first().get(group).size());
        for (StudyAclEntry.StudyPermissions adminAcl : AuthorizationManager.getAdminAcls()) {
            assertTrue(studyAcls.first().get(group).contains(adminAcl.name()));
        }

        StudyAclParams aclParams = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), group, aclParams, RESET, ownerSessionId);
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
        StudyAclParams aclParams = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), userNotRegistered, aclParams, RESET, ownerSessionId);
    }

    @Test
    public void removeNonExistingGroupFromRole() throws CatalogException {
        String groupNotRegistered = "@groupNotRegistered";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        StudyAclParams aclParams = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), groupNotRegistered, aclParams, RESET, ownerSessionId);
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
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.UpdateAction.REMOVE,
                new GroupUpdateParams(Collections.singletonList(externalUser)), ownerSessionId);
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
        catalogManager.getStudyManager().updateGroup(String.valueOf(studyFqn), "@members", ParamUtils.UpdateAction.REMOVE,
                new GroupUpdateParams(Collections.singletonList(externalUser)), ownerSessionId);
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
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1), memberUser, new FileAclParams(null, null), SET, ownerSessionId);
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
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.AccountType.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password).getToken();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data, null, sessionId);
    }

    @Test
    public void readFileSharedForGroup() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.AccountType.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password).getToken();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(studyFqn, "@external", newUser, ownerSessionId);
        catalogManager.getStudyManager().createGroup(studyFqn, newGroup, Collections.singletonList(newUser), ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), newGroup, aclParams, ADD, ownerSessionId);
        // Specify all file permissions for that concrete file
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1_d2_d3_d4), newGroup, new FileAclParams(null, ALL_FILE_PERMISSIONS), SET,
                ownerSessionId);
        catalogManager.getFileManager().get(studyFqn, data_d1_d2_d3_d4, null, sessionId);
    }

    @Test
    public void readFileForbiddenForUser() throws CatalogException {
        // Remove all permissions to the admin group in that folder
        catalogManager.getStudyManager().createGroup(studyFqn, groupMember, Collections.singletonList(externalUser), ownerSessionId);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), groupMember, new StudyAclParams("", "admin"), SET,
                ownerSessionId);
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1_d2), externalUser, new FileAclParams(null, DENY_FILE_PERMISSIONS), SET,
                ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data_d1_d2, null, externalSessionId);
    }

    @Test
    public void readFileForbiddenForGroupSharedWithUser() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.AccountType.FULL, null);
        String sessionId = catalogManager.getUserManager().login(ownerUser, password).getToken();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(studyFqn, "@external", newUser, ownerSessionId);
        catalogManager.getStudyManager().createGroup(studyFqn, newGroup, Collections.singletonList(newUser), ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), newGroup, aclParams, ADD, ownerSessionId);
        fileManager.updateAcl(studyFqn, Arrays.asList(data_d1_d2), newGroup, new FileAclParams(null, ALL_FILE_PERMISSIONS), SET, ownerSessionId);
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1_d2, null, sessionId);
        assertEquals(1, file.getNumResults());
    }

    /*--------------------------*/
    // Create file
    /*--------------------------*/

    @Test
    public void createFileByOwnerNoGroups() throws CatalogException {
        DataResult<File> folder = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/newFolder").toString(),
                true, null, QueryOptions.empty(), ownerSessionId);
        assertEquals(1, folder.getNumResults());
    }

    @Test
    public void createExplicitlySharedFile() throws CatalogException {
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/folder/").toString(), false, null,
                QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createInheritedSharedFile() throws CatalogException {
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/folder/").toString(), false,
                null, QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/d3/folder/").toString(), false,
                null, QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/d3/d4/folder/").toString(), false,
                null, QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("folder/").toString(), false, null,
                QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createFileNoStudyMember() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.AccountType.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password).getToken();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/my_folder/").toString(), false, null,
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
        StudyAclParams aclParams = new StudyAclParams("", null);
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), ownerUser, aclParams, ADD, ownerSessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp1.getId()), ownerUser, noSamplePermissions, SET,
                false, ownerSessionId);

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
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.AccountType.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password).getToken();
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
        catalogManager.getSampleManager().updateAcl(String.valueOf(studyFqn), Arrays.asList(smp2.getId()), externalUser,
                new SampleAclParams(null, null, null, ""), SET, false, ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, smp2.getId(), null, externalSessionId);
    }

    @Test
    public void readSampleSharedForGroup() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.AccountType.FULL, null);
        String sessionId = catalogManager.getUserManager().login(ownerUser, password).getToken();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(studyFqn, "@external", newUser, ownerSessionId);
        catalogManager.getStudyManager().createGroup(studyFqn, newGroup, Collections.singletonList(newUser), ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), newGroup, aclParams, ADD, ownerSessionId);

        // Share the sample with the group
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp4.getId()), newGroup, allSamplePermissions, SET,
                false, ownerSessionId);

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
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.AccountType.FULL, null);
        String sessionId = catalogManager.getUserManager().login(ownerUser, password).getToken();
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.UpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(newUser)), ownerSessionId);

        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, smp6.getId(), null, sessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void adminShareSampleWithOtherUser() throws CatalogException {
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(smp4.getId()), externalUser, allSamplePermissions,
                SET, false, studyAdmin1SessionId);
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
    public void aclQuery() throws CatalogException {
        SampleManager sampleManager = catalogManager.getSampleManager();

        sampleManager.create(studyFqn, new Sample().setId("s1"), QueryOptions.empty(), ownerSessionId);
        sampleManager.create(studyFqn, new Sample().setId("s2"), QueryOptions.empty(), ownerSessionId);
        sampleManager.create(studyFqn, new Sample().setId("s3"), QueryOptions.empty(), ownerSessionId);
        sampleManager.create(studyFqn, new Sample().setId("s4"), QueryOptions.empty(), ownerSessionId);
        sampleManager.create(studyFqn, new Sample().setId("s5"), QueryOptions.empty(), ownerSessionId);

        sampleManager.updateAcl(studyFqn, Collections.singletonList("s1"), memberUser,
                new SampleAclParams(null, null, null, SampleAclEntry.SamplePermissions.DELETE.name()), SET, false, ownerSessionId);
        sampleManager.updateAcl(studyFqn, Collections.singletonList("s2"), memberUser,
                new SampleAclParams(null, null, null, SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name()), SET, false,
                ownerSessionId);
        sampleManager.updateAcl(studyFqn, Collections.singletonList("s3"), memberUser, new SampleAclParams(
                null, null, null, SampleAclEntry.SamplePermissions.DELETE.name()), SET, false, ownerSessionId);
        sampleManager.updateAcl(studyFqn, Collections.singletonList("s4"), memberUser, new SampleAclParams(
                null, null, null, SampleAclEntry.SamplePermissions.VIEW.name()), SET, false, ownerSessionId);

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());

        // member user wants to know the samples for which he has DELETE permissions
        Query query = new Query(ParamConstants.ACL_PARAM, memberUser + ":" + SampleAclEntry.SamplePermissions.DELETE.name());
        List<Sample> results = sampleManager.search(studyFqn, query, options, memberSessionId).getResults();
        assertEquals(3, results.stream().map(Sample::getId).collect(Collectors.toSet()).size());
        assertTrue(Arrays.asList("s1", "s3", "smp6").containsAll(results.stream().map(Sample::getId).collect(Collectors.toSet())));

        // member user wants to know the samples for which he has UPDATE permissions
        // He should have UPDATE permissions in the same samples where he has DELETE permissions
        query = new Query(ParamConstants.ACL_PARAM, memberUser + ":" + SampleAclEntry.SamplePermissions.UPDATE.name());
        results = sampleManager.search(studyFqn, query, options, memberSessionId).getResults();
        System.out.println(results.stream().map(Sample::getId).collect(Collectors.toSet()));
        assertEquals(8, results.stream().map(Sample::getId).collect(Collectors.toSet()).size());
        assertTrue(Arrays.asList("s1", "s3", "s5", "smp1", "smp3", "smp4", "smp5", "smp6")
                .containsAll(results.stream().map(Sample::getId).collect(Collectors.toSet())));

        // Owner user wants to know the samples for which member user has DELETE permissions
        query = new Query(ParamConstants.ACL_PARAM, memberUser + ":" + SampleAclEntry.SamplePermissions.DELETE.name());
        results = sampleManager.search(studyFqn, query, options, ownerSessionId).getResults();
        assertEquals(3, results.stream().map(Sample::getId).collect(Collectors.toSet()).size());
        assertTrue(Arrays.asList("s1", "s3", "smp6").containsAll(results.stream().map(Sample::getId).collect(Collectors.toSet())));

        // member user wants to know the samples for which other user has DELETE permissions
        query = new Query(ParamConstants.ACL_PARAM, ownerUser + ":" + SampleAclEntry.SamplePermissions.DELETE.name());
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("Only study owners or admins");
        sampleManager.search(studyFqn, query, options, memberSessionId);
    }

    @Test
    public void getAclsTest() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        studyManager.createGroup(studyFqn, "group1", Collections.singletonList(externalUser), ownerSessionId);
        studyManager.createGroup(studyFqn, "group2", Collections.singletonList(externalUser), ownerSessionId);

        studyManager.updateAcl(Collections.singletonList(studyFqn), "@group1",
                new StudyAclParams(StudyAclEntry.StudyPermissions.VIEW_COHORTS.name(), ""), ADD, ownerSessionId);
        studyManager.updateAcl(Collections.singletonList(studyFqn), externalUser,
                new StudyAclParams(StudyAclEntry.StudyPermissions.VIEW_FILES.name(), ""), ADD, ownerSessionId);

        OpenCGAResult<Map<String, List<String>>> acls = studyManager.getAcls(Collections.singletonList(studyFqn), externalUser, false,
                externalSessionId);
        assertEquals(1, acls.getNumResults());
        assertTrue(acls.first().keySet().containsAll(Arrays.asList(externalUser, "@group1")));

        studyManager.updateAcl(Collections.singletonList(studyFqn), "@group2",
                new StudyAclParams(StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), ""), ADD, ownerSessionId);
        acls = studyManager.getAcls(Collections.singletonList(studyFqn), externalUser, false,
                externalSessionId);
        assertEquals(1, acls.getNumResults());
        assertTrue(acls.first().keySet().containsAll(Arrays.asList(externalUser, "@group1", "@group2")));

        assertEquals(StudyAclEntry.StudyPermissions.VIEW_FILES.name(), acls.first().get(externalUser).get(0));
        assertEquals(StudyAclEntry.StudyPermissions.VIEW_COHORTS.name(), acls.first().get("@group1").get(0));
        assertEquals(StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), acls.first().get("@group2").get(0));
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
        DataResult<Job> allJobs = catalogManager.getJobManager().search(studyFqn, null, null, sessionId);

        assertEquals(expectedJobs.size(), allJobs.getNumResults());
        allJobs.getResults().forEach(job -> assertTrue(expectedJobs + " does not contain job " + job.getId(), expectedJobs.contains(job
                .getUid())));
    }

    /////////// Aux methods
    private Map<String, Group> getGroupMap() throws CatalogException {
        return catalogManager.getStudyManager().get(studyFqn, null, ownerSessionId).first().getGroups().stream()
                .collect(Collectors.toMap(Group::getId, f -> f));
    }

}