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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.AclEntry;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileAclParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclParams;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.catalog.utils.ParamUtils.AclAction.*;

/**
 * Created on 19/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class CatalogAuthorizationManagerTest extends AbstractManagerTest {

    /*--------------------------*/
    // Add group members
    /*--------------------------*/

    private DataResult<Group> updateGroup(String studyStr, String groupId, @Nullable String addUsers, @Nullable String removeUsers,
                                          @Nullable String setUsers, String sessionId) throws CatalogException {
        GroupUpdateParams groupParams = null;
        ParamUtils.BasicUpdateAction action = null;
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(addUsers)) {
            groupParams = new GroupUpdateParams(Arrays.asList(addUsers.split(",")));
            action = ParamUtils.BasicUpdateAction.ADD;
        } else if (org.apache.commons.lang3.StringUtils.isNotEmpty(removeUsers)) {
            groupParams = new GroupUpdateParams(Arrays.asList(removeUsers.split(",")));
            action = ParamUtils.BasicUpdateAction.REMOVE;
        } else if (org.apache.commons.lang3.StringUtils.isNotEmpty(setUsers)) {
            groupParams = new GroupUpdateParams(Arrays.asList(setUsers.split(",")));
            action = ParamUtils.BasicUpdateAction.SET;
        }
        if (groupParams == null) {
            throw new CatalogException("No action");
        }
        return catalogManager.getStudyManager().updateGroup(studyStr, groupId, action, groupParams, sessionId);
    }

    @Test
    public void addMemberToGroup() throws CatalogException {
        updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, normalUserId3, null, null, ownerToken);
        Map<String, Group> groups = getGroupMap();
        assertTrue(groups.get(ParamConstants.ADMINS_GROUP).getUserIds().contains(normalUserId3));
    }

    @Test
    public void addMemberToGroupExistingNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, normalUserId3, null, null, normalToken3);
    }

    @Test
    public void changeGroupMembership() throws CatalogException {
        updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, normalUserId3, null, null, ownerToken);
//        catalogManager.addUsersToGroup(studyFqn, ParamConstants.ADMINS_GROUP, normalUserId3, token);
        Map<String, Group> groups = getGroupMap();
        assertTrue(groups.get(ParamConstants.ADMINS_GROUP).getUserIds().contains(normalUserId3));
//        thrown.expect(CatalogException.class);
//        catalogManager.addUsersToGroup(s1, restrictedGroup, normalUserId3, token);
        updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, null, normalUserId3, null, ownerToken);
        updateGroup(studyFqn, restrictedGroup, normalUserId1, null, null, ownerToken);
//        catalogManager.getStudyManager().createGroup(studyFqn, new Group(restrictedGroup, Collections.singletonList(normalUserId3)),
//                ownerToken);
        //        catalogManager.updateGroup(Long.toString(s1), restrictedGroup, normalUserId3, null, null, token);
        groups = getGroupMap();
        assertTrue(groups.get(restrictedGroup).getUserIds().contains(normalUserId1));
        assertTrue(!groups.get(ParamConstants.ADMINS_GROUP).getUserIds().contains(normalUserId1));
    }

    @Test
    public void adminUserRemovesFromAdminsGroup() throws CatalogException {
        thrown.expectMessage("Only the owner");
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Collections.singletonList(orgAdminUserId2)), studyAdminToken1);
    }

    @Test
    public void adminUserAddsFromAdminsGroup() throws CatalogException {
        thrown.expectMessage("Only the owner");
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(normalUserId3)), studyAdminToken1);
    }

    @Test
    public void adminUserSetsFromAdminsGroup() throws CatalogException {
        thrown.expectMessage("Only the owner");
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, ParamUtils.BasicUpdateAction.SET,
                new GroupUpdateParams(Collections.singletonList(orgAdminUserId1)), studyAdminToken1);
    }

//    @Test
//    public void addMemberToTheBelongingGroup() throws CatalogException {
//        catalogManager.addUsersToGroup(studyFqn, ParamConstants.ADMINS_GROUP, normalUserId3, token);
//        thrown.expect(CatalogException.class);
//        catalogManager.addUsersToGroup(studyFqn, ParamConstants.ADMINS_GROUP, normalUserId3, token);
//    }

    /*--------------------------*/
    // Remove group members
    /*--------------------------*/

    @Test
    public void removeMemberFromGroup() throws CatalogException {
        // Remove one of the users
        assertTrue(getGroupMap().get(restrictedGroup).getUserIds().contains(normalUserId3));
        updateGroup(studyFqn, restrictedGroup, null, normalUserId3, null, ownerToken);
        assertFalse(getGroupMap().get(restrictedGroup).getUserIds().contains(normalUserId3));

        // Remove the last user in the admin group
        updateGroup(studyFqn, restrictedGroup, null, normalUserId2, null, ownerToken);
        assertFalse(getGroupMap().get(restrictedGroup).getUserIds().contains(normalUserId2));

//        // Cannot remove group with defined ACLs
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("ACL defined");
        catalogManager.getStudyManager().deleteGroup(studyFqn, restrictedGroup, ownerToken);
        assertNull(getGroupMap().get(restrictedGroup));
    }

    @Test
    public void removeMemberFromGroupNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, null, orgOwnerUserId, null, normalToken3);
//        catalogManager.removeUsersFromGroup(studyFqn, ParamConstants.ADMINS_GROUP, ownerUserId, normalToken3);
    }

    @Test
    public void removeMemberFromNonExistingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
//        catalogManager.removeUsersFromGroup(studyFqn, "NO_GROUP", ownerUserId, token);
        updateGroup(studyFqn, "NO_GROUP", null, orgOwnerUserId, null, ownerToken);
    }

    /*--------------------------*/
    // Add users/groups to roles
    /*--------------------------*/

    // A user with proper permissions adds an existing user to a role
    @Test
    public void addExistingUserToRole() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(organizationId, newUser, newUser, "email@ccc.ccc", TestParamConstants.PASSWORD, "ASDF", null, Account.AccountType.FULL, ownerToken);
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(studyFqn, newUser, aclParams, ParamUtils.AclAction.ADD, ownerToken);
    }

    // A user with no permissions tries to add an existing user to a role
    @Test
    public void addExistingUserToRole2() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(organizationId, newUser, newUser, "email@ccc.ccc", TestParamConstants.PASSWORD, "ASDF", null, Account.AccountType.FULL, ownerToken);
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("administrators");
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(studyFqn, newUser, aclParams, ParamUtils.AclAction.ADD, normalToken3);
    }

    @Test
    public void addExistingGroupToRole() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(organizationId, newUser, newUser, "email@ccc.ccc", TestParamConstants.PASSWORD, "ASDF", null, Account.AccountType.FULL, ownerToken);
        String group = "@newGroup";
//        catalogManager.addUsersToGroup(studyFqn, group, newUser, adminToken1);
        catalogManager.getStudyManager().createGroup(studyFqn, group, Collections.singletonList(newUser), orgAdminToken1);
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(studyFqn, group, aclParams, ParamUtils.AclAction.ADD, orgAdminToken1);
        OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> studyAcls = catalogManager.getAuthorizationManager()
                .getStudyAcl(organizationId, studyUid, group, orgAdminUserId1);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(group, studyAcls.first().getAcl().get(0).getMember());

        assertEquals(AuthorizationManager.getAnalystAcls().size(), studyAcls.first().getAcl().get(0).getPermissions().size());
        for (StudyPermissions.Permissions analystAcl : AuthorizationManager.getAnalystAcls()) {
            assertTrue(studyAcls.first().getAcl().get(0).getPermissions().contains(analystAcl));
        }
    }

    @Test
    public void addNonExistingUserToRole() throws CatalogException {
        String userNotRegistered = "userNotRegistered";
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("does not exist");
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(studyFqn, userNotRegistered, aclParams, ParamUtils.AclAction.ADD,
                orgAdminToken1);
    }

    @Test
    public void addNonExistingGroupToRole() throws CatalogException {
        String groupNotRegistered = "@groupNotRegistered";
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().updateAcl(studyFqn, groupNotRegistered, new StudyAclParams("",
                AuthorizationManager.ROLE_ANALYST), ParamUtils.AclAction.SET, orgAdminToken1);
    }

    @Test
    public void changeUserRole() throws CatalogException {
        OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> studyAcls = catalogManager.getStudyManager()
                .getAcls(Collections.singletonList(studyFqn), normalUserId3, false, orgAdminToken1);

        assertEquals(1, studyAcls.getNumResults());
        assertEquals(1, studyAcls.first().getAcl().size());
        assertEquals(normalUserId3, studyAcls.first().getAcl().get(0).getMember());

        // Change role
        StudyAclParams aclParams1 = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId3, aclParams1, RESET,
                orgAdminToken1);
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId3, aclParams, ParamUtils.AclAction.ADD,
                orgAdminToken1);

        studyAcls = catalogManager.getStudyManager().getAcls(Collections.singletonList(studyFqn), normalUserId3, false,
                orgAdminToken1);

        assertEquals(1, studyAcls.getNumResults());
        assertEquals(1, studyAcls.first().getAcl().size());
        assertEquals(normalUserId3, studyAcls.first().getAcl().get(0).getMember());
        assertEquals(AuthorizationManager.getAnalystAcls().size(), studyAcls.first().getAcl().get(0).getPermissions().size());
        for (StudyPermissions.Permissions analystAcl : AuthorizationManager.getAnalystAcls()) {
            assertTrue(studyAcls.first().getAcl().get(0).getPermissions().contains(analystAcl));
        }
    }

    /*--------------------------*/
    // Remove users/groups from roles
    /*--------------------------*/

    // A user with proper permissions removes an existing user from a role
    @Test
    public void removeUserFromRole() throws CatalogException {
        StudyAclParams aclParams = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId3, aclParams, RESET, orgAdminToken1);

        OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> studyAcls = catalogManager.getStudyManager().getAcls(
                Collections.singletonList(studyFqn), normalUserId3, false, orgAdminToken1);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(1, studyAcls.first().getAcl().size());
        assertEquals(normalUserId3, studyAcls.first().getAcl().get(0).getMember());
        assertNull(studyAcls.first().getAcl().get(0).getPermissions());
    }

    // A user with proper permissions removes an existing user from a role
    @Test
    public void denyAllPermissions() throws CatalogException {
        StudyAclParams aclParams = new StudyAclParams("", null);
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId3, aclParams, SET, orgAdminToken1);

        OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> studyAcls = catalogManager.getStudyManager().getAcls(
                Collections.singletonList(studyFqn), normalUserId3, false, orgAdminToken1);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(1, studyAcls.first().getAcl().size());
        assertEquals(normalUserId3, studyAcls.first().getAcl().get(0).getMember());
        assertEquals(1, studyAcls.first().getAcl().get(0).getPermissions().size());
        assertTrue(studyAcls.first().getAcl().get(0).getPermissions().contains(StudyPermissions.Permissions.NONE));
    }

    // A user with no permissions tries to remove an existing user from a role
    @Test
    public void removeUserFromRole2() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        StudyAclParams aclParams = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId3, aclParams, RESET, normalToken3);
    }

    @Test
    public void removeGroupFromRole() throws CatalogException {
        String group = "@newGroup";
        catalogManager.getStudyManager().createGroup(studyFqn, new Group(group, Arrays.asList(orgAdminUserId1, orgAdminUserId2)),
                orgAdminToken1);
        catalogManager.getStudyManager().updateAcl(studyFqn, group, new StudyAclParams("", "admin"), SET, ownerToken);

        Study study = catalogManager.getStudyManager().get(studyFqn, QueryOptions.empty(), orgAdminToken1).first();
        OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(organizationId, study.getUid(), group, orgAdminUserId1
        );
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(group, studyAcls.first().getAcl().get(0).getMember());

        assertEquals(AuthorizationManager.getAdminAcls().size(), studyAcls.first().getAcl().get(0).getPermissions().size());
        for (StudyPermissions.Permissions adminAcl : AuthorizationManager.getAdminAcls()) {
            assertTrue(studyAcls.first().getAcl().get(0).getPermissions().contains(adminAcl));
        }

        StudyAclParams aclParams = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(studyFqn, group, aclParams, RESET, ownerToken);
        studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(organizationId, study.getUid(), group, orgOwnerUserId);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(1, studyAcls.first().getAcl().size());
        assertEquals(group, studyAcls.first().getAcl().get(0).getMember());
        assertNull(studyAcls.first().getAcl().get(0).getPermissions());
    }

    @Test
    public void removeNonExistingUserFromRole() throws CatalogException {
        String userNotRegistered = "userNotRegistered";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
//        catalogManager.unshareStudy(studyFqn, userNotRegistered, token);
        StudyAclParams aclParams = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(studyFqn, userNotRegistered, aclParams, RESET, ownerToken);
    }

    @Test
    public void removeNonExistingGroupFromRole() throws CatalogException {
        String groupNotRegistered = "@groupNotRegistered";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        StudyAclParams aclParams = new StudyAclParams(null, null);
        catalogManager.getStudyManager().updateAcl(studyFqn, groupNotRegistered, aclParams, RESET, ownerToken);
    }

    /*--------------------------*/
    // Read Project
    /*--------------------------*/

    @Test
    public void readProject() throws CatalogException {
        DataResult<Project> project = catalogManager.getProjectManager().get(project1, null, ownerToken);
        assertEquals(1, project.getNumResults());
        project = catalogManager.getProjectManager().get(project1, null, normalToken3);
        assertEquals(1, project.getNumResults());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("denied");
        catalogManager.getProjectManager().get(project3, null, normalToken3);
    }

    @Test
    public void readProjectDeny() throws CatalogException {
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Arrays.asList(normalUserId3, ParamConstants.ANONYMOUS_USER_ID)), ownerToken);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getProjectManager().get(project3, null, normalToken3);
    }

    /*--------------------------*/
    // Read Study
    /*--------------------------*/

    @Test
    public void readStudy() throws CatalogException {
        DataResult<Study> study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken);
        assertEquals(1, study.getNumResults());
        study = catalogManager.getStudyManager().get(studyFqn, null, normalToken3);
        assertEquals(1, study.getNumResults());
    }

    @Test
    public void readStudyDeny() throws CatalogException {
        catalogManager.getStudyManager().updateGroup(String.valueOf(studyFqn), "@members", ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Arrays.asList(normalUserId3, ParamConstants.ANONYMOUS_USER_ID)), ownerToken);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudyManager().get(studyFqn, null, normalToken3);
    }

    /*--------------------------*/
    // Read file
    /*--------------------------*/

    @Test
    public void readFileByOwner() throws CatalogException {
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1, null, ownerToken);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readExplicitlySharedFolder() throws CatalogException {
        catalogManager.getFileManager().get(studyFqn, data_d1, null, normalToken3);
    }

    @Test
    public void readExplicitlyUnsharedFile() throws CatalogException {
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1, null, normalToken3);
        assertEquals(1, file.getNumResults());
        // Set an ACL with no permissions
        catalogManager.getFileManager().updateAcl(studyFqn, Arrays.asList(data_d1), normalUserId3, new FileAclParams(null, null), SET, ownerToken);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data_d1, null, normalToken3);
    }

    @Test
    public void readInheritedSharedFile() throws CatalogException {
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1_d2, null, normalToken3);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data_d1_d2_d3, null, normalToken3);
    }

    @Test
    public void readInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data_d1_d2_d3_d4, null, normalToken3);
    }

    @Test
    public void readExplicitlySharedFile() throws CatalogException {
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1_d2_d3_d4_txt, null, normalToken3);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data, null, normalToken3);
    }

    @Test
    public void readFileNoStudyMember() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(organizationId, newUser, newUser, "asda@mail.com", TestParamConstants.PASSWORD, "org", 1000L, Account.AccountType.FULL, ownerToken);
        String sessionId = catalogManager.getUserManager().login(organizationId, newUser, TestParamConstants.PASSWORD).getToken();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data, null, sessionId);
    }

    @Test
    public void readFileSharedForGroup() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(organizationId, newUser, newUser, "asda@mail.com", TestParamConstants.PASSWORD, "org", 1000L, Account.AccountType.FULL, ownerToken);
        String sessionId = catalogManager.getUserManager().login(organizationId, newUser, TestParamConstants.PASSWORD).getToken();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(studyFqn, "@external", newUser, token);
        catalogManager.getStudyManager().createGroup(studyFqn, newGroup, Collections.singletonList(newUser), ownerToken);
        // Add the group to the locked role, so no permissions will be given
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(studyFqn, newGroup, aclParams, ADD, ownerToken);
        // Specify all file permissions for that concrete file
        catalogManager.getFileManager().updateAcl(studyFqn, Arrays.asList(data_d1_d2_d3_d4), newGroup, new FileAclParams(null, ALL_FILE_PERMISSIONS), SET,
                ownerToken);
        catalogManager.getFileManager().get(studyFqn, data_d1_d2_d3_d4, null, sessionId);
    }

    @Test
    public void readFileForbiddenForUser() throws CatalogException {
        // Remove all permissions to the admin group in that folder
        catalogManager.getStudyManager().updateAcl(studyFqn, restrictedGroup, new StudyAclParams("", AuthorizationManager.ROLE_ADMIN), SET, ownerToken);
        catalogManager.getFileManager().updateAcl(studyFqn, Arrays.asList(data_d1_d2), normalUserId3, new FileAclParams(null, DENY_FILE_PERMISSIONS), SET,
                ownerToken);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(studyFqn, data_d1_d2, null, normalToken3);
    }

    @Test
    public void readFileForbiddenForGroupSharedWithUser() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(organizationId, newUser, newUser, "asda@mail.com", TestParamConstants.PASSWORD, "org", 1000L, Account.AccountType.FULL, ownerToken);
        String sessionId = catalogManager.getUserManager().login(organizationId, orgOwnerUserId, TestParamConstants.PASSWORD).getToken();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(studyFqn, "@external", newUser, token);
        catalogManager.getStudyManager().createGroup(studyFqn, newGroup, Collections.singletonList(newUser), ownerToken);
        // Add the group to the locked role, so no permissions will be given
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(studyFqn, newGroup, aclParams, ADD, ownerToken);
        catalogManager.getFileManager().updateAcl(studyFqn, Arrays.asList(data_d1_d2), newGroup, new FileAclParams(null, ALL_FILE_PERMISSIONS), SET, ownerToken);
        DataResult<File> file = catalogManager.getFileManager().get(studyFqn, data_d1_d2, null, sessionId);
        assertEquals(1, file.getNumResults());
    }

    /*--------------------------*/
    // Create file
    /*--------------------------*/

    @Test
    public void createFileByOwnerNoGroups() throws CatalogException {
        DataResult<File> folder = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/newFolder").toString(),
                true, null, QueryOptions.empty(), ownerToken);
        assertEquals(1, folder.getNumResults());
    }

    @Test
    public void createExplicitlySharedFile() throws CatalogException {
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/folder/").toString(), false, null,
                QueryOptions.empty(), normalToken3);
    }

    @Test
    public void createInheritedSharedFile() throws CatalogException {
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/folder/").toString(), false,
                null, QueryOptions.empty(), normalToken3);
    }

    @Test
    public void createExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/d3/folder/").toString(), false,
                null, QueryOptions.empty(), normalToken3);
    }

    @Test
    public void createInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/d1/d2/d3/d4/folder/").toString(), false,
                null, QueryOptions.empty(), normalToken3);
    }

    @Test
    public void createNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("folder/").toString(), false, null,
                QueryOptions.empty(), normalToken3);
    }

    @Test
    public void createFileNoStudyMember() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(organizationId, newUser, newUser, "asda@mail.com", TestParamConstants.PASSWORD, "org", 1000L, Account.AccountType.FULL, ownerToken);
        String sessionId = catalogManager.getUserManager().login(organizationId, newUser, TestParamConstants.PASSWORD).getToken();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/my_folder/").toString(), false, null,
                QueryOptions.empty(), sessionId);
    }

    /*--------------------------*/
    // Read Samples
    /*--------------------------*/

    @Test
    public void readSampleOwnerUser() throws CatalogException {
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, s_1Id, null, ownerToken);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSampleManager().get(studyFqn, s_2Id, null, ownerToken);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSampleManager().get(studyFqn, s_3Id, null, ownerToken);
        assertEquals(1, sample.getNumResults());

        // Owner always have access even if he has been removed all the permissions
        StudyAclParams aclParams = new StudyAclParams("", null);
        catalogManager.getStudyManager().updateAcl(studyFqn, orgOwnerUserId, aclParams, ADD, ownerToken);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(s_1Id), orgOwnerUserId, noSamplePermissions, SET,
                ownerToken);

        sample = catalogManager.getSampleManager().get(studyFqn, s_1Id, null, ownerToken);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleExplicitShared() throws CatalogException {
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, s_1Id, null, normalToken3);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleExplicitUnshared() throws CatalogException {
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, s_1Id, null, normalToken3);
        assertEquals(1, sample.getNumResults());
        catalogManager.getAuthorizationManager().removeAcls(organizationId, Collections.singletonList(restrictedGroup),
                new AuthorizationManager.CatalogAclParams(Collections.singletonList(s_1Uid), null, Enums.Resource.SAMPLE));
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, s_1Id, null, normalToken3);
    }

    @Test
    public void readSampleNoShared() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, s_2Id, null, normalToken3);
    }

    @Test
    public void readSampleExplicitForbidden() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, s_3Id, null, normalToken3);
    }

    @Test
    public void readSampleExternalUser() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(organizationId, newUser, newUser, "asda@mail.com", TestParamConstants.PASSWORD, "org", 1000L,
                Account.AccountType.FULL, ownerToken);
        String sessionId = catalogManager.getUserManager().login(organizationId, newUser, TestParamConstants.PASSWORD).getToken();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, s_2Id, null, sessionId);
    }

    @Test
    public void readSampleAdminUser() throws CatalogException {
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, s_1Id, null, orgAdminToken1);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSampleManager().get(studyFqn, s_3Id, null, orgAdminToken1);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleForbiddenForExternalUser() throws CatalogException {
        catalogManager.getSampleManager().updateAcl(String.valueOf(studyFqn), Arrays.asList(s_2Id), normalUserId3,
                new SampleAclParams(null, null, null, null, ""), SET, ownerToken);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(studyFqn, s_2Id, null, normalToken3);
    }

    @Test
    public void readSampleSharedForGroup() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(organizationId, newUser, newUser, "asda@mail.com", TestParamConstants.PASSWORD, "org", 1000L, Account.AccountType.FULL, ownerToken);
        String sessionId = catalogManager.getUserManager().login(organizationId, orgOwnerUserId, TestParamConstants.PASSWORD).getToken();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(studyFqn, "@external", newUser, token);
        catalogManager.getStudyManager().createGroup(studyFqn, newGroup, Collections.singletonList(newUser), ownerToken);
        // Add the group to the locked role, so no permissions will be given
        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(studyFqn, newGroup, aclParams, ADD, ownerToken);

        // Share the sample with the group
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(s_4Id), newGroup, allSamplePermissions, SET,
                ownerToken);

        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, s_4Id, null, sessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleSharedForOthers() throws CatalogException {
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, s_6Id, null, normalToken3);
        assertEquals(1, sample.getNumResults());
    }

    // Read a sample where the user is registered in OpenCGA. However, the user has not been included in the study.
    @Test
    public void readSampleSharedForOthersNotWithStudyPermissions() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(organizationId, newUser, newUser, "asda@mail.com", TestParamConstants.PASSWORD, "org", 1000L,
                Account.AccountType.FULL, ownerToken);
        String sessionId = catalogManager.getUserManager().login(organizationId, orgOwnerUserId, TestParamConstants.PASSWORD).getToken();
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(newUser)), ownerToken);

        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, s_6Id, null, sessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void adminShareSampleWithOtherUser() throws CatalogException {
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(s_4Id), normalUserId3, allSamplePermissions,
                SET, orgAdminToken1);
        DataResult<Sample> sample = catalogManager.getSampleManager().get(studyFqn, s_4Id, null, normalToken3);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readAllSamplesOwner() throws CatalogException {
        Map<Long, Sample> sampleMap = catalogManager.getSampleManager().search(studyFqn, new Query(), new QueryOptions(), ownerToken)
                .getResults().stream().collect(Collectors.toMap(Sample::getUid, f -> f));

        assertTrue(sampleMap.containsKey(s_1Uid));
        assertTrue(sampleMap.containsKey(s_2Uid));
        assertTrue(sampleMap.containsKey(s_3Uid));
    }

//    @Test
//    public void readAllSamplesAdmin() throws CatalogException {
//        Map<Long, Sample> sampleMap = catalogManager.getSampleManager().get(studyFqn, new Query(), new QueryOptions(), adminToken1)
//                .getResults().stream().collect(Collectors.toMap(Sample::getId, f -> f));
//
//        assertTrue(sampleMap.containsKey(s_1.getId()));
//        assertFalse(sampleMap.containsKey(s_2.getId()));
//        assertTrue(sampleMap.containsKey(s_3.getId()));
//    }

    @Test
    public void readAllSamplesMember() throws CatalogException {
        Map<Long, Sample> sampleMap = catalogManager.getSampleManager().search(studyFqn, new Query(), new QueryOptions(), normalToken3)
                .getResults().stream().collect(Collectors.toMap(Sample::getUid, f -> f));

        assertTrue(sampleMap.containsKey(s_1Uid));
        assertFalse(sampleMap.containsKey(s_2Uid));
        assertFalse(sampleMap.containsKey(s_3Uid));
    }

    @Test
    public void aclQuery() throws CatalogException {
        catalogManager.getStudyManager().updateAcl(studyFqn, restrictedGroup, new StudyAclParams(null, AuthorizationManager.ROLE_ANALYST),
                ADD, ownerToken);
        SampleManager sampleManager = catalogManager.getSampleManager();

        sampleManager.create(studyFqn, new Sample().setId("s1"), QueryOptions.empty(), ownerToken);
        sampleManager.create(studyFqn, new Sample().setId("s2"), QueryOptions.empty(), ownerToken);
        sampleManager.create(studyFqn, new Sample().setId("s3"), QueryOptions.empty(), ownerToken);
        sampleManager.create(studyFqn, new Sample().setId("s4"), QueryOptions.empty(), ownerToken);
        sampleManager.create(studyFqn, new Sample().setId("s5"), QueryOptions.empty(), ownerToken);

        sampleManager.updateAcl(studyFqn, Collections.singletonList("s1"), normalUserId3,
                new SampleAclParams(null, null, null, null, SamplePermissions.DELETE.name()), SET, ownerToken);
        sampleManager.updateAcl(studyFqn, Collections.singletonList("s2"), normalUserId3,
                new SampleAclParams(null, null, null, null, SamplePermissions.VIEW_ANNOTATIONS.name()), SET,
                ownerToken);
        sampleManager.updateAcl(studyFqn, Collections.singletonList("s3"), normalUserId3, new SampleAclParams(
                null, null, null, null, SamplePermissions.DELETE.name()), SET, ownerToken);
        sampleManager.updateAcl(studyFqn, Collections.singletonList("s4"), normalUserId3, new SampleAclParams(
                null, null, null, null, SamplePermissions.VIEW.name()), SET, ownerToken);

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());

        // member user wants to know the samples for which he has DELETE permissions
        Query query = new Query(ParamConstants.ACL_PARAM, normalUserId3 + ":" + SamplePermissions.DELETE.name());
        List<Sample> results = sampleManager.search(studyFqn, query, options, normalToken3).getResults();
        assertEquals(4, results.stream().map(Sample::getId).collect(Collectors.toSet()).size());
        assertTrue(Arrays.asList("s1", "s3", s_1Id, s_6Id).containsAll(results.stream().map(Sample::getId).collect(Collectors.toSet())));

        // member user wants to know the samples for which he has UPDATE permissions
        // He should have UPDATE permissions in the same samples where he has DELETE permissions
        query = new Query(ParamConstants.ACL_PARAM, normalUserId3 + ":" + SamplePermissions.WRITE.name());
        results = sampleManager.search(studyFqn, query, options, normalToken3).getResults();
        System.out.println(results.stream().map(Sample::getId).collect(Collectors.toSet()));
        assertEquals(9, results.stream().map(Sample::getId).collect(Collectors.toSet()).size());
        assertTrue(Arrays.asList("s1", "s3", "s5", s_1Id, s_4Id, s_6Id, s_7Id, s_8Id, s_9Id)
                .containsAll(results.stream().map(Sample::getId).collect(Collectors.toSet())));

        // Owner user wants to know the samples for which member user has DELETE permissions
        query = new Query(ParamConstants.ACL_PARAM, normalUserId3 + ":" + SamplePermissions.DELETE.name());
        results = sampleManager.search(studyFqn, query, options, ownerToken).getResults();
        assertEquals(4, results.stream().map(Sample::getId).collect(Collectors.toSet()).size());
        assertTrue(Arrays.asList("s1", "s3", s_1Id, s_6Id).containsAll(results.stream().map(Sample::getId).collect(Collectors.toSet())));

        // member user wants to know the samples for which other user has DELETE permissions
        query = new Query(ParamConstants.ACL_PARAM, orgOwnerUserId + ":" + SamplePermissions.DELETE.name());
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("Only study owners or admins");
        sampleManager.search(studyFqn, query, options, normalToken3);
    }

    @Test
    public void getAclsTest() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        studyManager.createGroup(studyFqn, "group1", Collections.singletonList(normalUserId3), ownerToken);
        studyManager.createGroup(studyFqn, "group2", Collections.singletonList(normalUserId3), ownerToken);

        studyManager.updateAcl(studyFqn, "@group1",
                new StudyAclParams(StudyPermissions.Permissions.VIEW_COHORTS.name(), ""), ADD, ownerToken);
        studyManager.updateAcl(studyFqn, normalUserId3,
                new StudyAclParams(StudyPermissions.Permissions.VIEW_FILES.name(), ""), ADD, ownerToken);

        OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> acls = studyManager.getAcls(Collections.singletonList(studyFqn), normalUserId3, false,
                normalToken3);
        assertEquals(1, acls.getNumResults());
        assertEquals(1, acls.first().getAcl().size());
        assertEquals(normalUserId3, acls.first().getAcl().get(0).getMember());
        assertEquals(1, acls.first().getAcl().get(0).getGroups().stream().filter(g -> "@group1".equals(g.getId())).count());
//        assertTrue(acls.first().keySet().containsAll(Arrays.asList(normalUserId3, "@group1")));

        studyManager.updateAcl(studyFqn, "@group2",
                new StudyAclParams(StudyPermissions.Permissions.VIEW_SAMPLES.name(), ""), ADD, ownerToken);
        acls = studyManager.getAcls(Collections.singletonList(studyFqn), normalUserId3, false,
                normalToken3);
        assertEquals(1, acls.getNumResults());
        assertEquals(1, acls.first().getAcl().size());
        assertEquals(normalUserId3, acls.first().getAcl().get(0).getMember());
        assertEquals(2, acls.first().getAcl().get(0).getGroups().stream()
                .filter(g -> "@group1".equals(g.getId()) || "@group2".equals(g.getId()))
                .count());

        assertTrue(acls.first().getAcl().get(0).getPermissions().contains(StudyPermissions.Permissions.VIEW_FILES));
        for (AclEntry.GroupAclEntry<StudyPermissions.Permissions> group : acls.first().getAcl().get(0).getGroups()) {
            switch (group.getId()) {
                case "@group1":
                    assertTrue(group.getPermissions().contains(StudyPermissions.Permissions.VIEW_COHORTS));
                    break;
                case "@group2":
                    assertTrue(group.getPermissions().contains(StudyPermissions.Permissions.VIEW_SAMPLES));
                    break;
                default:
                    break;
            }
        }
    }

    @Test
    public void readCohort() throws CatalogException {
        assertEquals(1, catalogManager.getCohortManager().search(studyFqn, new Query(), null, ownerToken).getNumResults());
        assertEquals(0, catalogManager.getCohortManager().search(studyFqn, new Query(), null, normalToken3).getNumResults());
    }

    /*--------------------------*/
    // Read Individuals
    /*--------------------------*/

    @Test
    public void readIndividualByReadingSomeSample() throws CatalogException {
        catalogManager.getIndividualManager().get(null, ind1, null, normalToken3);
    }

    @Test
    public void readIndividualForbidden() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getIndividualManager().get(null, ind2, null, normalToken3);
    }

    @Test
    public void readIndividualStudyManager() throws CatalogException {
        catalogManager.getIndividualManager().get(studyFqn, ind2, null, orgAdminToken1);
    }

    @Test
    public void readAllIndividuals() throws CatalogException {
        assertEquals(2, catalogManager.getIndividualManager().search(studyFqn, new Query(), null, ownerToken).getNumResults());
        assertEquals(2, catalogManager.getIndividualManager().search(studyFqn, new Query(), null, orgAdminToken1).getNumResults());
        assertEquals(1, catalogManager.getIndividualManager().search(studyFqn, new Query(), null, normalToken3).getNumResults());
    }

    /*--------------------------*/
    // Read Jobs
    /*--------------------------*/

    @Test
    public void getAllJobs() throws CatalogException {
        Job job = new Job()
                .setId("job1")
                .setOutDir(new File().setPath(data_d1_d2));
        long job1 = catalogManager.getJobManager().create(studyFqn, job, INCLUDE_RESULT, ownerToken).first().getUid();

        job.setId("job2");
        long job2 = catalogManager.getJobManager().create(studyFqn, job, INCLUDE_RESULT, ownerToken).first().getUid();

        job.setId("job3");
        long job3 = catalogManager.getJobManager().create(studyFqn, job, INCLUDE_RESULT, ownerToken).first().getUid();

        job.setId("job4");
        long job4 = catalogManager.getJobManager().create(studyFqn, job, INCLUDE_RESULT, ownerToken).first().getUid();


        checkGetAllJobs(Arrays.asList(job1, job2, job3, job4), ownerToken);    //Owner can see everything
        checkGetAllJobs(Collections.emptyList(), normalToken3);               //Can't see inside data_d1_d2_d3
    }

    private void checkGetAllJobs(Collection<Long> expectedJobs, String sessionId) throws CatalogException {
        DataResult<Job> allJobs = catalogManager.getJobManager().search(studyFqn, null, null, sessionId);

        assertEquals(expectedJobs.size(), allJobs.getNumResults());
        allJobs.getResults().forEach(job -> assertTrue(expectedJobs + " does not contain job " + job.getId(), expectedJobs.contains(job
                .getUid())));
    }

    /////////// Aux methods
    private Map<String, Group> getGroupMap() throws CatalogException {
        return catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first().getGroups().stream()
                .collect(Collectors.toMap(Group::getId, f -> f));
    }

}