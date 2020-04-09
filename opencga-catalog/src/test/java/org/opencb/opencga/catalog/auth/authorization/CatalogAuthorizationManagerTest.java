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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
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
    private long p1;
    private long s1;
    private long data;                 //
    private long data_d1;              // Shared with member, Forbidden for @admins, Shared with studyAdmin1
    private long data_d1_d2;           // Forbidden for @admins
    private long data_d1_d2_d3;        // Forbidden for member
    private long data_d1_d2_d3_d4;     // Shared for @admins
    private long data_d1_d2_d3_d4_txt; // Shared for member
    private Sample smp1;   // Shared with member
    private Sample smp2;   // Shared with studyAdmin1
    private Sample smp3;   // Shared with member
    private Sample smp4;   // Shared with @members
    private Sample smp6;   // Shared with *
    private Sample smp5;   // Shared with @members, forbidden for memberUse
    private long ind1;
    private long ind2;

    @Before
    public void before() throws Exception {
        Configuration configuration = Configuration.load(getClass().getResource("/configuration-test.yml")
                .openStream());
        configuration.getAdmin().setSecretKey("dummy");
        configuration.getAdmin().setAlgorithm("HS256");
        CatalogManagerExternalResource.clearCatalog(configuration);

        catalogManager = new CatalogManager(configuration);
        catalogManager.installCatalogDB();
        fileManager = catalogManager.getFileManager();

        catalogManager.getUserManager().create(ownerUser, ownerUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        catalogManager.getUserManager().create(studyAdminUser1, studyAdminUser1, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        catalogManager.getUserManager().create(studyAdminUser2, studyAdminUser2, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        catalogManager.getUserManager().create(memberUser, memberUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        catalogManager.getUserManager().create(externalUser, externalUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);

        ownerSessionId = catalogManager.getUserManager().login(ownerUser, password).getToken();
        studyAdmin1SessionId = catalogManager.getUserManager().login(studyAdminUser1, password).getToken();
        studyAdmin2SessionId = catalogManager.getUserManager().login(studyAdminUser2, password).getToken();
        memberSessionId = catalogManager.getUserManager().login(memberUser, password).getToken();
        externalSessionId = catalogManager.getUserManager().login(externalUser, password).getToken();

        p1 = catalogManager.getProjectManager().create("p1", "p1", null, null, "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), ownerSessionId).first().getId();
        s1 = catalogManager.getStudyManager().create(String.valueOf(p1), "s1", "s1", Study.Type.CASE_CONTROL, null, null, null, null,
                null, null, null, null, null, null, ownerSessionId).first().getId();
        data_d1 = catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("data/d1/").toString(), null, true, null,
                QueryOptions.empty(), ownerSessionId).first().getId();
        data_d1_d2 = catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("data/d1/d2/").toString(), null, false,
                null, QueryOptions.empty(), ownerSessionId).first().getId();
        data_d1_d2_d3 = catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("data/d1/d2/d3/").toString(), null,
        false, null, QueryOptions.empty(), ownerSessionId).first().getId();
        data_d1_d2_d3_d4 = catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("data/d1/d2/d3/d4/").toString(),
                null, false, null, QueryOptions.empty(), ownerSessionId).first().getId();
        QueryResult<File> queryResult = catalogManager.getFileManager().create(Long.toString(s1), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "data/d1/d2/d3/d4/my.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, ownerSessionId);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(("file " +
                "content").getBytes()), queryResult.first(), ownerSessionId, false, false, true);
        data_d1_d2_d3_d4_txt = catalogManager.getFileManager().get(queryResult.first().getId(), null, ownerSessionId).first().getId();
        data = catalogManager.getFileManager().get(s1, new Query(FileDBAdaptor.QueryParams.PATH.key(), "data/"), null, ownerSessionId).first().getId();

        // Add studyAdminUser1 and studyAdminUser2 to admin group and admin role.
        catalogManager.getStudyManager().updateGroup(Long.toString(s1), groupAdmin, new GroupParams(studyAdminUser1 + "," +
                        studyAdminUser2, GroupParams.Action.SET), ownerSessionId);

//        Study.StudyAclParams aclParams2 = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ADMIN);
//        catalogManager.getStudyManager().updateAcl(Long.toString(s1), groupAdmin, aclParams2, ownerSessionId).get(0);

        Study.StudyAclParams aclParams1 = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), memberUser, aclParams1, studyAdmin1SessionId).get(0);
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), externalUser, aclParams, studyAdmin1SessionId).get(0);

        fileManager.updateAcl(Long.toString(s1), Arrays.asList(Long.toString(data_d1)), externalUser, new File.FileAclParams(ALL_FILE_PERMISSIONS,
                AclParams.Action.SET, null), ownerSessionId);
        //        fileManager.updateAcl(Long.toString(s1), Long.toString(data_d1), groupAdmin, new File.FileAclParams(ALL_FILE_PERMISSIONS,
//                AclParams.Action.SET, null), ownerSessionId);
        fileManager.updateAcl(Long.toString(s1), Arrays.asList(Long.toString(data_d1_d2_d3)), externalUser, new File.FileAclParams
                (DENY_FILE_PERMISSIONS, AclParams.Action.SET, null), ownerSessionId);
        fileManager.updateAcl(Long.toString(s1), Arrays.asList(Long.toString(data_d1_d2_d3_d4_txt)), externalUser, new File.FileAclParams
                (ALL_FILE_PERMISSIONS, AclParams.Action.SET, null), ownerSessionId);

        smp1 = catalogManager.getSampleManager().create(Long.toString(s1), "smp1", null, null, null, false, null, new HashMap<>(), null, null,
                ownerSessionId).first();
        smp2 = catalogManager.getSampleManager().create(Long.toString(s1), "smp2", null, null, null, false, null, new HashMap<>(), null, null,
                ownerSessionId).first();
        smp3 = catalogManager.getSampleManager().create(Long.toString(s1), "smp3", null, null, null, false, null, new HashMap<>(), null, null,
                ownerSessionId).first();
        smp4 = catalogManager.getSampleManager().create(Long.toString(s1), "smp4", null, null, null, false, null, new HashMap<>(), null, null,
                ownerSessionId).first();
        smp5 = catalogManager.getSampleManager().create(Long.toString(s1), "smp5", null, null, null, false, null, new HashMap<>(), null, null,
                ownerSessionId).first();
        smp6 = catalogManager.getSampleManager().create(Long.toString(s1), "smp6", null, null, null, false, null, new HashMap<>(), null, null,
                ownerSessionId).first();
        catalogManager.getCohortManager().create(s1, "all", Study.Type.COLLECTION, "", Arrays.asList(smp1, smp2, smp3), null, null,
                ownerSessionId);
        ind1 = catalogManager.getIndividualManager().create(s1, "ind1", "", (long) 0, (long) 0, Individual.Sex.UNKNOWN, "", "", "", "",
                "", Individual.KaryotypicSex.UNKNOWN, Individual.LifeStatus.UNKNOWN, Individual.AffectationStatus.UNKNOWN, null,
                ownerSessionId).first().getId();
        ind2 = catalogManager.getIndividualManager().create(s1, "ind2", "", (long) 0, (long) 0, Individual.Sex.UNKNOWN, "", "", "", "",
                "", Individual.KaryotypicSex.UNKNOWN, Individual.LifeStatus.UNKNOWN, Individual.AffectationStatus.UNKNOWN, null, ownerSessionId).first().getId();
        catalogManager.getSampleManager().update(smp1.getId(), new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), ind1),
                QueryOptions.empty(), ownerSessionId);
        catalogManager.getSampleManager().update(smp2.getId(), new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), ind2),
                QueryOptions.empty(), ownerSessionId);

        catalogManager.getSampleManager().updateAcl(Long.toString(s1), Arrays.asList(Long.toString(smp1.getId())), externalUser, allSamplePermissions, ownerSessionId);
        catalogManager.getSampleManager().updateAcl(Long.toString(s1), Arrays.asList(Long.toString(smp3.getId())), externalUser, noSamplePermissions, ownerSessionId);
        catalogManager.getSampleManager().updateAcl(Long.toString(s1), Arrays.asList(Long.toString(smp2.getId())), "*", noSamplePermissions, ownerSessionId);
        catalogManager.getSampleManager().updateAcl(Long.toString(s1), Arrays.asList(Long.toString(smp5.getId())), externalUser, noSamplePermissions, ownerSessionId);
        catalogManager.getSampleManager().updateAcl(Long.toString(s1), Arrays.asList(Long.toString(smp6.getId())), "@members", allSamplePermissions, ownerSessionId);
    }

    @After
    public void after() throws Exception {
        catalogManager.close();
    }

    /*--------------------------*/
    // Add group members
    /*--------------------------*/

    private QueryResult<Group> updateGroup(String studyStr, String groupId, @Nullable String addUsers, @Nullable String removeUsers,
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
        updateGroup(Long.toString(s1), groupAdmin, externalUser, null, null, ownerSessionId);
        Map<String, Group> groups = getGroupMap();
        assertTrue(groups.get(groupAdmin).getUserIds().contains(externalUser));
    }

    @Test
    public void addMemberToGroupExistingNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        updateGroup(Long.toString(s1), groupAdmin, externalUser, null, null, externalSessionId);
    }

    @Test
    public void changeGroupMembership() throws CatalogException {
        updateGroup(Long.toString(s1), groupAdmin, externalUser, null, null, ownerSessionId);
//        catalogManager.addUsersToGroup(s1, groupAdmin, externalUser, ownerSessionId);
        Map<String, Group> groups = getGroupMap();
        assertTrue(groups.get(groupAdmin).getUserIds().contains(externalUser));
//        thrown.expect(CatalogException.class);
//        catalogManager.addUsersToGroup(s1, groupMember, externalUser, ownerSessionId);
        updateGroup(Long.toString(s1), groupAdmin, null, externalUser, null, ownerSessionId);
        catalogManager.getStudyManager().createGroup(Long.toString(s1), new Group(groupMember, Collections.singletonList(externalUser)),
                ownerSessionId);
        //        catalogManager.updateGroup(Long.toString(s1), groupMember, externalUser, null, null, ownerSessionId);
        groups = getGroupMap();
        assertTrue(groups.get(groupMember).getUserIds().contains(externalUser));
        assertTrue(!groups.get(groupAdmin).getUserIds().contains(externalUser));
    }

//    @Test
//    public void addMemberToTheBelongingGroup() throws CatalogException {
//        catalogManager.addUsersToGroup(s1, groupAdmin, externalUser, ownerSessionId);
//        thrown.expect(CatalogException.class);
//        catalogManager.addUsersToGroup(s1, groupAdmin, externalUser, ownerSessionId);
//    }

    /*--------------------------*/
    // Remove group members
    /*--------------------------*/

    @Test
    public void removeMemberFromGroup() throws CatalogException {
        // Create new group
        catalogManager.getStudyManager().createGroup(String.valueOf(s1),
                new Group(groupMember, Arrays.asList(studyAdminUser1, studyAdminUser2)), ownerSessionId);

        // Remove one of the users
        updateGroup(Long.toString(s1), groupMember, null, studyAdminUser1, null, ownerSessionId);
        assertFalse(getGroupMap().get(groupMember).getUserIds().contains(studyAdminUser1));

        // Remove the last user in the admin group
        updateGroup(Long.toString(s1), groupMember, null, studyAdminUser2, null, ownerSessionId);
        assertFalse(getGroupMap().get(groupMember).getUserIds().contains(studyAdminUser2));

//        // Cannot remove group with defined ACLs
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("ACL defined");
        catalogManager.getStudyManager().deleteGroup(Long.toString(s1), groupMember, ownerSessionId);
        assertNull(getGroupMap().get(groupMember));

    }

    @Test
    public void removeMemberFromGroupNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        updateGroup(Long.toString(s1), groupAdmin, null, ownerUser, null, externalSessionId);
//        catalogManager.removeUsersFromGroup(s1, groupAdmin, ownerUser, externalSessionId);
    }

    @Test
    public void removeMemberFromNonExistingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
//        catalogManager.removeUsersFromGroup(s1, "NO_GROUP", ownerUser, ownerSessionId);
        updateGroup(Long.toString(s1), "NO_GROUP", null, ownerUser, null, ownerSessionId);
    }

    @Test
    public void removeMemberFromNonBelongingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
//        catalogManager.removeUsersFromGroup(s1, groupMember, ownerUser, ownerSessionId);
        updateGroup(Long.toString(s1), groupMember, null, ownerUser, null, ownerSessionId);
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
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), newUser, aclParams, ownerSessionId).get(0);
    }

    // A user with no permissions tries to add an existing user to a role
    @Test
    public void addExistingUserToRole2() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("Only owners or administrative users");
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), newUser, aclParams, memberSessionId).get(0);
    }

    @Test
    public void addExistingGroupToRole() throws CatalogException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, Account.Type.FULL, null);
        String group = "@newGroup";
//        catalogManager.addUsersToGroup(s1, group, newUser, studyAdmin1SessionId);
        catalogManager.getStudyManager().createGroup(Long.toString(s1), group, newUser, studyAdmin1SessionId);
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), group, aclParams, studyAdmin1SessionId).get(0);
        QueryResult<StudyAclEntry> studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(studyAdminUser1, s1, group);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(group, studyAcls.first().getMember());
        assertArrayEquals(AuthorizationManager.getAnalystAcls().toArray(), studyAcls.first().getPermissions().toArray());
    }

    @Test
    public void addNonExistingUserToRole() throws CatalogException {
        String userNotRegistered = "userNotRegistered";
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("does not exist");
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), userNotRegistered, aclParams, studyAdmin1SessionId).get(0);
    }

    @Test
    public void addNonExistingGroupToRole() throws CatalogException {
        String groupNotRegistered = "@groupNotRegistered";
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), groupNotRegistered, new Study.StudyAclParams("",
                AclParams.Action.SET, AuthorizationManager.ROLE_ANALYST), studyAdmin1SessionId);
    }

    @Test
    public void changeUserRole() throws CatalogException {
        QueryResult<StudyAclEntry> studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(studyAdminUser1, s1, externalUser);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(externalUser, studyAcls.first().getMember());

        // Change role
        Study.StudyAclParams aclParams1 = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), externalUser, aclParams1, studyAdmin1SessionId).get(0);
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), externalUser, aclParams, studyAdmin1SessionId).get(0);
        String userId = catalogManager.getUserManager().getUserId(studyAdmin1SessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, Long.toString(s1));
        studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(userId, studyId, externalUser);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(externalUser, studyAcls.first().getMember());
        assertArrayEquals(AuthorizationManager.getAnalystAcls().toArray(), studyAcls.first().getPermissions().toArray());
    }

     /*--------------------------*/
    // Remove users/groups from roles
    /*--------------------------*/

    // A user with proper permissions removes an existing user from a role
    @Test
    public void removeUserFromRole() throws CatalogException {
//        catalogManager.unshareStudy(s1, externalUser, studyAdmin1SessionId);
        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), externalUser, aclParams, studyAdmin1SessionId).get(0);
        String userId = catalogManager.getUserManager().getUserId(studyAdmin1SessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, Long.toString(s1));
        QueryResult<StudyAclEntry> studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(userId, studyId, externalUser);
        assertEquals(0, studyAcls.getNumResults());
    }

    // A user with no permissions tries to remove an existing user from a role
    @Test
    public void removeUserFromRole2() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), externalUser, aclParams, memberSessionId).get(0);
    }

    @Test
    public void removeGroupFromRole() throws CatalogException {
        String group = "@newGroup";
        catalogManager.getStudyManager().createGroup(Long.toString(s1), new Group(group, Arrays.asList(studyAdminUser1, studyAdminUser2)),
                studyAdmin1SessionId);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), group, new Study.StudyAclParams("", AclParams.Action.SET, "admin"), ownerSessionId);

        long studyId1 = catalogManager.getStudyManager().getId(studyAdminUser1, Long.toString(s1));
        QueryResult<StudyAclEntry> studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(studyAdminUser1, studyId1, group);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(group, studyAcls.first().getMember());
        assertArrayEquals(AuthorizationManager.getAdminAcls().toArray(), studyAcls.first().getPermissions().toArray());

        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), group, aclParams, ownerSessionId).get(0);
        String userId = catalogManager.getUserManager().getUserId(ownerSessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, Long.toString(s1));
        studyAcls = catalogManager.getAuthorizationManager().getStudyAcl(userId, studyId, group);
        assertEquals(0, studyAcls.getNumResults());
    }

    @Test
    public void removeNonExistingUserFromRole() throws CatalogException {
        String userNotRegistered = "userNotRegistered";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
//        catalogManager.unshareStudy(s1, userNotRegistered, ownerSessionId);
        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), userNotRegistered, aclParams, ownerSessionId).get(0);
    }

    @Test
    public void removeNonExistingGroupFromRole() throws CatalogException {
        String groupNotRegistered = "@groupNotRegistered";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), groupNotRegistered, aclParams, ownerSessionId).get(0);
    }

    /*--------------------------*/
    // Read Project
    /*--------------------------*/

    @Test
    public void readProject() throws CatalogException {
        QueryResult<Project> project = catalogManager.getProjectManager().get(String.valueOf((Long) p1), null, ownerSessionId);
        assertEquals(1, project.getNumResults());
        project = catalogManager.getProjectManager().get(String.valueOf((Long) p1), null, memberSessionId);
        assertEquals(1, project.getNumResults());
    }

    @Test
    public void readProjectDeny() throws CatalogException {
        catalogManager.getStudyManager().updateGroup(String.valueOf(s1), "@members", new GroupParams(externalUser,
                        GroupParams.Action.REMOVE), ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getProjectManager().get(String.valueOf((Long) p1), null, externalSessionId);
    }

    /*--------------------------*/
    // Read Study
    /*--------------------------*/

    @Test
    public void readStudy() throws CatalogException {
        QueryResult<Study> study = catalogManager.getStudyManager().get(String.valueOf((Long) s1), null, ownerSessionId);
        assertEquals(1, study.getNumResults());
        study = catalogManager.getStudyManager().get(String.valueOf((Long) s1), null, memberSessionId);
        assertEquals(1, study.getNumResults());
    }

    @Test
    public void readStudyDeny() throws CatalogException {
        catalogManager.getStudyManager().updateGroup(String.valueOf(s1), "@members", new GroupParams(externalUser,
                GroupParams.Action.REMOVE), ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudyManager().get(String.valueOf((Long) s1), null, externalSessionId);
    }

    /*--------------------------*/
    // Read file
    /*--------------------------*/

    @Test
    public void readFileByOwner() throws CatalogException {
        QueryResult<File> file = catalogManager.getFileManager().get(data_d1, null, ownerSessionId);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readExplicitlySharedFolder() throws CatalogException {
        catalogManager.getFileManager().get(data_d1, null, externalSessionId);
    }

    @Test
    public void readExplicitlyUnsharedFile() throws CatalogException {
        QueryResult<File> file = catalogManager.getFileManager().get(data_d1, null, memberSessionId);
        assertEquals(1, file.getNumResults());
        // Set an ACL with no permissions
        fileManager.updateAcl(Long.toString(s1), Arrays.asList(Long.toString(data_d1)), memberUser, new File.FileAclParams(null, AclParams.Action.SET,
                null), ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(data_d1, null, memberSessionId);
    }

    @Test
    public void readInheritedSharedFile() throws CatalogException {
        QueryResult<File> file = catalogManager.getFileManager().get(data_d1_d2, null, externalSessionId);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(data_d1_d2_d3, null, externalSessionId);
    }

    @Test
    public void readInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(data_d1_d2_d3_d4, null, externalSessionId);
    }

    @Test
    public void readExplicitlySharedFile() throws CatalogException {
        QueryResult<File> file = catalogManager.getFileManager().get(data_d1_d2_d3_d4_txt, null, externalSessionId);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(data, null, externalSessionId);
    }

    @Test
    public void readFileNoStudyMember() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password).getToken();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(data, null, sessionId);
    }

    @Test
    public void readFileSharedForGroup() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password).getToken();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(s1, "@external", newUser, ownerSessionId);
        catalogManager.getStudyManager().createGroup(Long.toString(s1), newGroup, newUser, ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), newGroup, aclParams, ownerSessionId).get(0);
        // Specify all file permissions for that concrete file
        fileManager.updateAcl(Long.toString(s1), Arrays.asList(Long.toString(data_d1_d2_d3_d4)), newGroup, new File.FileAclParams(ALL_FILE_PERMISSIONS,
                AclParams.Action.SET, null), ownerSessionId);
        catalogManager.getFileManager().get(data_d1_d2_d3_d4, null, sessionId);
    }

    @Test
    public void readFileForbiddenForUser() throws CatalogException {
        // Remove all permissions to the admin group in that folder
        catalogManager.getStudyManager().createGroup(Long.toString(s1), groupMember, externalUser, ownerSessionId);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), groupMember, new Study.StudyAclParams("", AclParams.Action.SET,
                "admin"), ownerSessionId);
        fileManager.updateAcl(Long.toString(s1), Arrays.asList(Long.toString(data_d1_d2)), externalUser, new File.FileAclParams(DENY_FILE_PERMISSIONS,
                AclParams.Action.SET, null), ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().get(data_d1_d2, null, externalSessionId);
    }

    @Test
    public void readFileForbiddenForGroupSharedWithUser() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(ownerUser, password).getToken();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(s1, "@external", newUser, ownerSessionId);
        catalogManager.getStudyManager().createGroup(Long.toString(s1), newGroup, newUser, ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), newGroup, aclParams, ownerSessionId).get(0);
        fileManager.updateAcl(Long.toString(s1), Arrays.asList(Long.toString(data_d1_d2)), newGroup, new File.FileAclParams(ALL_FILE_PERMISSIONS,
                AclParams.Action.SET, null), ownerSessionId);
        QueryResult<File> file = catalogManager.getFileManager().get(data_d1_d2, null, sessionId);
        assertEquals(1, file.getNumResults());
    }

    /*--------------------------*/
    // Create file
    /*--------------------------*/

    @Test
    public void createFileByOwnerNoGroups() throws CatalogException {
        QueryResult<File> folder = catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("data/newFolder").toString(),
                null, true, null, QueryOptions.empty(), ownerSessionId);
        assertEquals(1, folder.getNumResults());
    }

    @Test
    public void createExplicitlySharedFile() throws CatalogException {
        catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("data/d1/folder/").toString(), null, false, null,
                QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createInheritedSharedFile() throws CatalogException {
        catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("data/d1/d2/folder/").toString(), null, false,
                null, QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("data/d1/d2/d3/folder/").toString(), null, false,
                null, QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("data/d1/d2/d3/d4/folder/").toString(), null, false,
                null, QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("folder/").toString(), null, false, null,
                QueryOptions.empty(), externalSessionId);
    }

    @Test
    public void createFileNoStudyMember() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password).getToken();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(Long.toString(s1), Paths.get("data/my_folder/").toString(), null, false, null,
                QueryOptions.empty(), sessionId);
    }

    /*--------------------------*/
    // Read Samples
    /*--------------------------*/

    @Test
    public void readSampleOwnerUser() throws CatalogException {
        QueryResult<Sample> sample = catalogManager.getSampleManager().get(smp1.getId(), null, ownerSessionId);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSampleManager().get(smp2.getId(), null, ownerSessionId);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSampleManager().get(smp3.getId(), null, ownerSessionId);
        assertEquals(1, sample.getNumResults());

        // Owner always have access even if he has been removed all the permissions
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), ownerUser, aclParams, ownerSessionId).get(0);
        catalogManager.getSampleManager().updateAcl(Long.toString(s1), Arrays.asList(Long.toString(smp1.getId())), ownerUser, noSamplePermissions, ownerSessionId);

        sample = catalogManager.getSampleManager().get(smp1.getId(), null, ownerSessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleExplicitShared() throws CatalogException {
        QueryResult<Sample> sample = catalogManager.getSampleManager().get(smp1.getId(), null, externalSessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleExplicitUnshared() throws CatalogException {
        QueryResult<Sample> sample = catalogManager.getSampleManager().get(smp1.getId(), null, externalSessionId);
        assertEquals(1, sample.getNumResults());
        catalogManager.getAuthorizationManager().removeAcls(Arrays.asList(smp1.getId()), Arrays.asList(externalUser), null,
                MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(smp1.getId(), null, externalSessionId);
    }

    @Test
    public void readSampleNoShared() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(smp2.getId(), null, externalSessionId);
    }

    @Test
    public void readSampleExplicitForbidden() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(smp3.getId(), null, externalSessionId);
    }

    @Test
    public void readSampleExternalUser() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(newUser, password).getToken();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(smp2.getId(), null, sessionId);
    }

    @Test
    public void readSampleAdminUser() throws CatalogException {
        QueryResult<Sample> sample = catalogManager.getSampleManager().get(smp1.getId(), null, studyAdmin1SessionId);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSampleManager().get(smp3.getId(), null, studyAdmin1SessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleForbiddenForExternalUser() throws CatalogException {
        catalogManager.getSampleManager().updateAcl(String.valueOf(s1), Arrays.asList(smp2.getName()), externalUser, new Sample.SampleAclParams("",
                AclParams.Action.SET, null, null, null), ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSampleManager().get(smp2.getId(), null, externalSessionId);
    }

    @Test
    public void readSampleSharedForGroup() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(ownerUser, password).getToken();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(s1, "@external", newUser, ownerSessionId);
        catalogManager.getStudyManager().createGroup(Long.toString(s1), newGroup, newUser, ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, AuthorizationManager.ROLE_LOCKED);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(s1)), newGroup, aclParams, ownerSessionId).get(0);

        // Share the sample with the group
        catalogManager.getSampleManager().updateAcl(Long.toString(s1), Arrays.asList(Long.toString(smp4.getId())), newGroup, allSamplePermissions, ownerSessionId);

        QueryResult<Sample> sample = catalogManager.getSampleManager().get(smp4.getId(), null, sessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleSharedForOthers() throws CatalogException {
        QueryResult<Sample> sample = catalogManager.getSampleManager().get(smp6.getId(), null, externalSessionId);
        assertEquals(1, sample.getNumResults());
    }

    // Read a sample where the user is registered in OpenCGA. However, the user has not been included in the study.
    @Test
    public void readSampleSharedForOthersNotWithStudyPermissions() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.getUserManager().create(newUser, newUser, "asda@mail.com", password, "org", 1000L, Account.Type.FULL, null);
        String sessionId = catalogManager.getUserManager().login(ownerUser, password).getToken();
        catalogManager.getStudyManager().updateGroup(Long.toString(s1), "@members", new GroupParams(newUser, GroupParams.Action.ADD),
                ownerSessionId);

        QueryResult<Sample> sample = catalogManager.getSampleManager().get(smp6.getId(), null, sessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void adminShareSampleWithOtherUser() throws CatalogException {
        catalogManager.getSampleManager().updateAcl(Long.toString(s1), Arrays.asList(Long.toString(smp4.getId())), externalUser, allSamplePermissions, studyAdmin1SessionId);
        QueryResult<Sample> sample = catalogManager.getSampleManager().get(smp4.getId(), null, externalSessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readAllSamplesOwner() throws CatalogException {
        Map<Long, Sample> sampleMap = catalogManager.getSampleManager().get(s1, new Query(), new QueryOptions(), ownerSessionId)
                .getResult().stream().collect(Collectors.toMap(Sample::getId, f -> f));

        assertTrue(sampleMap.containsKey(smp1.getId()));
        assertTrue(sampleMap.containsKey(smp2.getId()));
        assertTrue(sampleMap.containsKey(smp3.getId()));
    }

//    @Test
//    public void readAllSamplesAdmin() throws CatalogException {
//        Map<Long, Sample> sampleMap = catalogManager.getSampleManager().get(s1, new Query(), new QueryOptions(), studyAdmin1SessionId)
//                .getResult().stream().collect(Collectors.toMap(Sample::getId, f -> f));
//
//        assertTrue(sampleMap.containsKey(smp1.getId()));
//        assertFalse(sampleMap.containsKey(smp2.getId()));
//        assertTrue(sampleMap.containsKey(smp3.getId()));
//    }

    @Test
    public void readAllSamplesMember() throws CatalogException {
        Map<Long, Sample> sampleMap = catalogManager.getSampleManager().get(s1, new Query(), new QueryOptions(), externalSessionId)
                .getResult().stream().collect(Collectors.toMap(Sample::getId, f -> f));

        assertTrue(sampleMap.containsKey(smp1.getId()));
        assertFalse(sampleMap.containsKey(smp2.getId()));
        assertFalse(sampleMap.containsKey(smp3.getId()));
    }

    @Test
    public void readCohort() throws CatalogException {
        assertEquals(1, catalogManager.getCohortManager().get(s1, null, null, ownerSessionId).getNumResults());
        assertEquals(0, catalogManager.getCohortManager().get(s1, null, null, externalSessionId).getNumResults());
    }

    /*--------------------------*/
    // Read Individuals
    /*--------------------------*/

    @Test
    public void readIndividualByReadingSomeSample() throws CatalogException {
        catalogManager.getIndividualManager().get(null, String.valueOf((Long) ind1), null, memberSessionId);
    }

    @Test
    public void readIndividualForbidden() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getIndividualManager().get(null, String.valueOf((Long) ind2), null, externalSessionId);
    }

    @Test
    public void readIndividualStudyManager() throws CatalogException {
        catalogManager.getIndividualManager().get(null, String.valueOf((Long) ind2), null, studyAdmin1SessionId);
    }

    @Test
    public void readAllIndividuals() throws CatalogException {
        assertEquals(2, catalogManager.getIndividualManager().get(s1, null, null, ownerSessionId).getNumResults());
        assertEquals(2, catalogManager.getIndividualManager().get(s1, null, null, studyAdmin1SessionId).getNumResults());
        assertEquals(0, catalogManager.getIndividualManager().get(s1, null, null, externalSessionId).getNumResults());
    }



    /*--------------------------*/
    // Read Jobs
    /*--------------------------*/

    @Test
    public void getAllJobs() throws CatalogException {
        long studyId = s1;
        long outDirId = this.data_d1_d2;

        URI tmpJobOutDir = catalogManager.getJobManager().createJobOutDir(studyId, StringUtils.randomString(5), ownerSessionId);
        long job1 = catalogManager.getJobManager().create(studyId, "job1", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello " +
                "World!\"", tmpJobOutDir, outDirId, Collections.emptyList(), Collections.emptyList(), new HashMap<>(), null, new Job
                .JobStatus(Job.JobStatus.ERROR), (long) 0, (long) 0, null, ownerSessionId).first().getId();
        long job2 = catalogManager.getJobManager().create(studyId, "job2", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello " +
                "World!\"", tmpJobOutDir, outDirId, Collections.singletonList(new File().setId(data_d1_d2)), Collections.emptyList(), new
                HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), (long) 0, (long) 0, null, ownerSessionId).first().getId();
        long job3 = catalogManager.getJobManager().create(studyId, "job3", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello " +
                "World!\"", tmpJobOutDir, outDirId, Collections.singletonList(new File().setId(data_d1_d2_d3)), Collections.emptyList(),
                new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), (long) 0, (long) 0, null, ownerSessionId).first().getId();
        long job4 = catalogManager.getJobManager().create(studyId, "job4", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello " +
                "World!\"", tmpJobOutDir, outDirId, Collections.singletonList(new File().setId(data_d1_d2_d3_d4)), Collections.emptyList
                (), new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), (long) 0, (long) 0, null, ownerSessionId).first().getId();

        checkGetAllJobs(studyId, Arrays.asList(job1, job2, job3, job4), ownerSessionId);    //Owner can see everything
        checkGetAllJobs(studyId, Collections.emptyList(), externalSessionId);               //Can't see inside data_d1_d2_d3
    }

    public void checkGetAllJobs(long studyId, Collection<Long> expectedJobs, String sessionId) throws CatalogException {
        QueryResult<Job> allJobs = catalogManager.getJobManager().get(String.valueOf(studyId), (Query) null, null, sessionId);

        assertEquals(expectedJobs.size(), allJobs.getNumResults());
        allJobs.getResult().forEach(job -> assertTrue(expectedJobs + " does not contain job " + job.getName(), expectedJobs.contains(job
                .getId())));
    }

    /////////// Aux methods
    private Map<String, Group> getGroupMap() throws CatalogException {
        return catalogManager.getStudyManager().get(String.valueOf((Long) s1), null, ownerSessionId).first().getGroups().stream().collect(Collectors.toMap(Group::getId, f -> f));
    }

}