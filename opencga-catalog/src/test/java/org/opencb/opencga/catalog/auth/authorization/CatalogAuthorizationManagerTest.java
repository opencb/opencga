package org.opencb.opencga.catalog.auth.authorization;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;

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
    private final String memberUser = "member";
    private final String externalUser = "external";
    private final String password = "1234";

    private final String groupAdmin = "@admins";
    private final String groupMember = "@members";

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

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private CatalogManager catalogManager;
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
    private long smp1;   // Shared with member
    private long smp2;   // Shared with studyAdmin1
    private long smp3;   // Shared with member
    private long smp4;   // Shared with @members
    private long smp6;   // Shared with *
    private long smp5;   // Shared with @members, forbidden for memberUse
    private long ind1;
    private long ind2;

    @Before
    public void before() throws Exception {
        CatalogConfiguration catalogConfiguration = CatalogConfiguration.load(getClass().getResource("/catalog-configuration-test.yml")
                .openStream());

        CatalogManagerExternalResource.clearCatalog(catalogConfiguration);

        catalogManager = new CatalogManager(catalogConfiguration);
        catalogManager.installCatalogDB();

        catalogManager.createUser(ownerUser, ownerUser, "email@ccc.ccc", password, "ASDF", null, null);
        catalogManager.createUser(studyAdminUser1, studyAdminUser1, "email@ccc.ccc", password, "ASDF", null, null);
        catalogManager.createUser(studyAdminUser2, studyAdminUser2, "email@ccc.ccc", password, "ASDF", null, null);
        catalogManager.createUser(memberUser, memberUser, "email@ccc.ccc", password, "ASDF", null, null);
        catalogManager.createUser(externalUser, externalUser, "email@ccc.ccc", password, "ASDF", null, null);

        ownerSessionId = catalogManager.login(ownerUser, password, "localhost").first().get("sessionId").toString();
        studyAdmin1SessionId = catalogManager.login(studyAdminUser1, password, "localhost").first().get("sessionId").toString();
        studyAdmin2SessionId = catalogManager.login(studyAdminUser2, password, "localhost").first().get("sessionId").toString();
        memberSessionId = catalogManager.login(memberUser, password, "localhost").first().get("sessionId").toString();
        externalSessionId = catalogManager.login(externalUser, password, "localhost").first().get("sessionId").toString();

        p1 = catalogManager.createProject("p1", "p1", null, null, null, ownerSessionId).first().getId();
        s1 = catalogManager.createStudy(p1, "s1", "s1", Study.Type.CASE_CONTROL, null, ownerSessionId).first().getId();
        data_d1 = catalogManager.createFolder(s1, Paths.get("data/d1/"), true, null, ownerSessionId).first().getId();
        data_d1_d2 = catalogManager.createFolder(s1, Paths.get("data/d1/d2/"), false, null, ownerSessionId).first().getId();
        data_d1_d2_d3 = catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/"), false, null, ownerSessionId).first().getId();
        data_d1_d2_d3_d4 = catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/d4/"), false, null, ownerSessionId).first().getId();
        data_d1_d2_d3_d4_txt = catalogManager.createFile(s1, File.Format.PLAIN, File.Bioformat.NONE, "data/d1/d2/d3/d4/my.txt", ("file " +
                "content").getBytes(), "", false, ownerSessionId).first().getId();
        data = catalogManager.searchFile(s1, new Query(FileDBAdaptor.QueryParams.PATH.key(), "data/"), ownerSessionId).first().getId();

        // Add studyAdminUser1 and studyAdminUser2 to admin group and admin role.
        catalogManager.createGroup(Long.toString(s1), groupAdmin, studyAdminUser1 + "," + studyAdminUser2, ownerSessionId);
        catalogManager.createStudyAcls(Long.toString(s1), groupAdmin, "", AuthorizationManager.ROLE_ADMIN, ownerSessionId);

        catalogManager.createStudyAcls(Long.toString(s1), memberUser, "", AuthorizationManager.ROLE_ANALYST, studyAdmin1SessionId);
        catalogManager.createStudyAcls(Long.toString(s1), externalUser, "", AuthorizationManager.ROLE_LOCKED, studyAdmin1SessionId);

        catalogManager.createFileAcls(Long.toString(data_d1), externalUser, ALL_FILE_PERMISSIONS, ownerSessionId);
        catalogManager.createFileAcls(Long.toString(data_d1), studyAdminUser1, ALL_FILE_PERMISSIONS, ownerSessionId);
        catalogManager.createFileAcls(Long.toString(data_d1_d2_d3), externalUser, DENY_FILE_PERMISSIONS, ownerSessionId);
        catalogManager.createFileAcls(Long.toString(data_d1_d2_d3_d4_txt), externalUser, ALL_FILE_PERMISSIONS, ownerSessionId);
//        catalogManager.createFileAcls(Long.toString(data_d1), memberUser, new AclEntry(memberUser, true, true, true, true), ownerSessionId);
//        catalogManager.createFileAcls(Long.toString(data_d1), "@" + groupAdmin, new AclEntry("@" + groupAdmin, false, false, false, false), ownerSessionId);
//        catalogManager.createFileAcls(Long.toString(data_d1), studyAdminUser1, new AclEntry(studyAdminUser1, true, true, true, true), ownerSessionId);
//        catalogManager.createFileAcls(Long.toString(data_d1_d2), "@" + groupAdmin, new AclEntry("@" + groupAdmin, false, false, false, false), ownerSessionId);
//        catalogManager.createFileAcls(Long.toString(data_d1_d2_d3), memberUser, new AclEntry(memberUser, false, false, false, false), ownerSessionId);
//        catalogManager.createFileAcls(Long.toString(data_d1_d2_d3_d4), "@" + groupAdmin, new AclEntry("@" + groupAdmin, true, true, true, true), ownerSessionId);
//        catalogManager.createFileAcls(Long.toString(data_d1_d2_d3_d4_txt), memberUser, new AclEntry(memberUser, true, true, true, true), ownerSessionId);

        smp1 = catalogManager.createSample(s1, "smp1", null, null, null, null, ownerSessionId).first().getId();
        smp2 = catalogManager.createSample(s1, "smp2", null, null, null, null, ownerSessionId).first().getId();
        smp3 = catalogManager.createSample(s1, "smp3", null, null, null, null, ownerSessionId).first().getId();
        smp4 = catalogManager.createSample(s1, "smp4", null, null, null, null, ownerSessionId).first().getId();
        smp5 = catalogManager.createSample(s1, "smp5", null, null, null, null, ownerSessionId).first().getId();
        smp6 = catalogManager.createSample(s1, "smp6", null, null, null, null, ownerSessionId).first().getId();
        catalogManager.createCohort(s1, "all", Study.Type.COLLECTION, "", Arrays.asList(smp1, smp2, smp3), null, ownerSessionId).first()
                .getId();
        ind1 = catalogManager.createIndividual(s1, "ind1", "", 0, 0, Individual.Sex.UNKNOWN, null, ownerSessionId).first().getId();
        ind2 = catalogManager.createIndividual(s1, "ind2", "", 0, 0, Individual.Sex.UNKNOWN, null, ownerSessionId).first().getId();
        catalogManager.modifySample(smp1, new QueryOptions("individualId", ind1), ownerSessionId);
        catalogManager.modifySample(smp2, new QueryOptions("individualId", ind1), ownerSessionId);
        catalogManager.modifySample(smp2, new QueryOptions("individualId", ind2), ownerSessionId);

        catalogManager.createSampleAcls(Long.toString(smp1), externalUser, ALL_SAMPLE_PERMISSIONS, ownerSessionId);
        catalogManager.createSampleAcls(Long.toString(smp3), externalUser, DENY_SAMPLE_PERMISSIONS, ownerSessionId);
        catalogManager.createSampleAcls(Long.toString(smp2), studyAdminUser1, DENY_SAMPLE_PERMISSIONS, ownerSessionId);
        catalogManager.createSampleAcls(Long.toString(smp5), externalUser, DENY_SAMPLE_PERMISSIONS, ownerSessionId);
        catalogManager.createSampleAcls(Long.toString(smp6), "*", ALL_SAMPLE_PERMISSIONS, ownerSessionId);
//        catalogManager.createSampleAcls(Long.toString(smp1), memberUser, new AclEntry(memberUser, true, true, true, true), ownerSessionId);
//        catalogManager.createSampleAcls(Long.toString(smp3), memberUser, new AclEntry(memberUser, false, false, false, false),
//                ownerSessionId);
//        catalogManager.createSampleAcls(Long.toString(smp2), studyAdminUser1, new AclEntry(studyAdminUser1, false, false, false, false), ownerSessionId);
//        catalogManager.createSampleAcls(Long.toString(smp4), "@" + groupMember,
//                new AclEntry("@" + groupMember, true, true, true, true), ownerSessionId);
//        catalogManager.createSampleAcls(Long.toString(smp5), "@" + groupMember,
//                new AclEntry("@" + groupMember, true, true, true, true), ownerSessionId);
//        catalogManager.createSampleAcls(Long.toString(smp5), memberUser, new AclEntry(memberUser, false, false, false, false),
//                ownerSessionId);
//        catalogManager.createSampleAcls(Long.toString(smp6), AclEntry.USER_OTHERS_ID,
//                new AclEntry(AclEntry.USER_OTHERS_ID, true, true, true, true), ownerSessionId);

    }

    @After
    public void after() throws Exception {
        catalogManager.close();
    }

    /*--------------------------*/
    // Add group members
    /*--------------------------*/

    @Test
    public void addMemberToGroup() throws CatalogException {
        catalogManager.updateGroup(Long.toString(s1), groupAdmin, externalUser, null, null, ownerSessionId);
        Map<String, Group> groups = getGroupMap();
        assertTrue(groups.get(groupAdmin).getUserIds().contains(externalUser));
    }

    @Test
    public void addMemberToGroupExistingNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.updateGroup(Long.toString(s1), groupAdmin, externalUser, null, null, externalSessionId);
    }

    @Test
    public void changeGroupMembership() throws CatalogException {
        catalogManager.updateGroup(Long.toString(s1), groupAdmin, externalUser, null, null, ownerSessionId);
//        catalogManager.addUsersToGroup(s1, groupAdmin, externalUser, ownerSessionId);
        Map<String, Group> groups = getGroupMap();
        assertTrue(groups.get(groupAdmin).getUserIds().contains(externalUser));
//        thrown.expect(CatalogException.class);
//        catalogManager.addUsersToGroup(s1, groupMember, externalUser, ownerSessionId);
        catalogManager.updateGroup(Long.toString(s1), groupAdmin, null, externalUser, null, ownerSessionId);
        catalogManager.createGroup(Long.toString(s1), groupMember, externalUser, ownerSessionId);
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
        // Remove one of the users
        catalogManager.updateGroup(Long.toString(s1), groupAdmin, null, studyAdminUser1, null, ownerSessionId);
        assertFalse(getGroupMap().get(groupAdmin).getUserIds().contains(studyAdminUser1));

        // Remove the last user in the admin group
        catalogManager.updateGroup(Long.toString(s1), groupAdmin, null, studyAdminUser2, null, ownerSessionId);
        assertFalse(getGroupMap().get(groupAdmin).getUserIds().contains(studyAdminUser2));

//        // Cannot remove group with defined ACLs
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("ACL defined");
        catalogManager.deleteGroup(Long.toString(s1), groupAdmin, ownerSessionId);
        assertNull(getGroupMap().get(groupAdmin));

    }

    @Test
    public void removeMemberFromGroupNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.updateGroup(Long.toString(s1), groupAdmin, null, ownerUser, null, externalSessionId);
//        catalogManager.removeUsersFromGroup(s1, groupAdmin, ownerUser, externalSessionId);
    }

    @Test
    public void removeMemberFromNonExistingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
//        catalogManager.removeUsersFromGroup(s1, "NO_GROUP", ownerUser, ownerSessionId);
        catalogManager.updateGroup(Long.toString(s1), "NO_GROUP", null, ownerUser, null, ownerSessionId);
    }

    @Test
    public void removeMemberFromNonBelongingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
//        catalogManager.removeUsersFromGroup(s1, groupMember, ownerUser, ownerSessionId);
        catalogManager.updateGroup(Long.toString(s1), groupMember, null, ownerUser, null, ownerSessionId);
    }

    /*--------------------------*/
    // Add users/groups to roles
    /*--------------------------*/

    // A user with proper permissions adds an existing user to a role
    @Test
    public void addExistingUserToRole() throws CatalogException {
        String newUser = "newUser";
        catalogManager.createUser(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, null);
        catalogManager.createStudyAcls(Long.toString(s1), newUser, "", AuthorizationManager.ROLE_ANALYST, ownerSessionId);
    }

    // A user with no permissions tries to add an existing user to a role
    @Test
    public void addExistingUserToRole2() throws CatalogException {
        String newUser = "newUser";
        catalogManager.createUser(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, null);
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("Permission denied");
        catalogManager.createStudyAcls(Long.toString(s1), newUser, "", AuthorizationManager.ROLE_ANALYST, memberSessionId);
    }

    @Test
    public void addExistingGroupToRole() throws CatalogException {
        String newUser = "newUser";
        catalogManager.createUser(newUser, newUser, "email@ccc.ccc", password, "ASDF", null, null);
        String group = "@newGroup";
//        catalogManager.addUsersToGroup(s1, group, newUser, studyAdmin1SessionId);
        catalogManager.createGroup(Long.toString(s1), group, newUser, studyAdmin1SessionId);
        catalogManager.createStudyAcls(Long.toString(s1), group, "", AuthorizationManager.ROLE_ANALYST, studyAdmin1SessionId);
        QueryResult<StudyAclEntry> studyAcls = catalogManager.getStudyAcls(Long.toString(s1), Arrays.asList(group), studyAdmin1SessionId);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(group, studyAcls.first().getMember());
        assertArrayEquals(AuthorizationManager.getAnalystAcls().toArray(), studyAcls.first().getPermissions().toArray());
    }

    @Test
    public void addNonExistingUserToRole() throws CatalogException {
        String userNotRegistered = "userNotRegistered";
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("does not exist");
        catalogManager.createStudyAcls(Long.toString(s1), userNotRegistered, "", AuthorizationManager.ROLE_ANALYST, studyAdmin1SessionId);
    }

    @Test
    public void addNonExistingGroupToRole() throws CatalogException {
        String groupNotRegistered = "@groupNotRegistered";
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("does not exist");
        catalogManager.createStudyAcls(Long.toString(s1), groupNotRegistered, "", AuthorizationManager.ROLE_ANALYST, studyAdmin1SessionId);
    }

    @Test
    public void changeUserRole() throws CatalogException {
        QueryResult<StudyAclEntry> studyAcls = catalogManager.getStudyAcls(Long.toString(s1), Arrays.asList(externalUser), studyAdmin1SessionId);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(externalUser, studyAcls.first().getMember());

        // Change role
        catalogManager.removeStudyAcl(Long.toString(s1), externalUser, studyAdmin1SessionId);
        catalogManager.createStudyAcls(Long.toString(s1), externalUser, "", AuthorizationManager.ROLE_ANALYST, studyAdmin1SessionId);
        studyAcls = catalogManager.getStudyAcl(Long.toString(s1), externalUser, studyAdmin1SessionId);
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
        catalogManager.removeStudyAcl(Long.toString(s1), externalUser, studyAdmin1SessionId);
        QueryResult<StudyAclEntry> studyAcls = catalogManager.getStudyAcl(Long.toString(s1), externalUser, studyAdmin1SessionId);
        assertEquals(0, studyAcls.getNumResults());
    }

    // A user with no permissions tries to remove an existing user from a role
    @Test
    public void removeUserFromRole2() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
//        catalogManager.unshareStudy(s1, externalUser, memberSessionId);
        catalogManager.removeStudyAcl(Long.toString(s1), externalUser, memberSessionId);
    }

    @Test
    public void removeGroupFromRole() throws CatalogException {
        QueryResult<StudyAclEntry> studyAcls = catalogManager.getStudyAcl(Long.toString(s1), groupAdmin, studyAdmin1SessionId);
        assertEquals(1, studyAcls.getNumResults());
        assertEquals(groupAdmin, studyAcls.first().getMember());
        assertArrayEquals(AuthorizationManager.getAdminAcls().toArray(), studyAcls.first().getPermissions().toArray());

//        catalogManager.unshareStudy(s1, groupAdmin, studyAdmin1SessionId);
        catalogManager.removeStudyAcl(Long.toString(s1), groupAdmin, studyAdmin1SessionId);
        studyAcls = catalogManager.getStudyAcl(Long.toString(s1), groupAdmin, ownerSessionId);
        assertEquals(0, studyAcls.getNumResults());
    }

    @Test
    public void removeOwnerFromRoleAdmin() throws CatalogException {
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not allowed");
//        catalogManager.unshareStudy(s1, ownerUser, ownerSessionId);
        catalogManager.removeStudyAcl(Long.toString(s1), ownerUser, ownerSessionId);
    }

    @Test
    public void removeNonExistingUserFromRole() throws CatalogException {
        String userNotRegistered = "userNotRegistered";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("did not have any ACLs defined");
//        catalogManager.unshareStudy(s1, userNotRegistered, ownerSessionId);
        catalogManager.removeStudyAcl(Long.toString(s1), userNotRegistered, ownerSessionId);
    }

    @Test
    public void removeNonExistingGroupFromRole() throws CatalogException {
        String groupNotRegistered = "@groupNotRegistered";
        thrown.expect(CatalogException.class);
        thrown.expectMessage("did not have any ACLs defined");
//        catalogManager.unshareStudy(s1, groupNotRegistered, ownerSessionId);
        catalogManager.removeStudyAcl(Long.toString(s1), groupNotRegistered, ownerSessionId);
    }

    /*--------------------------*/
    // Read Project
    /*--------------------------*/

    @Test
    public void readProject() throws CatalogException {
        QueryResult<Project> project = catalogManager.getProject(p1, null, ownerSessionId);
        assertEquals(1, project.getNumResults());
        project = catalogManager.getProject(p1, null, memberSessionId);
        assertEquals(1, project.getNumResults());
    }

    @Test
    public void readProjectDeny() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getProject(p1, null, externalSessionId);
    }

    /*--------------------------*/
    // Read Study
    /*--------------------------*/

    @Test
    public void readStudy() throws CatalogException {
        QueryResult<Study> study = catalogManager.getStudy(s1, null, ownerSessionId);
        assertEquals(1, study.getNumResults());
        study = catalogManager.getStudy(s1, null, memberSessionId);
        assertEquals(1, study.getNumResults());
    }

    @Test
    public void readStudyDeny() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudy(s1, externalSessionId);
    }

    /*--------------------------*/
    // Read file
    /*--------------------------*/

    @Test
    public void readFileByOwner() throws CatalogException {
        QueryResult<File> file = catalogManager.getFile(data_d1, ownerSessionId);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readExplicitlySharedFolder() throws CatalogException {
        catalogManager.getFile(data_d1, externalSessionId);
    }

    @Test
    public void readExplicitlyUnsharedFile() throws CatalogException {
        QueryResult<File> file = catalogManager.getFile(data_d1, memberSessionId);
        assertEquals(1, file.getNumResults());
        // Set an ACL with no permissions
        catalogManager.createFileAcls(Long.toString(data_d1), memberUser, "", ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1, memberSessionId);
    }

    @Test
    public void readInheritedSharedFile() throws CatalogException {
        QueryResult<File> file = catalogManager.getFile(data_d1_d2, externalSessionId);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1_d2_d3, externalSessionId);
    }

    @Test
    public void readInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1_d2_d3_d4, externalSessionId);
    }

    @Test
    public void readExplicitlySharedFile() throws CatalogException {
        QueryResult<File> file = catalogManager.getFile(data_d1_d2_d3_d4_txt, externalSessionId);
        assertEquals(1, file.getNumResults());
    }

    @Test
    public void readNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data, externalSessionId);
    }

    @Test
    public void readFileNoStudyMember() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.createUser(newUser, newUser, "asda@mail.com", password, "org", 1000L, null);
        String sessionId = catalogManager.login(newUser, password, "localhost").first().get("sessionId").toString();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data, sessionId);
    }

    @Test
    public void readFileSharedForGroup() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.createUser(newUser, newUser, "asda@mail.com", password, "org", 1000L, null);
        String sessionId = catalogManager.login(newUser, password, "localhost").first().get("sessionId").toString();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(s1, "@external", newUser, ownerSessionId);
        catalogManager.createGroup(Long.toString(s1), newGroup, newUser, ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        catalogManager.createStudyAcls(Long.toString(s1), newGroup, "", AuthorizationManager.ROLE_LOCKED, ownerSessionId);
        // Specify all file permissions for that concrete file
        catalogManager.createFileAcls(Long.toString(data_d1_d2_d3_d4), newGroup, ALL_FILE_PERMISSIONS, ownerSessionId);
        catalogManager.getFile(data_d1_d2_d3_d4, sessionId);
    }

    @Test
    public void readFileForbiddenForGroup() throws CatalogException {
        // Remove all permissions to the admin group in that folder
        catalogManager.createFileAcls(Long.toString(data_d1_d2), groupAdmin, DENY_FILE_PERMISSIONS, ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1_d2, studyAdmin1SessionId);
    }

    @Test
    public void readFileForbiddenForGroupSharedWithUser() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.createUser(newUser, newUser, "asda@mail.com", password, "org", 1000L, null);
        String sessionId = catalogManager.login(newUser, password, "localhost").first().get("sessionId").toString();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(s1, "@external", newUser, ownerSessionId);
        catalogManager.createGroup(Long.toString(s1), newGroup, newUser, ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        catalogManager.createStudyAcls(Long.toString(s1), newGroup, "", AuthorizationManager.ROLE_LOCKED, ownerSessionId);
        catalogManager.createFileAcls(Long.toString(data_d1_d2), newUser, ALL_FILE_PERMISSIONS, ownerSessionId);
        QueryResult<File> file = catalogManager.getFile(data_d1_d2, sessionId);
        assertEquals(1, file.getNumResults());
    }

    /*--------------------------*/
    // Create file
    /*--------------------------*/

    @Test
    public void createFileByOwnerNoGroups() throws CatalogException {
        QueryResult<File> folder = catalogManager.createFolder(s1, Paths.get("data/newFolder"), true, null, ownerSessionId);
        assertEquals(1, folder.getNumResults());
    }

    @Test
    public void createExplicitlySharedFile() throws CatalogException {
        catalogManager.createFolder(s1, Paths.get("data/d1/folder/"), false, null, externalSessionId);
    }

    @Test
    public void createInheritedSharedFile() throws CatalogException {
        catalogManager.createFolder(s1, Paths.get("data/d1/d2/folder/"), false, null, externalSessionId);
    }

    @Test
    public void createExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/folder/"), false, null, externalSessionId);
    }

    @Test
    public void createInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/d4/folder/"), false, null, externalSessionId);
    }

    @Test
    public void createNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("folder/"), false, null, externalSessionId);
    }

    @Test
    public void createFileNoStudyMember() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.createUser(newUser, newUser, "asda@mail.com", password, "org", 1000L, null);
        String sessionId = catalogManager.login(newUser, password, "localhost").first().get("sessionId").toString();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("data/my_folder/"), false, null, sessionId);
    }

    /*--------------------------*/
    // Read Samples
    /*--------------------------*/

    @Test
    public void readSampleOwnerUser() throws CatalogException {
        QueryResult<Sample> sample = catalogManager.getSample(smp1, null, ownerSessionId);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSample(smp2, null, ownerSessionId);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSample(smp3, null, ownerSessionId);
        assertEquals(1, sample.getNumResults());

        //Owner always have access
        catalogManager.createSampleAcls(Long.toString(smp1), ownerUser, DENY_SAMPLE_PERMISSIONS, ownerSessionId);
        sample = catalogManager.getSample(smp1, null, ownerSessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleExplicitShared() throws CatalogException {
        QueryResult<Sample> sample = catalogManager.getSample(smp1, null, externalSessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleExplicitUnshared() throws CatalogException {
        QueryResult<Sample> sample = catalogManager.getSample(smp1, null, externalSessionId);
        assertEquals(1, sample.getNumResults());
//        catalogManager.unshareSample(Long.toString(smp1), externalUser, null, ownerSessionId);
        catalogManager.removeSampleAcl(Long.toString(smp1), externalUser, ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp1, null, externalSessionId);
    }

    @Test
    public void readSampleNoShared() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp2, null, externalSessionId);
    }

    @Test
    public void readSampleExplicitForbidden() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp3, null, externalSessionId);
    }

    @Test
    public void readSampleExternalUser() throws CatalogException, IOException {
        String newUser = "newUser";
        catalogManager.createUser(newUser, newUser, "asda@mail.com", password, "org", 1000L, null);
        String sessionId = catalogManager.login(newUser, password, "localhost").first().get("sessionId").toString();
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp2, null, sessionId);
    }

    @Test
    public void readSampleAdminUser() throws CatalogException {
        QueryResult<Sample> sample = catalogManager.getSample(smp1, null, studyAdmin1SessionId);
        assertEquals(1, sample.getNumResults());
        sample = catalogManager.getSample(smp3, null, studyAdmin1SessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleForbiddenForAdminUser() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp2, null, studyAdmin1SessionId);
    }

    @Test
    public void readSampleSharedForGroup() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.createUser(newUser, newUser, "asda@mail.com", password, "org", 1000L, null);
        String sessionId = catalogManager.login(newUser, password, "localhost").first().get("sessionId").toString();
        String newGroup = "@external";
//        catalogManager.addUsersToGroup(s1, "@external", newUser, ownerSessionId);
        catalogManager.createGroup(Long.toString(s1), newGroup, newUser, ownerSessionId);
        // Add the group to the locked role, so no permissions will be given
        catalogManager.createStudyAcls(Long.toString(s1), newGroup, "", AuthorizationManager.ROLE_LOCKED, ownerSessionId);

        // Share the sample with the group
        catalogManager.createSampleAcls(Long.toString(smp4), newGroup, ALL_SAMPLE_PERMISSIONS, ownerSessionId);

        QueryResult<Sample> sample = catalogManager.getSample(smp4, null, sessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readSampleSharedForOthers() throws CatalogException {
        QueryResult<Sample> sample = catalogManager.getSample(smp6, null, externalSessionId);
        assertEquals(1, sample.getNumResults());
    }

    // Read a sample where the user is registered in OpenCGA. However, the user has not been included in the study.
    @Test
    public void readSampleSharedForOthersNotWithStudyPermissions() throws CatalogException, IOException {
        // Add a new user to a new group
        String newUser = "newUser";
        catalogManager.createUser(newUser, newUser, "asda@mail.com", password, "org", 1000L, null);
        String sessionId = catalogManager.login(newUser, password, "localhost").first().get("sessionId").toString();

        QueryResult<Sample> sample = catalogManager.getSample(smp6, null, sessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void adminShareSampleWithOtherUser() throws CatalogException {
        catalogManager.createSampleAcls(Long.toString(smp4), externalUser, ALL_SAMPLE_PERMISSIONS, studyAdmin1SessionId);
        QueryResult<Sample> sample = catalogManager.getSample(smp4, null, externalSessionId);
        assertEquals(1, sample.getNumResults());
    }

    @Test
    public void readAllSamplesOwner() throws CatalogException {
        Map<Long, Sample> sampleMap = catalogManager.getAllSamples(s1, new Query(), new QueryOptions(), ownerSessionId)
                .getResult().stream().collect(Collectors.toMap(Sample::getId, f -> f));

        assertTrue(sampleMap.containsKey(smp1));
        assertTrue(sampleMap.containsKey(smp2));
        assertTrue(sampleMap.containsKey(smp3));
    }

    @Test
    public void readAllSamplesAdmin() throws CatalogException {
        Map<Long, Sample> sampleMap = catalogManager.getAllSamples(s1, new Query(), new QueryOptions(), studyAdmin1SessionId)
                .getResult().stream().collect(Collectors.toMap(Sample::getId, f -> f));

        assertTrue(sampleMap.containsKey(smp1));
        assertFalse(sampleMap.containsKey(smp2));
        assertTrue(sampleMap.containsKey(smp3));
    }

    @Test
    public void readAllSamplesMember() throws CatalogException {
        Map<Long, Sample> sampleMap = catalogManager.getAllSamples(s1, new Query(), new QueryOptions(), externalSessionId)
                .getResult().stream().collect(Collectors.toMap(Sample::getId, f -> f));

        assertTrue(sampleMap.containsKey(smp1));
        assertFalse(sampleMap.containsKey(smp2));
        assertFalse(sampleMap.containsKey(smp3));
    }

    @Test
    public void readCohort() throws CatalogException {
        assertEquals(1, catalogManager.getAllCohorts(s1, null, null, ownerSessionId).getNumResults());
        assertEquals(0, catalogManager.getAllCohorts(s1, null, null, externalSessionId).getNumResults());
    }

    /*--------------------------*/
    // Read Individuals
    /*--------------------------*/

    @Test
    public void readIndividualByReadingSomeSample() throws CatalogException {
        catalogManager.getIndividual(ind1, null, memberSessionId);
    }

    @Test
    public void readIndividualForbidden() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getIndividual(ind2, null, externalSessionId);
    }

    @Test
    public void readIndividualStudyManager() throws CatalogException {
        catalogManager.getIndividual(ind2, null, studyAdmin1SessionId);
    }

    @Test
    public void readAllIndividuals() throws CatalogException {
        assertEquals(2, catalogManager.getAllIndividuals(s1, null, null, ownerSessionId).getNumResults());
        assertEquals(2, catalogManager.getAllIndividuals(s1, null, null, studyAdmin1SessionId).getNumResults());
        assertEquals(0, catalogManager.getAllIndividuals(s1, null, null, externalSessionId).getNumResults());
    }



    /*--------------------------*/
    // Read Jobs
    /*--------------------------*/

    @Test
    public void getAllJobs() throws CatalogException {
        long studyId = s1;
        long outDirId = this.data_d1_d2;

        URI tmpJobOutDir = catalogManager.createJobOutDir(studyId, StringUtils.randomString(5), ownerSessionId);
        long job1 = catalogManager.createJob(studyId, "job1", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello World!\"",
                tmpJobOutDir, outDirId, Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), 0, 0, null, ownerSessionId).first().getId();
        long job2 = catalogManager.createJob(studyId, "job2", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello World!\"",
                tmpJobOutDir, outDirId, Collections.singletonList(data_d1_d2), Collections.emptyList(),
                new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), 0, 0, null, ownerSessionId).first().getId();
        long job3 = catalogManager.createJob(studyId, "job3", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello World!\"",
                tmpJobOutDir, outDirId, Collections.singletonList(data_d1_d2_d3), Collections.emptyList(),
                new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), 0, 0, null, ownerSessionId).first().getId();
        long job4 = catalogManager.createJob(studyId, "job4", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello World!\"",
                tmpJobOutDir, outDirId, Collections.singletonList(data_d1_d2_d3_d4), Collections.emptyList(),
                new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), 0, 0, null, ownerSessionId).first().getId();

        checkGetAllJobs(studyId, Arrays.asList(job1, job2, job3, job4), ownerSessionId);    //Owner can see everything
        checkGetAllJobs(studyId, Collections.emptyList(), externalSessionId);               //Can't see inside data_d1_d2_d3
    }

    public void checkGetAllJobs(long studyId, Collection<Long> expectedJobs, String sessionId) throws CatalogException {
        QueryResult<Job> allJobs = catalogManager.getAllJobs(studyId, sessionId);

        assertEquals(expectedJobs.size(), allJobs.getNumResults());
        allJobs.getResult().forEach(job -> assertTrue(expectedJobs + " does not contain job " + job.getName(), expectedJobs.contains(job
                .getId())));
    }

    /////////// Aux methods
    private Map<String, Group> getGroupMap() throws CatalogException {
        return catalogManager.getStudy(s1, ownerSessionId).first().getGroups().stream().collect(Collectors.toMap(Group::getName, f -> f));
    }

}