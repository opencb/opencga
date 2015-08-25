package org.opencb.opencga.catalog.authorization;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created on 19/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthorizationManagerTest {

    private final String ownerUser = "owner";
    private final String studyAdminUser1 = "studyAdmin1";
    private final String studyAdminUser2 = "studyAdmin2";
    private final String memberUser = "member";
    private final String externalUser = "external";
    private final String password = "1234";
    private CatalogManager catalogManager;
    private String ownerSessionId;
    private String studyAdmin1SessionId;
    private String studyAdmin2SessionId;
    private String memberSessionId;
    private String externalSessionId;
    private int p1;
    private int s1;

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private int data;                 //
    private int data_d1;              // Shared with member, Forbidden for @admins, Shared with studyAdmin1
    private int data_d1_d2;           // Forbidden for @admins
    private int data_d1_d2_d3;        // Forbidden for member
    private int data_d1_d2_d3_d4;     // Shared for @admins
    private int smp1;   // Shared with member
    private int smp2;   // Shared with studyAdmin1
    private int smp3;   // Shared with member
    private int smp4;   // Shared with @members
    private int smp6;   // Shared with *
    private int smp5;   // Shared with @members, forbidden for memberUse

    @Before
    public void before () throws Exception {
        InputStream is = CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);

        CatalogManagerTest.clearCatalog(properties);

        catalogManager = new CatalogManager(properties);

        catalogManager.createUser(ownerUser, ownerUser, "email@ccc.ccc", password, "ASDF", null);
        catalogManager.createUser(studyAdminUser1, studyAdminUser1, "email@ccc.ccc", password, "ASDF", null);
        catalogManager.createUser(studyAdminUser2, studyAdminUser2, "email@ccc.ccc", password, "ASDF", null);
        catalogManager.createUser(memberUser, memberUser, "email@ccc.ccc", password, "ASDF", null);
        catalogManager.createUser(externalUser, externalUser, "email@ccc.ccc", password, "ASDF", null);

        ownerSessionId = catalogManager.login(ownerUser, password, "localhost").first().get("sessionId").toString();
        studyAdmin1SessionId = catalogManager.login(studyAdminUser1, password, "localhost").first().get("sessionId").toString();
        studyAdmin2SessionId = catalogManager.login(studyAdminUser2, password, "localhost").first().get("sessionId").toString();
        memberSessionId = catalogManager.login(memberUser, password, "localhost").first().get("sessionId").toString();
        externalSessionId = catalogManager.login(externalUser, password, "localhost").first().get("sessionId").toString();

        p1 = catalogManager.createProject(ownerUser, "p1", "p1", null, null, null, ownerSessionId).first().getId();
        s1 = catalogManager.createStudy(p1, "s1", "s1", Study.Type.CASE_CONTROL, null, ownerSessionId).first().getId();
        data = catalogManager.searchFile(s1, new QueryOptions(CatalogFileDBAdaptor.FileFilterOption.path.toString(), "data/"), ownerSessionId).first().getId();
        data_d1 = catalogManager.createFolder(s1, Paths.get("data/d1/"), false, null, ownerSessionId).first().getId();
        data_d1_d2 = catalogManager.createFolder(s1, Paths.get("data/d1/d2/"), false, null, ownerSessionId).first().getId();
        data_d1_d2_d3 = catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/"), false, null, ownerSessionId).first().getId();
        data_d1_d2_d3_d4 = catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/d4/"), false, null, ownerSessionId).first().getId();

        catalogManager.shareProject(p1, new AclEntry(memberUser, true, true, true, true), ownerSessionId);
        catalogManager.addMemberToGroup(s1, AuthorizationManager.MEMBERS_GROUP, memberUser, ownerSessionId);
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, studyAdminUser1, ownerSessionId);

        catalogManager.shareFile(data_d1, new AclEntry(memberUser, true, true, true, true), ownerSessionId);
        catalogManager.shareFile(data_d1, new AclEntry("@" + AuthorizationManager.ADMINS_GROUP, false, false, false, false), ownerSessionId);
        catalogManager.shareFile(data_d1, new AclEntry(studyAdminUser1, true, true, true, true), ownerSessionId);
        catalogManager.shareFile(data_d1_d2, new AclEntry("@" + AuthorizationManager.ADMINS_GROUP, false, false, false, false), ownerSessionId);
        catalogManager.shareFile(data_d1_d2_d3, new AclEntry(memberUser, false, false, false, false), ownerSessionId);
        catalogManager.shareFile(data_d1_d2_d3_d4, new AclEntry("@" + AuthorizationManager.ADMINS_GROUP, true, true, true, true), ownerSessionId);

        smp1 = catalogManager.createSample(s1, "smp1", null, null, null, null, ownerSessionId).first().getId();
        smp2 = catalogManager.createSample(s1, "smp2", null, null, null, null, ownerSessionId).first().getId();
        smp3 = catalogManager.createSample(s1, "smp3", null, null, null, null, ownerSessionId).first().getId();
        smp4 = catalogManager.createSample(s1, "smp4", null, null, null, null, ownerSessionId).first().getId();
        smp5 = catalogManager.createSample(s1, "smp5", null, null, null, null, ownerSessionId).first().getId();
        smp6 = catalogManager.createSample(s1, "smp6", null, null, null, null, ownerSessionId).first().getId();

        catalogManager.shareSample(smp1, new AclEntry(memberUser, true, true, true, true), ownerSessionId);
        catalogManager.shareSample(smp3, new AclEntry(memberUser, false, false, false, false), ownerSessionId);
        catalogManager.shareSample(smp2, new AclEntry(studyAdminUser1, false, false, false, false), ownerSessionId);
        catalogManager.shareSample(smp4, new AclEntry("@" + AuthorizationManager.MEMBERS_GROUP, true, true, true, true), ownerSessionId);
        catalogManager.shareSample(smp5, new AclEntry("@" + AuthorizationManager.MEMBERS_GROUP, true, true, true, true), ownerSessionId);
        catalogManager.shareSample(smp5, new AclEntry(memberUser, false, false, false, false), ownerSessionId);
        catalogManager.shareSample(smp6, new AclEntry(AclEntry.USER_OTHERS_ID, true, true, true, true), ownerSessionId);

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
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, externalUser, ownerSessionId);
        Map<String, Group> groups = getGroupMap();
        assertTrue(groups.get(AuthorizationManager.ADMINS_GROUP).getUserIds().contains(externalUser));
    }

    @Test
    public void addMemberToGroupExistingNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, externalUser, externalSessionId);
    }

    @Test
    public void addMemberToGroupInOtherGroup() throws CatalogException {
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, externalUser, ownerSessionId);
        thrown.expect(CatalogException.class);
        catalogManager.addMemberToGroup(s1, AuthorizationManager.MEMBERS_GROUP, externalUser, ownerSessionId);
    }

    @Test
    public void addMemberToTheBelongingGroup() throws CatalogException {
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, externalUser, ownerSessionId);
        thrown.expect(CatalogException.class);
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, externalUser, ownerSessionId);
    }

    @Test
    public void addMemberToNonExistingGroup() throws CatalogException {
        thrown.expect(CatalogDBException.class);
        catalogManager.addMemberToGroup(s1, "NO_GROUP", externalUser, ownerSessionId);
    }

    /*--------------------------*/
    // Remove group members
    /*--------------------------*/

    @Test
    public void removeMemberFromGroup() throws CatalogException {
        catalogManager.removeMemberFromGroup(s1, AuthorizationManager.ADMINS_GROUP, ownerUser, ownerSessionId);
        assertFalse(getGroupMap().get(AuthorizationManager.ADMINS_GROUP).getUserIds().contains(ownerUser));
    }

    @Test
    public void removeMemberFromGroupNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.removeMemberFromGroup(s1, AuthorizationManager.ADMINS_GROUP, ownerUser, externalSessionId);
    }

    @Test
    public void removeMemberFromNonExistingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
        catalogManager.removeMemberFromGroup(s1, "NO_GROUP", ownerUser, ownerSessionId);
    }

    @Test
    public void removeMemberFromNonBelongingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
        catalogManager.removeMemberFromGroup(s1, AuthorizationManager.MEMBERS_GROUP, ownerUser, ownerSessionId);
    }

    /*--------------------------*/
    // Read Project
    /*--------------------------*/

    @Test
    public void readProject() throws CatalogException {
        catalogManager.getProject(p1, null, memberSessionId);
    }

    @Test
    public void readProjectDenny() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getProject(p1, null, externalSessionId);
    }

    /*--------------------------*/
    // Read Study
    /*--------------------------*/

    @Test
    public void readStudy() throws CatalogException {
        catalogManager.getStudy(s1, memberSessionId);
    }

    @Test
    public void readStudyDenny() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudy(s1, externalSessionId);
    }

    /*--------------------------*/
    // Read file
    /*--------------------------*/

    @Test
    public void readFileByOwner() throws CatalogException {
        catalogManager.getFile(data_d1, ownerSessionId);
    }

    @Test
    public void readFileByOwnerNoGroups() throws CatalogException {
        catalogManager.removeMemberFromGroup(s1, AuthorizationManager.ADMINS_GROUP, ownerUser, ownerSessionId);
        catalogManager.getFile(data_d1, ownerSessionId);
    }

    @Test
    public void readExplicitlySharedFile() throws CatalogException {
        catalogManager.getFile(data_d1, memberSessionId);
    }

    @Test
    public void readExplicitlyUnsharedFile() throws CatalogException {
        catalogManager.getFile(data_d1, memberSessionId);
        catalogManager.unshareFile(data_d1, memberUser, ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1, memberSessionId);
    }

    @Test
    public void readInheritedSharedFile() throws CatalogException {
        catalogManager.getFile(data_d1_d2, memberSessionId);
    }

    @Test
    public void readExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1_d2_d3, memberSessionId);
    }

    @Test
    public void readInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1_d2_d3_d4, memberSessionId);
    }

    @Test
    public void readNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data, memberSessionId);
    }

    @Test
    public void readFileNoStudyMember() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data, externalSessionId);
    }

    @Test
    public void readFileSharedForGroup() throws CatalogException {
        catalogManager.getFile(data_d1_d2_d3_d4, studyAdmin1SessionId);
    }

    @Test
    public void readFileForbiddenForGroup() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1_d2, studyAdmin1SessionId);
    }

    @Test
    public void readFileForbiddenForGroupSharedWithUser() throws CatalogException {
        catalogManager.getFile(data_d1, studyAdmin1SessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1, studyAdmin2SessionId);
    }

    /*--------------------------*/
    // Create file
    /*--------------------------*/

    @Test
    public void createFileByOwnerNoGroups() throws CatalogException {
        catalogManager.removeMemberFromGroup(s1, AuthorizationManager.ADMINS_GROUP, ownerUser, ownerSessionId);
        catalogManager.createFolder(s1, Paths.get("data/newFolder"), true, null, ownerSessionId);
    }

    @Test
    public void createExplicitlySharedFile() throws CatalogException {
        catalogManager.createFolder(s1, Paths.get("data/d1/folder/"), false, null, memberSessionId);
    }

    @Test
    public void createInheritedSharedFile() throws CatalogException {
        catalogManager.createFolder(s1, Paths.get("data/d1/d2/folder/"), false, null, memberSessionId);
    }

    @Test
    public void createExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/folder/"), false, null, memberSessionId);
    }

    @Test
    public void createInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/d4/folder/"), false, null, memberSessionId);
    }

    @Test
    public void createNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("folder/"), false, null, memberSessionId);
    }

    @Test
    public void createFileNoStudyMember() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("data/my_folder/"), false, null, externalSessionId);
    }

    /*--------------------------*/
    // Read Samples
    /*--------------------------*/

    @Test
    public void readSampleOwnerUser() throws CatalogException {
        catalogManager.getSample(smp1, null, ownerSessionId);
        catalogManager.getSample(smp2, null, ownerSessionId);
        catalogManager.getSample(smp3, null, ownerSessionId);

        //Owner always have access
        catalogManager.shareSample(smp1, new AclEntry(ownerUser, false, false, false, false), ownerSessionId);
        catalogManager.getSample(smp1, null, ownerSessionId);
    }

    @Test
    public void readSampleExplicitShared() throws CatalogException {
        catalogManager.getSample(smp1, null, memberSessionId);
    }

    @Test
    public void readSampleExplicitUnshared() throws CatalogException {
        catalogManager.getSample(smp1, null, memberSessionId);
        catalogManager.unshareSample(smp1, memberUser, ownerSessionId);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp1, null, memberSessionId);
    }

    @Test
    public void readSampleNonShared() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp2, null, memberSessionId);
    }

    @Test
    public void readSampleExplicitForbidden() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp3, null, memberSessionId);
    }

    @Test
    public void readSampleExternalUser() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp2, null, externalSessionId);
    }

    @Test
    public void readSampleAdminUser() throws CatalogException {
        catalogManager.getSample(smp1, null, studyAdmin1SessionId);
        catalogManager.getSample(smp3, null, studyAdmin1SessionId);
    }

    @Test
    public void readSampleForbiddenForSampleManagerUser() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp2, null, studyAdmin1SessionId);
    }

    @Test
    public void readSampleSharedForGroup() throws CatalogException {
        catalogManager.getSample(smp4, null, memberSessionId);
    }

    @Test
    public void readSampleSharedForOthers() throws CatalogException {
        catalogManager.getSample(smp6, null, memberSessionId);
    }

    @Test
    public void readSampleSharedForBelongingGroupForbiddenForUser() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getSample(smp5, null, memberSessionId);
    }

    @Test
    public void shareSampleBySampleManagerUser() throws CatalogException {
        catalogManager.shareSample(smp2, new AclEntry(studyAdminUser1, true, true, true, true), studyAdmin1SessionId);
        catalogManager.getSample(smp2, null, studyAdmin1SessionId);
    }

    @Test
    public void readAllSamplesOwner() throws CatalogException {
        Map<Integer, Sample> sampleMap = catalogManager.getAllSamples(s1, new QueryOptions(), ownerSessionId).getResult().stream().collect(Collectors.toMap(Sample::getId, f -> f));

        assertTrue(sampleMap.containsKey(smp1));
        assertTrue(sampleMap.containsKey(smp2));
        assertTrue(sampleMap.containsKey(smp3));
    }
    @Test
    public void readAllSamplesAdmin() throws CatalogException {
        Map<Integer, Sample> sampleMap = catalogManager.getAllSamples(s1, new QueryOptions(), studyAdmin1SessionId).getResult().stream().collect(Collectors.toMap(Sample::getId, f -> f));

        assertTrue(sampleMap.containsKey(smp1));
        assertFalse(sampleMap.containsKey(smp2));
        assertTrue(sampleMap.containsKey(smp3));
    }

    @Test
    public void readAllSamplesMember() throws CatalogException {
        Map<Integer, Sample> sampleMap = catalogManager.getAllSamples(s1, new QueryOptions(), memberSessionId).getResult().stream().collect(Collectors.toMap(Sample::getId, f -> f));

        assertTrue(sampleMap.containsKey(smp1));
        assertFalse(sampleMap.containsKey(smp2));
        assertFalse(sampleMap.containsKey(smp3));
    }

    @Test
    public void readCohort() throws CatalogException {
        int all = catalogManager.createCohort(s1, "all", Cohort.Type.COLLECTION, "", Arrays.asList(smp1, smp2, smp3), null, ownerSessionId).first().getId();
        assertEquals(1, catalogManager.getAllCohorts(s1, null, ownerSessionId).getNumResults());
        assertEquals(0, catalogManager.getAllCohorts(s1, null, memberSessionId).getNumResults());
    }

    /////////// Aux methods
    private Map<String, Group> getGroupMap() throws CatalogException {
        return catalogManager.getStudy(s1, ownerSessionId).first().getGroups().stream().collect(Collectors.toMap(Group::getId, f -> f));
    }

}