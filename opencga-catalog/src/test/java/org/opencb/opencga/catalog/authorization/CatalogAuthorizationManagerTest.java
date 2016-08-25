package org.opencb.opencga.catalog.authorization;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.StringUtils;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

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
    private int data_d1_d2_d3_d4_txt; // Shared for member
    private int smp1;   // Shared with member
    private int smp2;   // Shared with studyAdmin1
    private int smp3;   // Shared with member
    private int smp4;   // Shared with @members
    private int smp6;   // Shared with *
    private int smp5;   // Shared with @members, forbidden for memberUse
    private int ind1;
    private int ind2;

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
        data_d1_d2_d3_d4_txt = catalogManager.createFile(s1, File.Format.PLAIN, File.Bioformat.NONE, "data/d1/d2/d3/d4/my.txt", "file content".getBytes(), "", false, ownerSessionId).first().getId();

        catalogManager.addMemberToGroup(s1, AuthorizationManager.MEMBERS_GROUP, memberUser, ownerSessionId);
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, studyAdminUser1, ownerSessionId);

        catalogManager.shareFile(data_d1, new AclEntry(memberUser, true, true, true, true), ownerSessionId);
        catalogManager.shareFile(data_d1, new AclEntry("@" + AuthorizationManager.ADMINS_GROUP, false, false, false, false), ownerSessionId);
        catalogManager.shareFile(data_d1, new AclEntry(studyAdminUser1, true, true, true, true), ownerSessionId);
        catalogManager.shareFile(data_d1_d2, new AclEntry("@" + AuthorizationManager.ADMINS_GROUP, false, false, false, false), ownerSessionId);
        catalogManager.shareFile(data_d1_d2_d3, new AclEntry(memberUser, false, false, false, false), ownerSessionId);
        catalogManager.shareFile(data_d1_d2_d3_d4, new AclEntry("@" + AuthorizationManager.ADMINS_GROUP, true, true, true, true), ownerSessionId);
        catalogManager.shareFile(data_d1_d2_d3_d4_txt, new AclEntry(memberUser, true, true, true, true), ownerSessionId);

        smp1 = catalogManager.createSample(s1, "smp1", null, null, null, null, ownerSessionId).first().getId();
        smp2 = catalogManager.createSample(s1, "smp2", null, null, null, null, ownerSessionId).first().getId();
        smp3 = catalogManager.createSample(s1, "smp3", null, null, null, null, ownerSessionId).first().getId();
        smp4 = catalogManager.createSample(s1, "smp4", null, null, null, null, ownerSessionId).first().getId();
        smp5 = catalogManager.createSample(s1, "smp5", null, null, null, null, ownerSessionId).first().getId();
        smp6 = catalogManager.createSample(s1, "smp6", null, null, null, null, ownerSessionId).first().getId();
        catalogManager.createCohort(s1, "all", Cohort.Type.COLLECTION, "", Arrays.asList(smp1, smp2, smp3), null, ownerSessionId).first().getId();
        ind1 = catalogManager.createIndividual(s1, "ind1", "", 0, 0, Individual.Gender.UNKNOWN, null, ownerSessionId).first().getId();
        ind2 = catalogManager.createIndividual(s1, "ind2", "", 0, 0, Individual.Gender.UNKNOWN, null, ownerSessionId).first().getId();
        catalogManager.modifySample(smp1, new QueryOptions("individualId", ind1), ownerSessionId);
        catalogManager.modifySample(smp2, new QueryOptions("individualId", ind1), ownerSessionId);
        catalogManager.modifySample(smp2, new QueryOptions("individualId", ind2), ownerSessionId);

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
        catalogManager.getProject(p1, null, ownerSessionId);
    }

    @Test
    public void readProjectDenny() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getProject(p1, null, externalSessionId);
    }

    @Test
    public void readProjectAllow() throws CatalogException {
        catalogManager.getProject(p1, null, memberSessionId);
    }

    @Test
    public void readProjectAllow2() throws CatalogException {
        catalogManager.getProject(p1, null, ownerSessionId);
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
    public void readExplicitlySharedFolder() throws CatalogException {
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
    public void readExplicitlySharedFile() throws CatalogException {
        catalogManager.getFile(data_d1_d2_d3_d4_txt, memberSessionId);
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
        assertEquals(1, catalogManager.getAllCohorts(s1, null, ownerSessionId).getNumResults());
        assertEquals(0, catalogManager.getAllCohorts(s1, null, memberSessionId).getNumResults());
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
        catalogManager.getIndividual(ind2, null, memberSessionId);
    }

    @Test
    public void readIndividualStudyManager() throws CatalogException {
        catalogManager.getIndividual(ind2, null, studyAdmin1SessionId);
    }

    @Test
    public void readAllIndividuals() throws CatalogException {
        assertEquals(2, catalogManager.getAllIndividuals(s1, null, ownerSessionId).getNumResults());
        assertEquals(2, catalogManager.getAllIndividuals(s1, null, studyAdmin1SessionId).getNumResults());
        assertEquals(1, catalogManager.getAllIndividuals(s1, null, memberSessionId).getNumResults());
    }



    /*--------------------------*/
    // Read Jobs
    /*--------------------------*/

    @Test
    public void getAllJobs() throws CatalogException {
        int studyId = s1;
        int outDirId = this.data_d1_d2;

        URI tmpJobOutDir = catalogManager.createJobOutDir(studyId, StringUtils.randomString(5), ownerSessionId);
        int job1 = catalogManager.createJob(studyId, "job1", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello World!\"",
                tmpJobOutDir, outDirId, Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), null, Job.Status.ERROR, 0, 0, null, ownerSessionId).first().getId();
        int job2 = catalogManager.createJob(studyId, "job2", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello World!\"",
                tmpJobOutDir, outDirId, Collections.singletonList(data_d1_d2), Collections.emptyList(),
                new HashMap<>(), null, Job.Status.ERROR, 0, 0, null, ownerSessionId).first().getId();
        int job3 = catalogManager.createJob(studyId, "job3", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello World!\"",
                tmpJobOutDir, outDirId, Collections.singletonList(data_d1_d2_d3), Collections.emptyList(),
                new HashMap<>(), null, Job.Status.ERROR, 0, 0, null, ownerSessionId).first().getId();
        int job4 = catalogManager.createJob(studyId, "job4", "toolName", "d", "", Collections.emptyMap(), "echo \"Hello World!\"",
                tmpJobOutDir, outDirId, Collections.singletonList(data_d1_d2_d3_d4), Collections.emptyList(),
                new HashMap<>(), null, Job.Status.ERROR, 0, 0, null, ownerSessionId).first().getId();

        checkGetAllJobs(studyId, Arrays.asList(job1, job2, job3, job4), ownerSessionId);    //Owner can see everything
        checkGetAllJobs(studyId, Arrays.asList(job1, job2), memberSessionId);               //Can't see inside data_d1_d2_d3
        checkGetAllJobs(studyId, Arrays.asList(job1, job4), studyAdmin1SessionId);          //Can only see data_d1_d2_d3_d4
    }

    public void checkGetAllJobs(int studyId, Collection<Integer> expectedJobs, String sessionId) throws CatalogException {
        QueryResult<Job> allJobs = catalogManager.getAllJobs(studyId, sessionId);

        assertEquals(expectedJobs.size(), allJobs.getNumResults());
        allJobs.getResult().forEach(job -> assertTrue(expectedJobs + " does not contain job " + job.getName(), expectedJobs.contains(job.getId())));
    }

    /////////// Aux methods
    private Map<String, Group> getGroupMap() throws CatalogException {
        return catalogManager.getStudy(s1, ownerSessionId).first().getGroups().stream().collect(Collectors.toMap(Group::getId, f -> f));
    }

}