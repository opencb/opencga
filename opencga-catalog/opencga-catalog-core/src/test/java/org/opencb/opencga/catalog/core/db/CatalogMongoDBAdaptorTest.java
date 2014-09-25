package org.opencb.opencga.catalog.core.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.junit.*;
import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.*;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.lib.common.TimeUtils;

import java.io.IOException;
import java.util.*;

@FixMethodOrder(MethodSorters.JVM)
public class CatalogMongoDBAdaptorTest extends GenericTest {

    private static CatalogDBAdaptor catalog;

    @BeforeClass
    public static void before() throws IllegalOpenCGACredentialsException, JsonProcessingException {
        MongoCredentials mongoCredentials = new MongoCredentials("localhost", 27017, "catalog", "", "");
        catalog = new CatalogMongoDBAdaptor(mongoCredentials);
    }

    @AfterClass
    public static void after(){
        catalog.disconnect();
    }


    /**
     * User methods
     * ***************************
     */
    @Test
    public void createUserTest() throws CatalogManagerException, JsonProcessingException {
        User jcoll = new User("jcoll", "Jacobo Coll", "jcoll@ebi", "1234", "", "", "");
        QueryResult createUser = catalog.createUser(jcoll);
        System.out.println(createUser.toString());

        User jmmut = new User("jmmut", "Jose Miguel", "jmmut@ebi", "1111", "ACME", "no", "off");
        QueryResult createUser2 = catalog.createUser(jmmut);
        System.out.println(createUser2.toString());

        User deletedUser = new User("deletedUser", "name", "email", "pass", "org", "rol", "status");
        catalog.createUser(deletedUser);

        User fullUser = new User("imedina", "Nacho", "nacho@gmail", "2222", "SPAIN", "BOSS", "active", "", 1222, 122222,
                Arrays.asList( new Project(-1, "90 GigaGenomes", "90G", "today", "very long description", "Spain", "", "", 0, Collections.EMPTY_LIST,
                        Arrays.asList( new Study(-1, "Study name", "ph1", "", "", "", "", "", 1234, "", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                                        Arrays.asList( new File("file.vcf", "t", "f", "bf", "/data/file.vcf", null, null, "", "", 1000)
                                        ), Collections.EMPTY_LIST, Collections.EMPTY_MAP, Collections.EMPTY_MAP
                                )
                        ), Collections.EMPTY_MAP)
                ),
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_MAP, Collections.EMPTY_MAP);

        System.out.println(catalog.createUser(fullUser).toString());


    }

    @Test
    public void deleteUserTest() throws CatalogManagerException {
        QueryResult queryResult = catalog.deleteUser("deletedUser");
        System.out.println(queryResult);
        try {
            catalog.deleteUser("noUser");
            fail("error: expected exception");
        } catch (CatalogManagerException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void getUserTest() throws CatalogManagerException {
        System.out.println(catalog.getUser("jcoll", null));
        try {
            System.out.println(catalog.getUser("nonExistingUser", null));
            fail("Expected \"Non existing user\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
    }

    @Test
    public void loginLogoutTest() throws CatalogManagerException, IOException {
        Session sessionJCOLL = new Session("127.0.0.1");
        Session sessionJMMUT = new Session("127.0.0.1");
        Session sessionANONY = new Session("127.0.0.1");
        System.out.println(sessionANONY);
        System.out.println(sessionJCOLL);
        System.out.println(sessionJMMUT);
        try {
            System.out.println(catalog.login("jcoll", "INVALID_PASSWORD", sessionJCOLL));
            fail("Expected \"wrong password\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
        System.out.println(catalog.login("jcoll", "1234", sessionJCOLL));
        System.out.println(catalog.login("jmmut", "1111", sessionJMMUT));
        System.out.println(catalog.loginAsAnonymous(sessionANONY));
        try {
            System.out.println(catalog.loginAsAnonymous(sessionANONY));
            fail("Expected \"invalid sessionId or userId\" exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

        try {
            System.out.println(catalog.login("jmmut", "1111", sessionJMMUT));
            fail("Expected \"invalid sessionId\" exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

        System.out.println(catalog.logout("jcoll", sessionJCOLL.getId()));
        System.out.println(catalog.logout("jmmut", sessionJMMUT.getId()));
        System.out.println(catalog.logoutAnonymous(sessionANONY.getId()));
    }

    @Test
    public void changePasswordTest() throws CatalogManagerException {
        System.out.println(catalog.changePassword("jmmut", "1111", "1234"));
        System.out.println(catalog.changePassword("jmmut", "1234", "1111"));
        try {
            System.out.println(catalog.changePassword("jmmut", "BAD_PASSWORD", "asdf"));
            fail("Expected \"bad password\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
    }

    /**
     * Project methods
     * ***************************
     */
    @Test
    public void createProjectTest() throws CatalogManagerException, JsonProcessingException {
        Project p ;
        p = new Project("Project about some genomes", "1000G", "Today", "Cool", "", "", 1000, "");
        LinkedList<Acl> acl = new LinkedList<>();
        acl.push(new Acl("jcoll", true, false, true, true));
        acl.push(new Acl("jmmut", false, true, true, true));
        p.setAcl(acl);
        System.out.println(catalog.createProject("jcoll", p));
        p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", "", "", 3000, "");
        System.out.println(catalog.createProject("jcoll", p));
        p = new Project("Project management project", "pmp", "yesterday", "it is a system", "", "", 2000, "");
        System.out.println(catalog.createProject("jmmut", p));
        System.out.println(catalog.createProject("jcoll", p));

        try {
            System.out.println(catalog.createProject("jcoll", p));
            fail("Expected \"projectAlias already exists\" exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }
    }

    @Test
    public void getProjectIdTest() throws CatalogManagerException {
        assert catalog.getProjectId("jcoll", "1000G") != -1;
        assert catalog.getProjectId("jcoll", "2000G") != -1;
        assert catalog.getProjectId("jcoll", "nonExistingProject") == -1;
    }


    @Test
    public void getProjectTest() throws CatalogManagerException {
        int projectId = catalog.getProjectId("jcoll", "pmp");
        System.out.println(catalog.getProject(projectId));
        try {
            System.out.println(catalog.getProject(-100));
            fail("Expected \"bad id\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getAllProjects() throws CatalogManagerException {
        System.out.println(catalog.getAllProjects("jcoll"));
    }

    /**
     * cases:
     *      ok: correct projectId, correct newName
     *      error: non-existent projectId
     *      error: newName already used
     *      error: newName == oldName
     * @throws CatalogManagerException
     */
    @Test
    public void renameProjectTest() throws CatalogManagerException {
        int projectId = catalog.getProjectId("jmmut", "pmp");
        System.out.println(catalog.renameProjectAlias(projectId, "newpmp"));

        try {
            System.out.println(catalog.renameProjectAlias(-1, "inexistentProject"));
            fail("renamed project with projectId=-1");
        } catch (CatalogManagerException e) {
            System.out.println("correct exception: " + e);
        }
        try {
            System.out.println(catalog.renameProjectAlias(catalog.getProjectId("jcoll", "1000G"), "2000G"));
            fail("renamed project with name collision");
        } catch (CatalogManagerException e){
            System.out.println("correct exception: " + e);
        }

        try {
            System.out.println(catalog.renameProjectAlias(catalog.getProjectId("jcoll", "1000G"), "1000G"));
            fail("renamed project to its old name");
        } catch (CatalogManagerException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void getProjectAclTest() throws CatalogManagerException {
        int projectId = catalog.getProjectId("jcoll", "1000G");
        List<Acl> acls = catalog.getProjectAcl(projectId, "jmmut").getResult();
        assertTrue(!acls.isEmpty());
        System.out.println(acls.get(0));
        List<Acl> acls2 = catalog.getProjectAcl(projectId, "noUser").getResult();
        assertTrue(acls2.isEmpty());
    }


    @Test
    public void setProjectAclTest() throws CatalogManagerException {
        int projectId = catalog.getProjectId("jcoll", "1000G");
        System.out.println(projectId);

        Acl granted = new Acl("jmmut", true, true, true, false);
        catalog.setProjectAcl(projectId, granted);
        granted.setUserId("imedina");
        catalog.setProjectAcl(projectId, granted);
        try {
            granted.setUserId("noUser");
            catalog.setProjectAcl(projectId, granted);
            fail("error: expected exception");
        } catch (CatalogManagerException e) {
            System.out.println("correct exception: " + e);
        }
    }



    /**
     * Study methods
     * ***************************
     */
    @Test
    public void createStudyTest() throws CatalogManagerException {
        int projectId = catalog.getProjectId("jcoll", "1000G");
        int projectId2 = catalog.getProjectId("jcoll", "2000G");

        Study s = new Study("Phase 1", "ph1", "TEST", "", "");
        LinkedList<Acl> acl = new LinkedList<>();
        acl.push(new Acl("jcoll", false, true, true, true));
        acl.push(new Acl("jmmut", false, false, true, false));
        s.setAcl(acl);
        System.out.println(catalog.createStudy(projectId, s));
        System.out.println(catalog.createStudy(projectId2, s));
        s = new Study("Phase 3", "ph3", "TEST", "", "");
        System.out.println(catalog.createStudy(projectId, s));
        s = new Study("Phase 7", "ph7", "TEST", "", "");
        System.out.println(catalog.createStudy(projectId, s));

        try {
            System.out.println(catalog.createStudy(projectId, s));  //Repeated study
            fail("Expected \"Study alias already exist\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
        try {
            System.out.println(catalog.createStudy(-100, s));  //ProjectId not exists
            fail("Expected \"bad project id\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getStudyIdTest() throws CatalogManagerException, IOException {
        int projectId = catalog.getProjectId("jcoll", "1000G");
        assertTrue(catalog.getStudyId("jcoll", "1000G", "ph3") != -1);
        assertTrue(catalog.getStudyId(projectId, "ph3") != -1);
        assertTrue(catalog.getStudyId( -20, "ph3") == -1);
        assertTrue(catalog.getStudyId(projectId, "badStudy") == -1);
    }

    @Test
    public void getAllStudiesTest() throws CatalogManagerException {
        int projectId = catalog.getProjectId("jcoll", "1000G");
        System.out.println(catalog.getAllStudies(projectId));
        try {
            System.out.println(catalog.getAllStudies(-100));
            fail("Expected \"bad project id\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getStudyTest() throws CatalogManagerException, JsonProcessingException {
        int projectId = catalog.getProjectId("jcoll", "1000G");
        int studyId = catalog.getStudyId(projectId, "ph1");
        System.out.println(catalog.getStudy(studyId));
        try {
            catalog.getStudy(-100);
            fail("Expected \"StudyId not found\" exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }
    }

    @Test
    public void modifyStudyTest() throws CatalogManagerException {
        int projectId = catalog.getProjectId("jcoll", "1000G");
        int studyId = catalog.getStudyId(projectId, "ph1");
//        Map<String, String> parameters = new HashMap<>();
//        parameters.put("name", "new name");
//        Map<String, Object> attributes = new HashMap<>();
//        attributes.put("algo", "new name");
//        attributes.put("otro", null);
//        catalog.modifyStudy(studyId, parameters, attributes, null);

        ObjectMap objectMap = new ObjectMap("name", "My new name");
        HashMap<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("Value", 1);
        attributes.put("Value2", true);
        attributes.put("Value3", new BasicDBObject("key", "ok"));
        objectMap.put("attributes", attributes);
        catalog.modifyStudy(studyId, objectMap);
    }

    @Test
    public void getStudyAclTest() throws CatalogManagerException {
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        List<Acl> jmmut = catalog.getStudyAcl(studyId, "jmmut").getResult();
        assertTrue(!jmmut.isEmpty());
        System.out.println(jmmut.get(0));
        List<Acl> noUser = catalog.getStudyAcl(studyId, "noUser").getResult();
        assertTrue(noUser.isEmpty());
    }

    @Test
    public void setStudyAclTest() throws CatalogManagerException {
        // unimplemented yet
        /*
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        System.out.println(studyId);

        Acl granted = new Acl("jmmut", true, true, true, false);
        catalog.setStudyAcl(studyId, granted);
        granted.setUserId("imedina");
        catalog.setStudyAcl(studyId, granted);
        try {
            granted.setUserId("noUser");
            catalog.setStudyAcl(studyId, granted);
            fail("error: expected exception");
        } catch (CatalogManagerException e) {
            System.out.println("correct exception: " + e);
        }
        */
    }

    /**
     * Files methods
     * ***************************
     */
    @Test
    public void createFileToStudyTest() throws CatalogManagerException, IOException {
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        assertTrue(studyId >= 0);
        File f;
        f = new File("data/", File.FOLDER, "f", "bam", "/data/", null, TimeUtils.getTime(), "", File.UPLOADING, 1000);
        LinkedList<Acl> acl = new LinkedList<>();
        acl.push(new Acl("jcoll", true, true, true, true));
        acl.push(new Acl("jmmut", false, false, true, true));
        f.setAcl(acl);
        System.out.println(catalog.createFileToStudy(studyId, f));
        f = new File("file.sam", File.FILE, "sam", "bam", "/data/file.sam", null, TimeUtils.getTime(), "", File.UPLOADING, 1000);
        System.out.println(catalog.createFileToStudy(studyId, f));
        f = new File("file.vcf", File.FILE, "vcf", "bam", "/data/file.vfc", null, TimeUtils.getTime(), "", File.UPLOADING, 1000);

        try {
            System.out.println(catalog.createFileToStudy(-20, f));
            fail("Expected \"StudyId not found\" exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

        System.out.println(catalog.createFileToStudy(studyId, f));

        try {
            System.out.println(catalog.createFileToStudy(studyId, f));
            fail("Expected \"File already exist\" exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }
    }

    @Test
    public void getFileIdTest() throws CatalogManagerException, IOException {
        assertTrue(catalog.getFileId("jcoll", "1000G", "ph1", "/data/") != -1);
        assertTrue(catalog.getFileId("jcoll", "1000G", "ph1", "/data/file.sam") != -1);
        assertTrue(catalog.getFileId("jcoll", "1000G", "ph1", "/data/file.txt") == -1);
    }

    @Test
    public void getFileTest() throws CatalogManagerException {
        System.out.println(catalog.getFile("jcoll", "1000G", "ph1", "/data/file.sam"));
        try {
            System.out.println(catalog.getFile(-1));
            fail("Expected \"FileId not found\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
    }

    @Test
    public void setFileStatus() throws CatalogManagerException, IOException {
        int fileId = catalog.getFileId("jcoll", "1000G", "ph1", "/data/file.vfc");
        System.out.println(catalog.setFileStatus(fileId, File.UPLOADING));
        assertEquals(catalog.getFile(fileId).getResult().get(0).getStatus(), File.UPLOADING);
        System.out.println(catalog.setFileStatus(fileId, File.UPLOADED));
        assertEquals(catalog.getFile(fileId).getResult().get(0).getStatus(), File.UPLOADED);
        System.out.println(catalog.setFileStatus(fileId, File.READY));
        assertEquals(catalog.getFile(fileId).getResult().get(0).getStatus(), File.READY);
        try {
            System.out.println(catalog.setFileStatus("jcoll", "1000G", "ph1", "/data/noExists", File.READY));
            fail("Expected \"FileId not found\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
    }

    @Test
    public void modifyFileTest() throws CatalogManagerException, IOException {
        int fileId = catalog.getFileId("jcoll", "1000G", "ph1", "/data/file.vfc");
        String newName = "newName-"+ StringUtils.randomString(10);
        DBObject stats = BasicDBObjectBuilder.start().append("stat1", 1).append("stat2", true).append("stat3", "ok"+StringUtils.randomString(20)).get();

        ObjectMap parameters = new ObjectMap();
        parameters.put("name", newName);
        parameters.put("status", File.READY);
        parameters.put("stats", stats);
        System.out.println(catalog.modifyFile(fileId, parameters));

        File file = catalog.getFile(fileId).getResult().get(0);
        assertEquals(file.getName(), newName);
        assertEquals(file.getStatus(), File.READY);
        assertEquals(file.getStats(), stats);

    }

    @Test
    public void deleteFileTest() throws CatalogManagerException, IOException {
        System.out.println(catalog.deleteFile("jcoll", "1000G", "ph1", "/data/file.sam"));
        try {
            System.out.println(catalog.deleteFile("jcoll", "1000G", "ph1", "/data/noExists"));
            fail("Expected \"FileId not found\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
    }
    @Test
    public void getFileAclsTest() throws CatalogManagerException {
        int fileId = catalog.getFileId("jcoll", "1000G", "ph1", "/data/");

        List<Acl> jcoll = catalog.getFileAcl(fileId, "jcoll").getResult();
        assertTrue(!jcoll.isEmpty());
        System.out.println(jcoll.get(0));
        List<Acl> imedina = catalog.getFileAcl(fileId, "imedina").getResult();
        assertTrue(imedina.isEmpty());
    }

    @Test
    public void setFileAclsTest() throws CatalogManagerException {
        int fileId = catalog.getFileId("jcoll", "1000G", "ph1", "/data/file.vfc");
        System.out.println(fileId);

        Acl granted = new Acl("jmmut", true, true, true, false);
        catalog.setFileAcl(fileId, granted);
        granted.setUserId("imedina");
        catalog.setFileAcl(fileId, granted);
        try {
            granted.setUserId("noUser");
            catalog.setFileAcl(fileId, granted);
            fail("error: expected exception");
        } catch (CatalogManagerException e) {
            System.out.println("correct exception: " + e);
        }
    }



    /**
     * Analyses methods
     * ***************************
     */
    @Test
    public void createAnalysisTest() throws CatalogManagerException {
        Analysis analysis = new Analysis(0, "analisis1Name", "analysis1Alias", "today", "creatorId", "creationDate", "analaysis 1 description");
        System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph1", analysis));
        System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph3", analysis));  // different study, same alias
        analysis = new Analysis(0, "analisis2Name", "analysis2Alias", "lastmonth", "creatorId", "creationDate", "analaysis 2 decrypton");
        System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph1", analysis));  // same study, different alias
        analysis = new Analysis(0, "analisis3Name", "analysis3Alias", "lastmonth", "jmmmut", "today", "analaysis 3 decrypton");
        System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph3", analysis));  // different study, different alias
        analysis = new Analysis(0, "analisis3Name", "analysis2Alias", "lastmonth", "jmmmut", "today", "analaysis 2 decrypton");
        System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph3", analysis));  // different study, different alias

        try {
            System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph3", analysis));
            fail("expected \"analysis already exists\" exception");
        } catch (CatalogManagerException e) {
        }
    }

    @Test
    public void getAllAnalysisTest() throws CatalogManagerException, JsonProcessingException {
        System.out.println(catalog.getAllAnalysis("jcoll", "1000G", "ph1"));
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        QueryResult<Analysis> allAnalysis = catalog.getAllAnalysis(studyId);
        System.out.println(allAnalysis);
    }

    @Test
    public void getAnalysisTest() throws CatalogManagerException, JsonProcessingException {
        QueryResult<Analysis> analysis = catalog.getAnalysis(-1);
        if (analysis.getNumResults() != 0) {
            fail("error: expected no analysis. instead returned: " + analysis);
        }
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        int analysisId = catalog.getAnalysisId(studyId, "analysis1Alias");  // analysis3Alias does not belong to ph1
        catalog.getAnalysis(analysisId);
    }

    @Test
    public void getAnalysisIdTest() throws CatalogManagerException {
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        int analysisId = catalog.getAnalysisId(studyId, "analysis3Alias");  // analysis3Alias does not belong to ph1
        System.out.println("analysisId: " + analysisId);
        assertTrue(analysisId < 0);

        studyId = catalog.getStudyId("jcoll", "1000G", "ph3");
        analysisId = catalog.getAnalysisId(studyId, "analysis2Alias");
        System.out.println("analysisId: " + analysisId);
        assertTrue(analysisId >= 0);
    }

    @Test
    public void modifyAnalysisTest() throws CatalogManagerException, IOException {
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph3");
        int analysisId = catalog.getAnalysisId(studyId, "analysis2Alias");
        String newName = "newName-"+ StringUtils.randomString(10);
        String description = "description-"+ StringUtils.randomString(50);
        DBObject attributes = BasicDBObjectBuilder.start().append("stat1", 1).append("stat2", true).append("stat3", "ok"+StringUtils.randomString(20)).get();

        ObjectMap parameters = new ObjectMap();
        parameters.put("name", newName);
        parameters.put("description", description);
        parameters.put("attributes", attributes);
        System.out.println(catalog.modifyAnalysis(analysisId, parameters));

        Analysis analysis = catalog.getAnalysis(analysisId).getResult().get(0);
        System.out.println(analysis);
        assertEquals(analysis.getName(), newName);
        assertEquals(analysis.getDescription(), description);
        assertEquals(analysis.getAttributes(), attributes);

    }
//    getStudyIdByAnalysisId    // TODO


    /**
     * Job methods
     * ***************************
     */
    @Test
    public void createJobTest () throws CatalogManagerException {
        Job job = new Job();
        job.setVisits(0);
        job.setName("jobName1");

        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        int analysisId = catalog.getAnalysisId(studyId, "analysis1Alias");
        System.out.println(catalog.createJob(analysisId, job));
        job.setName("jobName2");
        System.out.println(catalog.createJob(analysisId, job));
        try {
            catalog.createJob(-1, job);
            fail("error: expected exception");
        } catch (CatalogManagerException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void getAllJobTest () throws CatalogManagerException {
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        int analysisId = catalog.getAnalysisId(studyId, "analysis1Alias");
        QueryResult<Job> allJobs = catalog.getAllJobs(analysisId);
        System.out.println(allJobs);
    }


    @Test
    public void getJobTest () throws CatalogManagerException {
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        int analysisId = catalog.getAnalysisId(studyId, "analysis1Alias");
        QueryResult<Job> allJobs = catalog.getAllJobs(analysisId);
        Job job = catalog.getJob(allJobs.getResult().get(0).getId()).getResult().get(0);
        System.out.println(job);

        try {
            catalog.getJob(-1);
            fail("error: expected exception");
        } catch (CatalogManagerException e) {
            System.out.println("correct exception: " + e);
        }

    }

    @Test
    public void incJobVisits () throws CatalogManagerException {
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        int analysisId = catalog.getAnalysisId(studyId, "analysis1Alias");
        int id = catalog.getAllJobs(analysisId).getResult().get(0).getId();
        Job jobBefore = catalog.getJob(id).getResult().get(0);

        Integer visits = (Integer) catalog.incJobVisits(jobBefore.getId()).getResult().get(0).get("visits");

        Job jobAfter = catalog.getJob(id).getResult().get(0);
        assertTrue(jobBefore.getVisits() == jobAfter.getVisits() - 1);
        assertTrue(visits == jobAfter.getVisits());
    }

}
