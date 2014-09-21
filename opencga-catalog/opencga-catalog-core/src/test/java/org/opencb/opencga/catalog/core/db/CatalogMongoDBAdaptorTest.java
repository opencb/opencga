package org.opencb.opencga.catalog.core.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.*;
import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.*;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.lib.common.TimeUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

@FixMethodOrder(MethodSorters.JVM)
public class CatalogMongoDBAdaptorTest extends GenericTest {

    private static CatalogMongoDBAdaptor catalog;

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


        User fullUser = new User("imedina", "Nacho", "nacho@gmail", "2222", "SPAIN", "BOSS", "active", "", 1222, 122222,
                Arrays.asList( new Project(-1, "90 GigaGenomes", "90G", "today", "very long description", "Spain", "", "", 0, Collections.EMPTY_LIST,
                        Arrays.asList( new Study(-1, "Study name", "ph1", "", "", "", "", "", 1234, "", Collections.EMPTY_LIST, Collections.EMPTY_LIST,
                                        Arrays.asList( new File("file.vcf", "t", "f", "bf", "/data/file.vcf", null, null, "", "", 1000, -1)
                                        ), Collections.EMPTY_LIST, Collections.EMPTY_MAP, Collections.EMPTY_MAP
                                )
                        ), Collections.EMPTY_MAP)
                ),
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_MAP, Collections.EMPTY_MAP);

        System.out.println(catalog.createUser(fullUser).toString());


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
        try {
            System.out.println(catalog.login("jcoll", "INVALID_PASSWORD", sessionJCOLL));
            fail("Expected \"wrong password\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
        System.out.println(catalog.login("jcoll", "1234", sessionJCOLL));
        System.out.println(catalog.login("jmmut", "1111", sessionJMMUT));
        System.out.println(catalog.loginAsAnonymous(sessionANONY));

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
     * Study methods
     * ***************************
     */
    @Test
    public void createStudyTest() throws CatalogManagerException {
        int projectId = catalog.getProjectId("jcoll", "1000G");

        Study s = new Study("Phase 1", "ph1", "TEST", "", "");
        System.out.println(catalog.createStudy(projectId, s));
        s = new Study("Phase 3", "ph3", "TEST", "", "");
        System.out.println(catalog.createStudy(projectId, s));

        try {
            System.out.println(catalog.createStudy(projectId, s));  //Repeated study
            fail("Expected \"bad alias study\" exception");
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
        assertTrue(catalog.getStudyId( projectId, "badStudy") == -1);
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

    /**
     * Files methods
     * ***************************
     */
    @Test
    public void createFileToStudyTest() throws CatalogManagerException, IOException {
        int studyId = catalog.getStudyId("jcoll", "1000G", "ph1");
        assertTrue(studyId >= 0);
        File f;
        f = new File("data/", File.FOLDER, "f", "bam", "/data/", null, TimeUtils.getTime(), "", File.UPLOADING, 1000, -1);
        System.out.println(catalog.createFileToStudy(studyId, f));
        f = new File("file.sam", File.FILE, "sam", "bam", "/data/file.sam", null, TimeUtils.getTime(), "", File.UPLOADING, 1000, -1);
        System.out.println(catalog.createFileToStudy(studyId, f));
        f = new File("file.vcf", File.FILE, "vcf", "bam", "/data/file.vfc", null, TimeUtils.getTime(), "", File.UPLOADING, 1000, -1);

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
        System.out.println(catalog.setFileStatus("jcoll", "1000G", "ph1", "/data/file.sam", File.READY));
        try {
            System.out.println(catalog.setFileStatus("jcoll", "1000G", "ph1", "/data/noExists", File.READY));
            fail("Expected \"FileId not found\" exception");
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
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


    /**
     * Analyses methods
     * ***************************
     */
    @Test
    public void createAnalysisTest() throws CatalogManagerException, JsonProcessingException {
        try {
            Analysis analysis = new Analysis(0, "analisis1Name", "analysis1Alias", "today", "analaysis 1 description", null, null);
            System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph1", analysis));
            analysis = new Analysis(0, "analisis2Name", "analysis2Alias", "lastmonth", "analaysis 2 decrypton", null, null);
            System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph1", analysis));  // different alias, same study
            analysis = new Analysis(0, "analisis2Name", "analysis2Alias", "lastmonth", "analaysis 2 decrypton", null, null);
            System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph3", analysis));  // different study, same alias
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getAllAnalysisTest() throws CatalogManagerException, JsonProcessingException {
        System.out.println(catalog.getAllAnalysis("jcoll", "1000G", "ph1"));
        QueryResult<Analysis> allAnalysis = catalog.getAllAnalysis(8);
        System.out.println(allAnalysis);
    }

    @Test
    public void getAnalysisTest() throws CatalogManagerException, IOException {
        Analysis analysis = new Analysis(0, "analisis1Name", "analysis1Alias", "today", "analaysis 1 description", null, null);
        try {
            System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph1", analysis));
        } catch (CatalogManagerException e) {
            e.printStackTrace();
        }
    }
}
