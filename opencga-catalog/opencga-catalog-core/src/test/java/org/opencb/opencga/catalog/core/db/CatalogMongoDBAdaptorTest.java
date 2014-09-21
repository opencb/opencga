package org.opencb.opencga.catalog.core.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.*;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.lib.common.TimeUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class CatalogMongoDBAdaptorTest extends GenericTest {

    public static final String ID_LOGIN_JCOLL = "ID_LOGIN_JCOLL";
    public static final String ID_LOGIN_JMMUT = "ID_LOGIN_JMMUT";
    public static final String ID_LOGIN_ANONY = "ID_LOGIN_ANONY";
    CatalogMongoDBAdaptor catalog;
    private Session session;

    @Before
    public void before() throws IllegalOpenCGACredentialsException, JsonProcessingException {

        MongoCredentials mongoCredentials = new MongoCredentials("localhost", 27017, "catalog", "", "");
        catalog = new CatalogMongoDBAdaptor(mongoCredentials);
    }

    @After
    public void after(){
        catalog.disconnect();
    }


    @Test
    public void allTests() throws CatalogManagerException, IOException {
        createUserTest();
        getUserTest();
        loginTest();
        logoutTest();
        changePasswordTest();
        createProjectTest();
        getProjectTest();
        getProjectIdTest();
        getAllProjects();
        createStudyTest();
        getStudyIdTest();
        getAllStudiesTest();
        getStudyTest();
        createAnalysisTest();
        getAllAnalysisTest();
        getAnalysisTest();
        getAnalysisIdTest();
        createFileToStudyTest();
    }

    /**
     * User methods
     * ***************************
     */
    @Test
    public void createUserTest() throws CatalogManagerException, JsonProcessingException {
        User user1 = new User("jcoll", "Jacobo Coll", "jcoll@ebi", "1234", "", "", "");
        QueryResult createUser = catalog.createUser(user1);
        System.out.println(createUser.toString());

        User user = new User("jmmut", "Jose Miguel", "jmmut@ebi", "1111", "ACME", "no", "off");
        QueryResult createUser2 = catalog.createUser(user);
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
    public void getUserTest() {
        try {
            QueryResult jcoll = catalog.getUser("jcoll", "");
            System.out.println(jcoll);
        } catch (CatalogManagerException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void loginTest() throws CatalogManagerException, IOException {
        session = new Session("127.0.0.1");
        Session sessionJCOLL = new Session("127.0.0.1"); sessionJCOLL.setId(ID_LOGIN_JCOLL);
        Session sessionJMMUT = new Session("127.0.0.1"); sessionJMMUT.setId(ID_LOGIN_JMMUT);
        Session sessionANONY = new Session("127.0.0.1"); sessionANONY.setId(ID_LOGIN_ANONY);
        System.out.println(catalog.login("jcoll", "INVALID_PASSWORD", session));
        System.out.println(catalog.login("jcoll", "1234", session));
        System.out.println(catalog.login("jcoll", "1234", sessionJCOLL));
        System.out.println(catalog.login("jmmut", "1111", sessionJMMUT));
        System.out.println(catalog.loginAsAnonymous(sessionANONY));

    }

    @Test
    public void logoutTest() throws CatalogManagerException, IOException {
        if(session != null){
            System.out.println(catalog.logout("jcoll", session.getId()));
        }
    }

    @Test
    public void logInOutTest() throws CatalogManagerException, IOException {
        Session session = new Session("127.0.0.1");
        System.out.println(catalog.login("jmmut", "1111", session));
        System.out.println(catalog.logout("jmmut", session.getId()));

    }

    @Test
    public void changePasswordTest() throws CatalogManagerException {
        System.out.println(catalog.changePassword("jmmut", ID_LOGIN_JMMUT, "1234"));
        System.out.println(catalog.changePassword("jmmut", ID_LOGIN_JMMUT, "BAD_PASSWORD"));
    }

    /**
     * Project methods
     * ***************************
     */
    @Test
    public void createProjectTest() throws CatalogManagerException, JsonProcessingException {
        Project p = new Project("Project about some genomes", "1000G", "Today", "Cool", "", "", 1000, "");
        System.out.println(catalog.createProject("jcoll", p));
        p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", "", "", 3000, "");
        System.out.println(catalog.createProject("jcoll", p));
        p = new Project("Project management project", "pmp", "yesterday", "it is a system", "", "", 2000, "");
        System.out.println(catalog.createProject("jcoll", p));
    }

    @Test
    public void getProjectTest() throws CatalogManagerException {
        System.out.println(catalog.getProject("jcoll", "pmp"));
    }


    @Test
    public void getProjectIdTest(){
        try {
            System.out.println(catalog.getProjectId("jcoll", "1000G"));
            System.out.println(catalog.getProjectId("jcoll", "2000G"));
        } catch (CatalogManagerException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getAllProjects() throws CatalogManagerException {
        System.out.println(catalog.getAllProjects("jcoll", ID_LOGIN_JCOLL));
    }

    /**
     * Study methods
     * ***************************
     */
    @Test
    public void createStudyTest() throws CatalogManagerException, JsonProcessingException {
        Project p = catalog.getProject("jcoll", "1000G").getResult().get(0);
        Study s = new Study("Phase 1", "ph1", "TEST", "", "");
        System.out.println(catalog.createStudy(p.getId(), s));
        s = new Study("Phase 3", "ph3", "TEST", "", "");
        System.out.println(catalog.createStudy(p.getId(), s));
    }

    @Test
    public void getStudyIdTest() throws CatalogManagerException, IOException {
        System.out.println(catalog.getStudyId("jcoll", "1000G", "ph3"));
    }

    @Test
    public void getAllStudiesTest() throws CatalogManagerException, JsonProcessingException {
        System.out.println(catalog.getAllStudies("jcoll", "1000G"));
    }

    @Test
    public void getStudyTest() throws CatalogManagerException, JsonProcessingException {

        QueryResult<Study> study = catalog.getStudy(5, ID_LOGIN_JCOLL);
        System.out.println(study);
    }

    /**
     * Files methods
     * ***************************
     */
    @Test
    public void createFileToStudyTest() throws CatalogManagerException, IOException {
        File f = new File("file.bam", "t", "f", "bam", "/data/file.bam", null, TimeUtils.getTime(), "", File.UPLOADING, 1000, -1);
        System.out.println(catalog.createFileToStudy("jcoll", "1000G", "ph1", f));
    }

    @Test
    public void getFileTest() throws CatalogManagerException, IOException {
        System.out.println(catalog.getFile("jcoll", "1000G", "ph1", "/data/file.bam"));
        System.out.println(catalog.getFile(1));
        System.out.println(catalog.getFile(2));
    }

    @Test
    public void setFileStatus() throws CatalogManagerException, IOException {
        System.out.println(catalog.setFileStatus("jcoll", "1000G", "ph1", "/data/file.bam", File.READY, ID_LOGIN_JCOLL));
    }

    @Test
    public void deleteFileTest() throws CatalogManagerException, IOException {
        System.out.println(catalog.deleteFile("jcoll", "1000G", "ph1", "/data/file.bam"));
    }


    /**
     * Analyses methods
     * ***************************
     */
    @Test
    public void createAnalysisTest() throws CatalogManagerException, IOException {
        Analysis analysis = new Analysis(0, "analisis1Name", "analysis1Alias", "today", "creatorId", "creationDate", "analaysis 1 description", null, null);
        System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph1", analysis));
        analysis = new Analysis(0, "analisis2Name", "analysis2Alias", "lastmonth", "creatorId", "creationDate", "analaysis 2 decrypton", null, null);
        System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph1", analysis));  // different alias, same study
        analysis = new Analysis(0, "analisis2Name", "analysis2Alias", "lastmonth", "creatorId", "creationDate", "analaysis 2 decrypton", null, null);
        System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph3", analysis));  // different study, same alias
        analysis = new Analysis(0, "analisis3Name", "analysis3Alias", "lastmonth", "jmmmut", "today", "analaysis 3 decrypton", null, null);
        System.out.println(catalog.createAnalysis("jcoll", "1000G", "ph3", analysis));  // different study, same alias
    }

    @Test
    public void getAllAnalysisTest() throws CatalogManagerException, JsonProcessingException {
        System.out.println(catalog.getAllAnalysis("jcoll", "1000G", "ph1"));
        QueryResult<Analysis> allAnalysis = catalog.getAllAnalysis(8);
        System.out.println(allAnalysis);
    }

    @Test
    public void getAnalysisTest() throws CatalogManagerException, JsonProcessingException {
            System.out.println(catalog.getAnalysis(8));
    }

    @Test
    public void getAnalysisIdTest() throws CatalogManagerException {
        System.out.println(catalog.getAnalysisId(8, "analysis2Alias"));
    }

}
