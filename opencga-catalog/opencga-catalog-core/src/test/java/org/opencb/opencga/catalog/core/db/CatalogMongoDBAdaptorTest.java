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
import java.nio.file.Paths;

import static org.junit.Assert.*;

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
        catalog.connect();

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
        createFileToStudyTest();
    }

    /**
     * User methods ···
     * ***************************
     */
    @Test
    public void createUserTest() throws CatalogManagerException, JsonProcessingException {
        User userInvalid = new User("jcoll", "Jacobo Coll", "jcoll@ebi", "1234", "", "", "");
        QueryResult createUser = catalog.createUser(userInvalid);
        System.out.println(createUser.toString());

        User user = new User("jmmut", "Jose Miguel", "jmmut@ebi", "1111", "ACME", "no", "off");
        QueryResult createUser2 = catalog.createUser(user);
        System.out.println(createUser2.toString());
    }

    @Test
    public void getUserTest() {
        try {
            QueryResult jcoll = catalog.getUser("jcoll", "", ID_LOGIN_JCOLL);
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
        System.out.println(catalog.logout("jcoll", session.getId()));
    }

    @Test
    public void logInOutTest() throws CatalogManagerException, IOException {
        Session session = new Session("127.0.0.1");
        System.out.println(catalog.login("jmmut", "1111", session));
        System.out.println(catalog.logout("jmmut", session.getId()));

    }

    @Test
    public void changePasswordTest() throws CatalogManagerException {
        System.out.println(catalog.changePassword("jcoll", ID_LOGIN_JCOLL, "1234", "asdf"));
        System.out.println(catalog.changePassword("jcoll", ID_LOGIN_JCOLL, "BAD_PASSWORD", "asdf"));
    }

    /**
     * Project methods ···
     * ***************************
     */
    @Test
    public void createProjectTest() throws CatalogManagerException, JsonProcessingException {
        Project p = new Project("Project about some genomes", "1000G", "Today", "Cool", "", "", 1000, "");
        System.out.println(catalog.createProject("jcoll", p, ID_LOGIN_JCOLL));
        p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", "", "", 3000, "");
        System.out.println(catalog.createProject("jcoll", p, ID_LOGIN_JCOLL));
        p = new Project("Project management project", "pmp", "yesterday", "it is a system", "", "", 2000, "");
        System.out.println(catalog.createProject("jcoll", p, ID_LOGIN_JMMUT));
    }

    @Test
    public void getProjectTest() throws CatalogManagerException {
        System.out.println(catalog.getProject("jcoll", "pmp", ID_LOGIN_JCOLL));
    }


    @Test
    public void getProjectIdTest(){
        System.out.println(catalog.getProjectId("jcoll", "1000G"));
        System.out.println(catalog.getProjectId("jcoll", "2000G"));
    }

    @Test
    public void getAllProjects() throws CatalogManagerException {
        System.out.println(catalog.getAllProjects("jcoll", ID_LOGIN_JCOLL));
    }

    /**
     * Study methods ···
     * ***************************
     */
    @Test
    public void createStudyTest() throws CatalogManagerException, JsonProcessingException {
        Study s = new Study("Phase 1", "ph1", "TEST", "", "");
        System.out.println(catalog.createStudy("jcoll", "1000G", s, ID_LOGIN_JCOLL));
        s = new Study("Phase 3", "ph3", "TEST", "", "");
        System.out.println(catalog.createStudy("jcoll", "1000G", s, ID_LOGIN_JCOLL));
    }

    @Test
    public void getStudyIdTest() throws CatalogManagerException {
        System.out.println(catalog.getStudyId("jcoll", "1000G", "ph3", ID_LOGIN_JCOLL));
    }

    @Test
    public void getAllStudiesTest() throws CatalogManagerException, JsonProcessingException {
        System.out.println(catalog.getAllStudies("jcoll", "1000G", ID_LOGIN_JCOLL));
    }


    /**
     * Files methods ···
     * ***************************
     */
    @Test
    public void createFileToStudyTest() throws CatalogManagerException, JsonProcessingException {
        File f = new File("file.sam", "t", "f", "bam", "/data/file.sam", null, TimeUtils.getTime(), "", File.UPLOADING, 1000, -1, -1);
        System.out.println(catalog.createFileToStudy("jcoll", "1000G", "ph1", f, ID_LOGIN_JCOLL));
    }

    @Test
    public void getFileTest() throws CatalogManagerException, IOException {
        System.out.println(catalog.getFile("jcoll", "1000G", "ph1", Paths.get("/data/file.sam"), ID_LOGIN_JCOLL));
        System.out.println(catalog.getFile(1, ID_LOGIN_JCOLL));
        System.out.println(catalog.getFile(2, ID_LOGIN_JCOLL));
    }

    @Test
    public void setFileStatus() throws CatalogManagerException, IOException {
        System.out.println(catalog.setFileStatus("jcoll", "1000G", "ph1", Paths.get("/data/file.sam"), File.READY, ID_LOGIN_JCOLL));
    }

    @Test
    public void deleteFileTest() throws CatalogManagerException {
        System.out.println(catalog.deleteFile("jcoll", "1000G", "ph1", Paths.get("/data/file.sam"), ID_LOGIN_JCOLL));
    }

}
