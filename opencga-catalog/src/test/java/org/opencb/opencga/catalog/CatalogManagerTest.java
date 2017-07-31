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

package org.opencb.opencga.catalog;

import com.mongodb.BasicDBObject;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.api.IIndividualManager;
import org.opencb.opencga.catalog.managers.api.IStudyManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.models.summaries.FeatureCount;
import org.opencb.opencga.catalog.models.summaries.VariableSetSummary;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidatorTest;
import org.opencb.opencga.core.results.LdapImportResult;

import javax.naming.NamingException;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import static org.opencb.opencga.catalog.db.api.SampleDBAdaptor.QueryParams.ANNOTATION_SET_NAME;
import static org.opencb.opencga.catalog.db.api.SampleDBAdaptor.QueryParams.VARIABLE_SET_ID;

public class CatalogManagerTest extends GenericTest {

    public final static String PASSWORD = "asdf";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    protected String sessionIdUser;
    protected String sessionIdUser2;
    protected String sessionIdUser3;
    private File testFolder;
    private long project1;
    private long project2;
    private long studyId;
    private long studyId2;
    private long s_1;
    private long s_2;
    private long s_3;
    private long s_4;
    private long s_5;
    private long s_6;
    private long s_7;
    private long s_8;
    private long s_9;

    /* TYPE_FILE UTILS */
    public static java.io.File createDebugFile() throws IOException {
        String fileTestName = "/tmp/fileTest " + StringUtils.randomString(5);
        return createDebugFile(fileTestName);
    }

    public static java.io.File createDebugFile(String fileTestName) throws IOException {
        return createDebugFile(fileTestName, 200);
    }

    public static java.io.File createDebugFile(String fileTestName, int lines) throws IOException {
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fileTestName));

        os.writeBytes("Debug file name: " + fileTestName + "\n");
        for (int i = 0; i < 100; i++) {
            os.writeBytes(i + ", ");
        }
        for (int i = 0; i < lines; i++) {
            os.writeBytes(StringUtils.randomString(500));
            os.write('\n');
        }
        os.close();

        return Paths.get(fileTestName).toFile();
    }


    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {

        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, null);
        catalogManager.createUser("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", null, null);
        catalogManager.createUser("user3", "User3 Name", "user.2@e.mail", PASSWORD, "ACME", null, null);

        sessionIdUser = catalogManager.login("user", PASSWORD, "127.0.0.1").first().getId();
        sessionIdUser2 = catalogManager.login("user2", PASSWORD, "127.0.0.1").first().getId();
        sessionIdUser3 = catalogManager.login("user3", PASSWORD, "127.0.0.1").first().getId();

        project1 = catalogManager.getProjectManager().create("Project about some genomes", "1000G", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        project2 = catalogManager.getProjectManager().create("Project Management Project", "pmp", "life art intelligent system", "myorg",
                "Homo sapiens", null, null, "GRCh38", new QueryOptions(), sessionIdUser2).first().getId();
        Project project3 = catalogManager.getProjectManager().create("project 1", "p1", "", "", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser3).first();

        studyId = catalogManager.createStudy(project1, "Phase 1", "phase1", Study.Type.TRIO, "Done", sessionIdUser).first().getId();
        studyId2 = catalogManager.createStudy(project1, "Phase 3", "phase3", Study.Type.CASE_CONTROL, "d", sessionIdUser).first().getId();
        catalogManager.createStudy(project2, "Study 1", "s1", Study.Type.CONTROL_SET, "", sessionIdUser2).first().getId();

        catalogManager.getFileManager().createFolder(Long.toString(studyId2), Paths.get("data/test/folder/").toString(), null, true,
                null, QueryOptions.empty(), sessionIdUser);

        catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("analysis/").toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser);
        catalogManager.getFileManager().createFolder(Long.toString(studyId2), Paths.get("analysis/").toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser);

        testFolder = catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("data/test/folder/").toString(),
                null, true, null, QueryOptions.empty(), sessionIdUser).first();
        ObjectMap attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("numValue", 5);
        catalogManager.getFileManager().update(testFolder.getId(), new ObjectMap("attributes", attributes), new QueryOptions(),
                sessionIdUser);

        File fileTest1k = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE,
                testFolder.getPath() + "test_1K.txt.gz",
                StringUtils.randomString(1000).getBytes(), "", false, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("name", "fileTest1k");
        attributes.put("numValue", "10");
        attributes.put("boolean", false);
        catalogManager.getFileManager().update(fileTest1k.getId(), new ObjectMap("attributes", attributes), new QueryOptions(),
                sessionIdUser);

        File fileTest05k = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.DATAMATRIX_EXPRESSION,
                testFolder.getPath() + "test_0.5K.txt",
                StringUtils.randomString(500).getBytes(), "", false, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "valuable");
        attributes.put("name", "fileTest05k");
        attributes.put("numValue", 5);
        attributes.put("boolean", true);
        catalogManager.getFileManager().update(fileTest05k.getId(), new ObjectMap("attributes", attributes), new QueryOptions(),
                sessionIdUser);

        File test01k = catalogManager.createFile(studyId, File.Format.IMAGE, File.Bioformat.NONE,
                testFolder.getPath() + "test_0.1K.png",
                StringUtils.randomString(100).getBytes(), "", false, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "other");
        attributes.put("name", "test01k");
        attributes.put("numValue", 50);
        attributes.put("nested", new ObjectMap("num1", 45).append("num2", 33).append("text", "HelloWorld"));
        catalogManager.getFileManager().update(test01k.getId(), new ObjectMap("attributes", attributes), new QueryOptions(), sessionIdUser);

        Set<Variable> variables = new HashSet<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:130"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("EXTRA", "", Variable.VariableType.TEXT, "", false, false, Collections.emptyList(), 5, "", "", null,
                        Collections.<String, Object>emptyMap())
        ));
        VariableSet vs = catalogManager.getStudyManager().createVariableSet(studyId, "vs", true, false, "", null, variables, sessionIdUser).first();


        s_1 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_1", "", "", null, false, null, null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_2 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_2", "", "", null, false, null, null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_3 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_3", "", "", null, false, null, null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_4 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_4", "", "", null, false, null, null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_5 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_5", "", "", null, false, null, null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_6 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_6", "", "", null, false, null, null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_7 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_7", "", "", null, false, null, null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_8 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_8", "", "", null, false, null, null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_9 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_9", "", "", null, false, null, null, new QueryOptions()
                , sessionIdUser).first().getId();

        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_1), null, Long.toString(vs.getId()), "annot1", new ObjectMap("NAME", "s_1").append("AGE", 6).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_2), null, Long.toString(vs.getId()), "annot1", new ObjectMap("NAME", "s_2").append("AGE", 10).append("ALIVE", false)

                .append("PHEN", "CASE"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_3), null, Long.toString(vs.getId()), "annot1", new ObjectMap("NAME", "s_3").append("AGE", 15).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_4), null, Long.toString(vs.getId()), "annot1", new ObjectMap("NAME", "s_4").append("AGE", 22).append("ALIVE", false)
                .append("PHEN", "CONTROL"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_5), null, Long.toString(vs.getId()), "annot1", new ObjectMap("NAME", "s_5").append("AGE", 29).append("ALIVE", true)
                .append("PHEN", "CASE"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_6), null, Long.toString(vs.getId()), "annot2", new ObjectMap("NAME", "s_6").append("AGE", 38).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_7), null, Long.toString(vs.getId()), "annot2", new ObjectMap("NAME", "s_7").append("AGE", 46).append("ALIVE", false)
                .append("PHEN", "CASE"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_8), null, Long.toString(vs.getId()), "annot2", new ObjectMap("NAME", "s_8").append("AGE", 72).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, sessionIdUser);


        catalogManager.getFileManager().update(test01k.getId(), new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(),
                        Arrays.asList(s_1, s_2, s_3, s_4, s_5)), new QueryOptions(), sessionIdUser);

    }

    @After
    public void tearDown() throws Exception {
    }

    public CatalogManager getTestCatalogManager() {
        return catalogManager;
    }

    @Test
    public void testAdminUserExists() throws Exception {
        QueryResult<Session> login = catalogManager.getUserManager().login("admin", "admin", "localhost");
        assertEquals("admin" ,catalogManager.getUserManager().getId(login.first().getId()));
    }

    @Test
    public void testCreateExistingUser() throws Exception {
        thrown.expect(CatalogException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, null);
    }

    @Test
    public void testLogin() throws Exception {
        catalogManager.login("user", PASSWORD, "127.0.0.1");

        thrown.expect(CatalogAuthenticationException.class);
        thrown.expectMessage(allOf(containsString("Incorrect"), containsString("password")));
        catalogManager.login("user", "fakePassword", "127.0.0.1");
    }

    @Test
    public void dummyLogin() throws Exception {
        QueryResult<Session> user = catalogManager.login("user", PASSWORD, "127.0.0.1");

        ObjectMap sessionMap = new ObjectMap();
        sessionMap.append("id", user.first().getId()).append("sessionId", user.first().getId()).append("ip", user.first().getIp())
                .append("date", user.first().getDate());

        QueryResult<ObjectMap> login = new QueryResult<>("login", user.getDbTime(), 1, 1, user.getWarningMsg(), user.getErrorMsg(),
                Arrays.asList(sessionMap));

        System.out.println(login);

    }

    @Test
    public void testGetUserInfo() throws CatalogException {
        QueryResult<User> user = catalogManager.getUser("user", null, sessionIdUser);
        System.out.println("user = " + user);
        QueryResult<User> userVoid = catalogManager.getUser("user", user.first().getLastModified(), sessionIdUser);
        System.out.println("userVoid = " + userVoid);
        assertTrue(userVoid.getResult().isEmpty());
        try {
            catalogManager.getUser("user", null, sessionIdUser2);
            fail();
        } catch (CatalogException e) {
            System.out.println(e);
        }
    }

    @Test
    public void testModifyUser() throws CatalogException, InterruptedException, IOException {
        ObjectMap params = new ObjectMap();
        String newName = "Changed Name " + StringUtils.randomString(10);
        String newPassword = StringUtils.randomString(10);
        String newEmail = "new@email.ac.uk";

        params.put("name", newName);
        ObjectMap attributes = new ObjectMap("myBoolean", true);
        attributes.put("value", 6);
        attributes.put("object", new BasicDBObject("id", 1234));
        params.put("attributes", attributes);

        User userPre = catalogManager.getUser("user", null, sessionIdUser).first();
        System.out.println("userPre = " + userPre);
        Thread.sleep(10);

        catalogManager.modifyUser("user", params, sessionIdUser);
        catalogManager.changeEmail("user", newEmail, sessionIdUser);
        catalogManager.changePassword("user", PASSWORD, newPassword);

        List<User> userList = catalogManager.getUser("user", userPre.getLastModified(),
                new QueryOptions(QueryOptions.INCLUDE,
                        Arrays.asList(UserDBAdaptor.QueryParams.PASSWORD.key(), UserDBAdaptor.QueryParams.NAME.key(),
                                UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ATTRIBUTES.key())),
                sessionIdUser).getResult();
        if (userList.isEmpty()) {
            fail("Error. LastModified should have changed");
        }
        User userPost = userList.get(0);
        System.out.println("userPost = " + userPost);
        assertTrue(!userPre.getLastModified().equals(userPost.getLastModified()));
        assertEquals(userPost.getName(), newName);
        assertEquals(userPost.getEmail(), newEmail);
        assertEquals(null, userPost.getPassword());

        catalogManager.getUserManager().login("user", newPassword, "localhost");
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(userPost.getAttributes().get(entry.getKey()), entry.getValue());
        }

        catalogManager.changePassword("user", newPassword, PASSWORD);
        catalogManager.getUserManager().login("user", PASSWORD, "localhost");

        try {
            params = new ObjectMap();
            params.put("password", "1234321");
            catalogManager.modifyUser("user", params, sessionIdUser);
            fail("Expected exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        try {
            catalogManager.modifyUser("user", params, sessionIdUser2);
            fail("Expected exception");
        } catch (CatalogException e) {
            System.out.println(e);
        }

    }

    @Ignore
    @Test
    public void importLdapUsers() throws CatalogException, NamingException {
        // Action only for admins
        ObjectMap params = new ObjectMap()
                .append("users", "pfurio,imedina");

        LdapImportResult ldapImportResult = catalogManager.getUserManager().importFromExternalAuthOrigin("ldap", Account.GUEST, params,
                "admin");

        assertEquals(2, ldapImportResult.getResult().getUserSummary().getTotal());
    }

    // To make this test work we will need to add a correct user and password to be able to login
    @Ignore
    @Test
    public void loginNotRegisteredUsers() throws CatalogException, NamingException, IOException {
        // Action only for admins
        catalogManager.getStudyManager().createGroup(Long.toString(studyId), "ldap", "", sessionIdUser);
        catalogManager.getStudyManager().syncGroupWith(Long.toString(studyId), "ldap", new Group.Sync("ldap", "bio"), sessionIdUser);
        catalogManager.getStudyManager().updateAcl(Long.toString(studyId), "@ldap", new Study.StudyAclParams("",
                AclParams.Action.SET, "view_only"), sessionIdUser);
        QueryResult<Session> login = catalogManager.getUserManager().login("user", "password", "hh");

        QueryResult<Study> studyQueryResult = catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), login.first().getId());
        assertEquals(1, studyQueryResult.getNumResults());

        // We remove the permissions for group ldap
        catalogManager.getStudyManager().updateAcl(Long.toString(studyId), "@ldap", new Study.StudyAclParams("",
                AclParams.Action.RESET, ""), sessionIdUser);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), login.first().getId());
    }

    @Ignore
    @Test
    public void importLdapGroups() throws CatalogException, NamingException, IOException {
        // Action only for admins
        ObjectMap params = new ObjectMap()
                .append("group", "bio")
                .append("study", "user@1000G:phase1")
                .append("study-group", "test");
        catalogManager.getUserManager().importFromExternalAuthOrigin("ldap", Account.GUEST, params, "admin");

        QueryResult<Group> test = catalogManager.getStudyManager().getGroup("user@1000G:phase1", "test", sessionIdUser);
        assertEquals(1, test.getNumResults());
        assertEquals("@test", test.first().getName());
        assertTrue(test.first().getUserIds().size() > 0);

        params.put("study-group", "test1");
        try {
            catalogManager.getUserManager().importFromExternalAuthOrigin("ldap", Account.GUEST, params, "admin");
            fail("Should not be possible creating another group containing the same users that belong to a different group");
        } catch (CatalogException e) {
            System.out.println(e.getMessage());
        }

        params = new ObjectMap()
                .append("group", "bioo")
                .append("study", "user@1000G:phase1")
                .append("study-group", "test2");
        catalogManager.getUserManager().importFromExternalAuthOrigin("ldap", Account.GUEST, params, "admin");

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not exist");
        catalogManager.getStudyManager().getGroup("user@1000G:phase1", "test2", sessionIdUser);
    }

    /**
     * Project methods
     * ***************************
     */


    @Test
    public void testGetAllProjects() throws Exception {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        QueryResult<Project> projects = catalogManager.getProjectManager().get(query, null, sessionIdUser);
        assertEquals(1, projects.getNumResults());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("Permission denied");
        catalogManager.getProjectManager().get(query, null, sessionIdUser2);
    }

    @Test
    public void testCreateProject() throws Exception {

        String projectAlias = "projectAlias_ASDFASDF";

        catalogManager.getProjectManager().create("Project", projectAlias, "", "", "Homo sapiens", null, null, "GRCh38", new
                QueryOptions(), sessionIdUser);

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.getProjectManager().create("Project", projectAlias, "", "", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser);
    }

    @Test
    public void testModifyProject() throws CatalogException {
        String newProjectName = "ProjectName " + StringUtils.randomString(10);
        long projectId = catalogManager.getUser("user", null, sessionIdUser).first().getProjects().get(0).getId();

        ObjectMap options = new ObjectMap();
        options.put("name", newProjectName);
        ObjectMap attributes = new ObjectMap("myBoolean", true);
        attributes.put("value", 6);
        attributes.put("object", new BasicDBObject("id", 1234));
        options.put("attributes", attributes);

        catalogManager.modifyProject(projectId, options, sessionIdUser);
        QueryResult<Project> result = catalogManager.getProject(projectId, null, sessionIdUser);
        Project project = result.first();
        System.out.println(result);

        assertEquals(newProjectName, project.getName());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(project.getAttributes().get(entry.getKey()), entry.getValue());
        }

        options = new ObjectMap();
        options.put("alias", "newProjectAlias");
        catalogManager.modifyProject(projectId, options, sessionIdUser);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Permission denied");
        catalogManager.modifyProject(projectId, options, sessionIdUser2);

    }

    /**
     * Study methods
     * ***************************
     */

    @Test
    public void testModifyStudy() throws Exception {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();
        String newName = "Phase 1 " + StringUtils.randomString(20);
        String newDescription = StringUtils.randomString(500);

        ObjectMap parameters = new ObjectMap();
        parameters.put("name", newName);
        parameters.put("description", newDescription);
        BasicDBObject attributes = new BasicDBObject("key", "value");
        parameters.put("attributes", attributes);
        catalogManager.modifyStudy(studyId, parameters, sessionIdUser);

        QueryResult<Study> result = catalogManager.getStudy(studyId, sessionIdUser);
        System.out.println(result);
        Study study = result.first();
        assertEquals(study.getName(), newName);
        assertEquals(study.getDescription(), newDescription);
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(study.getAttributes().get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void testGetAllStudies() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        catalogManager.createStudy(projectId, "study_1", "study_1", Study.Type.CASE_CONTROL, "creationDate", "description",
                new Status(), null, null, null, null, null, null, null, sessionIdUser);
        catalogManager.createStudy(projectId, "study_2", "study_2", Study.Type.CASE_CONTROL, "creationDate", "description",
                new Status(), null, null, null, null, null, null, null, sessionIdUser);
        catalogManager.createStudy(projectId, "study_3", "study_3", Study.Type.CASE_CONTROL, "creationDate", "description",
                new Status(), null, null, null, null, null, null, null, sessionIdUser);
        long study_4 = catalogManager.createStudy(projectId, "study_4", "study_4", Study.Type.CASE_CONTROL, "creationDate",
                "description", new Status(), null, null, null, null, null, null, null, sessionIdUser).first().getId();

        assertEquals(new HashSet<>(Collections.emptyList()), catalogManager.getAllStudies(new Query(StudyDBAdaptor.QueryParams
                .GROUP_USER_IDS.key(), "user2"), null, sessionIdUser).getResult().stream().map(Study::getAlias)
                .collect(Collectors.toSet()));

        catalogManager.createGroup(Long.toString(study_4), "admins", "user3", sessionIdUser);
        assertEquals(new HashSet<>(Arrays.asList("study_4")), catalogManager.getAllStudies(new Query(StudyDBAdaptor.QueryParams
                .GROUP_USER_IDS.key(), "user3"), null, sessionIdUser).getResult().stream().map(Study::getAlias)
                .collect(Collectors.toSet()));

        assertEquals(new HashSet<>(Arrays.asList("phase1", "phase3", "study_1", "study_2", "study_3", "study_4")), catalogManager
                .getAllStudies(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, sessionIdUser)
                .getResult().stream().map(Study::getAlias).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("phase1", "phase3", "study_1", "study_2", "study_3", "study_4")), catalogManager
                .getAllStudies(new Query(), null, sessionIdUser).getResult().stream().map(Study::getAlias).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("study_1", "study_2", "study_3", "study_4")), catalogManager.getAllStudies(new
                Query(StudyDBAdaptor.QueryParams.ALIAS.key(), "~^study"), null, sessionIdUser).getResult().stream()
                .map(Study::getAlias).collect(Collectors.toSet()));
        assertEquals(Collections.singleton("s1"), catalogManager.getAllStudies(new Query(), null, sessionIdUser2).getResult().stream()
                .map(Study::getAlias).collect(Collectors.toSet()));
    }

    @Test
    public void testGetId() throws CatalogException {
        // Create another study with alias phase3
        catalogManager.createStudy(project2, "Phase 3", "phase3", Study.Type.CASE_CONTROL, "d", sessionIdUser2);

        String userId = catalogManager.getUserManager().getId(sessionIdUser);
        List<Long> ids = catalogManager.getStudyManager().getIds(userId, "*");
        assertTrue(ids.contains(studyId) && ids.contains(studyId2));

        ids = catalogManager.getStudyManager().getIds(userId, "");
        assertTrue(ids.contains(studyId) && ids.contains(studyId2));

        ids = catalogManager.getStudyManager().getIds(userId, null);
        assertTrue(ids.contains(studyId) && ids.contains(studyId2));

        ids = catalogManager.getStudyManager().getIds(userId, "1000G:*");
        assertTrue(ids.contains(studyId) && ids.contains(studyId2));

        ids = catalogManager.getStudyManager().getIds(userId, userId + "@1000G:*");
        assertTrue(ids.contains(studyId) && ids.contains(studyId2));

        ids = catalogManager.getStudyManager().getIds(userId, userId + "@1000G:phase1,phase3");
        assertTrue(ids.contains(studyId) && ids.contains(studyId2));

        ids = catalogManager.getStudyManager().getIds(userId, userId + "@1000G:phase3," + Long.toString(studyId));
        assertTrue(ids.contains(studyId) && ids.contains(studyId2));

        try {
            catalogManager.getStudyManager().getId(userId, null);
            fail("This method should fail because it should find several studies");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("More than one study"));
        }

        long id = catalogManager.getStudyManager().getId(userId, "phase3");
        assertEquals(studyId2, id);
    }

    @Test
    public void testGetOnlyStudyUserAnonymousCanSee() throws CatalogException {
        IStudyManager studyManager = catalogManager.getStudyManager();

        try {
            studyManager.getIds("*", null);
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        // Create another study with alias phase3
        QueryResult<Study> study = catalogManager.createStudy(project2, "Phase 3", "phase3", Study.Type.CASE_CONTROL, "d", sessionIdUser2);
        try {
            studyManager.getIds("*", null);
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        catalogManager.createStudyAcls("phase3", "*", "VIEW_STUDY", null, sessionIdUser2);

        List<Long> ids = studyManager.getIds("*", null);
        assertEquals(1, ids.size());
        assertEquals(study.first().getId(), (long) ids.get(0));
    }

    @Test
    public void testGetSelectedStudyUserAnonymousCanSee() throws CatalogException {
        IStudyManager studyManager = catalogManager.getStudyManager();

        try {
            studyManager.getIds("*", "phase3");
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        // Create another study with alias phase3
        QueryResult<Study> study = catalogManager.createStudy(project2, "Phase 3", "phase3", Study.Type.CASE_CONTROL, "d", sessionIdUser2);
        catalogManager.createStudyAcls("phase3", "*", "VIEW_STUDY", null, sessionIdUser2);

        List<Long> ids = studyManager.getIds("*", "phase3");
        assertEquals(1, ids.size());
        assertEquals(study.first().getId(), (long) ids.get(0));
    }

    @Test
    public void testUpdateGroupInfo() throws CatalogException {
        IStudyManager studyManager = catalogManager.getStudyManager();

        studyManager.createGroup(Long.toString(studyId), "group1", "", sessionIdUser);
        studyManager.createGroup(Long.toString(studyId), "group2", "", sessionIdUser);

        Group.Sync syncFrom = new Group.Sync("auth", "aaa");
        studyManager.syncGroupWith(Long.toString(studyId), "group2", syncFrom, sessionIdUser);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Cannot modify already existing sync information");
        studyManager.syncGroupWith(Long.toString(studyId), "group2", syncFrom, sessionIdUser);
    }


    @Test
    public void removeAllPermissionsToMember() throws CatalogException {
        IStudyManager studyManager = catalogManager.getStudyManager();

        // Assign permissions to study
        Study.StudyAclParams studyAclParams = new Study.StudyAclParams("VIEW_STUDY", AclParams.Action.SET, null);
        List<QueryResult<StudyAclEntry>> queryResults = studyManager.updateAcl(Long.toString(studyId), "user2,user3", studyAclParams,
                sessionIdUser);
        assertEquals(Long.toString(studyId), queryResults.get(0).getId());
        assertEquals(2, queryResults.get(0).getNumResults());
        for (StudyAclEntry studyAclEntry : queryResults.get(0).getResult()) {
            assertTrue(studyAclEntry.getPermissions().contains(StudyAclEntry.StudyPermissions.VIEW_STUDY));
            assertTrue(Arrays.asList("user2", "user3").contains(studyAclEntry.getMember()));
        }

        // Obtain all samples from study
        Query query = new Query(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(query, QueryOptions.empty(), sessionIdUser);
        assertTrue(sampleQueryResult.getNumResults() > 0);

        // Assign permissions to all the samples
        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams("VIEW,UPDATE", AclParams.Action.SET, null, null, null);
        List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
        List<QueryResult<SampleAclEntry>> sampleAclResult = catalogManager.getSampleManager().updateAcl(
                org.apache.commons.lang3.StringUtils.join(sampleIds, ","), Long.toString(studyId), "user2,user3",
                sampleAclParams, sessionIdUser);
        assertEquals(sampleIds.size(), sampleAclResult.size());
        for (QueryResult<SampleAclEntry> sampleAclEntryQueryResult : sampleAclResult) {
            assertEquals(2, sampleAclEntryQueryResult.getNumResults());
            for (SampleAclEntry sampleAclEntry : sampleAclEntryQueryResult.getResult()) {
                assertTrue(sampleAclEntry.getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW));
                assertTrue(sampleAclEntry.getPermissions().contains(SampleAclEntry.SamplePermissions.UPDATE));
                assertTrue(Arrays.asList("user2", "user3").contains(sampleAclEntry.getMember()));
            }
        }

        // Remove all the permissions to both users in the study. That should also remove the permissions they had in all the samples.
        studyAclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        queryResults = studyManager.updateAcl(Long.toString(studyId), "user2,user3", studyAclParams, sessionIdUser);
        assertEquals(0, queryResults.get(0).getNumResults());

        // Get sample permissions for those members
        for (Long sampleId : sampleIds) {
            QueryResult<SampleAclEntry> sampleAcl = catalogManager.getAuthorizationManager().getSampleAcl("user", sampleId, "user2");
            assertEquals(0, sampleAcl.getNumResults());
            sampleAcl = catalogManager.getAuthorizationManager().getSampleAcl("user", sampleId, "user3");
            assertEquals(0, sampleAcl.getNumResults());
        }
    }

    @Test
    public void removeUsersFromStudies() throws CatalogException {
        IStudyManager studyManager = catalogManager.getStudyManager();

        // Assign permissions to study
        Study.StudyAclParams studyAclParams = new Study.StudyAclParams("VIEW_STUDY", AclParams.Action.SET, null);
        List<QueryResult<StudyAclEntry>> queryResults = studyManager.updateAcl(Long.toString(studyId), "user2,user3", studyAclParams,
                sessionIdUser);
        assertEquals(Long.toString(studyId), queryResults.get(0).getId());
        assertEquals(2, queryResults.get(0).getNumResults());
        for (StudyAclEntry studyAclEntry : queryResults.get(0).getResult()) {
            assertTrue(studyAclEntry.getPermissions().contains(StudyAclEntry.StudyPermissions.VIEW_STUDY));
            assertTrue(Arrays.asList("user2", "user3").contains(studyAclEntry.getMember()));
        }

        // Obtain all samples from study
        Query query = new Query(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(query, QueryOptions.empty(), sessionIdUser);
        assertTrue(sampleQueryResult.getNumResults() > 0);

        // Assign permissions to all the samples
        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams("VIEW,UPDATE", AclParams.Action.SET, null, null, null);
        List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
        List<QueryResult<SampleAclEntry>> sampleAclResult = catalogManager.getSampleManager().updateAcl(
                org.apache.commons.lang3.StringUtils.join(sampleIds, ","), Long.toString(studyId), "user2,user3",
                sampleAclParams, sessionIdUser);
        assertEquals(sampleIds.size(), sampleAclResult.size());
        for (QueryResult<SampleAclEntry> sampleAclEntryQueryResult : sampleAclResult) {
            assertEquals(2, sampleAclEntryQueryResult.getNumResults());
            for (SampleAclEntry sampleAclEntry : sampleAclEntryQueryResult.getResult()) {
                assertTrue(sampleAclEntry.getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW));
                assertTrue(sampleAclEntry.getPermissions().contains(SampleAclEntry.SamplePermissions.UPDATE));
                assertTrue(Arrays.asList("user2", "user3").contains(sampleAclEntry.getMember()));
            }
        }

        catalogManager.getStudyManager().updateGroup(Long.toString(studyId), "@members",
                new GroupParams("user2,user3", GroupParams.Action.REMOVE), sessionIdUser);

        QueryResult<StudyAclEntry> studyAcl = catalogManager.getStudyAcl(Long.toString(studyId), "user2", sessionIdUser);
        assertEquals(0, studyAcl.getNumResults());
        studyAcl = catalogManager.getStudyAcl(Long.toString(studyId), "user3", sessionIdUser);
        assertEquals(0, studyAcl.getNumResults());

        QueryResult<Group> groupQueryResult = catalogManager.getStudyManager().getGroup(Long.toString(studyId), null, sessionIdUser);
        for (Group group : groupQueryResult.getResult()) {
            assertTrue(!group.getUserIds().contains("user2"));
            assertTrue(!group.getUserIds().contains("user3"));
        }

        for (Long sampleId : sampleIds) {
            QueryResult<SampleAclEntry> sampleAcl = catalogManager.getAuthorizationManager().getSampleAcl("user", sampleId, "user2");
            assertEquals(0, sampleAcl.getNumResults());
            sampleAcl = catalogManager.getAuthorizationManager().getSampleAcl("user", sampleId, "user3");
            assertEquals(0, sampleAcl.getNumResults());
        }
    }

    /**
     * Job methods
     * ***************************
     */

    @Test
    public void testCreateJob() throws CatalogException, IOException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        File outDir = catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("jobs", "myJob").toString(),
                null, true, null, QueryOptions.empty(), sessionIdUser).first();

        URI tmpJobOutDir = catalogManager.createJobOutDir(studyId, StringUtils.randomString(5), sessionIdUser);
        catalogManager.createJob(
                studyId, "myJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir, outDir
                        .getId(),
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.PREPARED), 0, 0, null, sessionIdUser);
//                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.PREPARED), 0, 0, null, sessionIdUser);

        catalogManager.createJob(
                studyId, "myReadyJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir,
                outDir.getId(),
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.READY), 0, 0, null, sessionIdUser);

        catalogManager.createJob(
                studyId, "myQueuedJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir,
                outDir.getId(),
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.QUEUED), 0, 0, null, sessionIdUser);

        catalogManager.createJob(
                studyId, "myErrorJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir,
                outDir.getId(),
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), 0, 0, null, sessionIdUser);

        query = new Query()
                .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Arrays.asList(Job.JobStatus.PREPARED, Job.JobStatus.QUEUED,
                        Job.JobStatus.RUNNING, Job.JobStatus.DONE))
                .append(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Job> unfinishedJobs = catalogManager.getJobManager().get(query, null, sessionIdUser);
        assertEquals(2, unfinishedJobs.getNumResults());

        QueryResult<Job> allJobs = catalogManager.getAllJobs(studyId, sessionIdUser);
        assertEquals(4, allJobs.getNumResults());
    }

    @Test
    public void testCreateFailJob() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        URI tmpJobOutDir = catalogManager.createJobOutDir(studyId, StringUtils.randomString(5), sessionIdUser);
        thrown.expect(CatalogException.class);
        catalogManager.createJob(
                studyId, "myErrorJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir,
                projectId, //Bad outputId
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), 0, 0, null, sessionIdUser);
    }

    @Test
    public void testGetAllJobs() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();
        File outDir = catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("jobs", "myJob").toString(),
                null, true, null, QueryOptions.empty(), sessionIdUser).first();

        URI tmpJobOutDir = catalogManager.createJobOutDir(studyId, StringUtils.randomString(5), sessionIdUser);
        catalogManager.createJob(
                studyId, "myErrorJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir,
                outDir.getId(),
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), 0, 0, null, sessionIdUser);

        QueryResult<Job> allJobs = catalogManager.getAllJobs(studyId, sessionIdUser);

        assertEquals(1, allJobs.getNumTotalResults());
        assertEquals(1, allJobs.getNumResults());
    }

    /**
     * VariableSet methods
     * ***************************
     */

    @Test
    public void testCreateVariableSet() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long variableSetNum = study.getVariableSets().size();

        Set<Variable> variables = new HashSet<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
                        null, Collections.<String, Object>emptyMap())
        ));
        QueryResult<VariableSet> queryResult = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", true, false, "", null,
                variables, sessionIdUser);

        assertEquals(1, queryResult.getResult().size());

        study = catalogManager.getStudy(study.getId(), sessionIdUser).first();
        assertEquals(variableSetNum + 1, study.getVariableSets().size());
    }

    @Test
    public void testCreateRepeatedVariableSet() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("NAME", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        thrown.expect(CatalogException.class);
        catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", true, false, "", null, variables, sessionIdUser);
    }

    @Test
    public void testDeleteVariableSet() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyId, "vs1", true, false, "", null, variables, sessionIdUser).first();

        VariableSet vs1_deleted = catalogManager.getStudyManager().deleteVariableSet(Long.toString(studyId), Long.toString(vs1.getId()),
                sessionIdUser).first();

        assertEquals(vs1.getId(), vs1_deleted.getId());

        thrown.expect(CatalogException.class);    //VariableSet does not exist
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().getVariableSet(Long.toString(studyId), vs1.getName(), null, sessionIdUser);
    }

    @Test
    public void testGetAllVariableSet() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyId, "vs1", true, false, "Cancer", null, variables, sessionIdUser).first();

        VariableSet vs2 = catalogManager.getStudyManager().createVariableSet(studyId, "vs2", true, false, "Virgo", null, variables, sessionIdUser).first();
        VariableSet vs3 = catalogManager.getStudyManager().createVariableSet(studyId, "vs3", true, false, "Piscis", null, variables, sessionIdUser).first();
        VariableSet vs4 = catalogManager.getStudyManager().createVariableSet(studyId, "vs4", true, false, "Aries", null, variables, sessionIdUser).first();

        long numResults;
        numResults = catalogManager.getStudyManager().searchVariableSets(Long.toString(studyId),
                new Query(StudyDBAdaptor.VariableSetParams.NAME.key(), "vs1"), QueryOptions.empty(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getStudyManager().searchVariableSets(Long.toString(studyId),
                new Query(StudyDBAdaptor.VariableSetParams.NAME.key(), "vs1,vs2"), QueryOptions.empty(), sessionIdUser)
                .getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getStudyManager().searchVariableSets(Long.toString(studyId),
                new Query(StudyDBAdaptor.VariableSetParams.NAME.key(), "VS1"), QueryOptions.empty(), sessionIdUser).getNumResults();
        assertEquals(0, numResults);

        numResults = catalogManager.getStudyManager().searchVariableSets(Long.toString(studyId),
                new Query(StudyDBAdaptor.VariableSetParams.ID.key(), vs1.getName()), QueryOptions.empty(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);
//        numResults = catalogManager.getStudyManager().searchVariableSets(Long.toString(studyId),
//                new Query(StudyDBAdaptor.VariableSetParams.ID.key(), vs1.getId() + "," + vs3.getId()), QueryOptions.empty(),
//                sessionIdUser).getNumResults();

        numResults = catalogManager.getStudyManager().searchVariableSets(Long.toString(studyId),
                new Query(StudyDBAdaptor.VariableSetParams.ID.key(), vs3.getName()), QueryOptions.empty(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);
    }

    @Test
    public void testDeleteVariableSetInUse() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        long sampleId1 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_1", "", "", null, false, null, null,
                new QueryOptions(), sessionIdUser).first().getId();
        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, false, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyId, "vs1", true, false, "", null, variables, sessionIdUser).first();

        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId1), null, Long.toString(vs1.getId()), "annotationId", Collections.singletonMap("NAME", "LINUS"), null, sessionIdUser);

        try {
            catalogManager.getStudyManager().deleteVariableSet(Long.toString(studyId), Long.toString(vs1.getId()), sessionIdUser).first();
        } finally {
            VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(Long.toString(studyId), vs1.getName(), null,
                    sessionIdUser).first();
            assertEquals(vs1.getId(), variableSet.getId());

            thrown.expect(CatalogDBException.class); //Expect the exception from the try
        }
    }

    /**
     * Sample methods
     * ***************************
     */

    @Test
    public void testCreateSample() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().create(Long.toString(studyId), "HG007", "IMDb", "",
                null, false, null, null, null, sessionIdUser);
        System.out.println("sampleQueryResult = " + sampleQueryResult);
    }

    @Test
    public void testCreateSampleWithDotInName() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        String name = "HG007.sample";
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().create(Long.toString(studyId), name, "IMDb", "", null,
                false, null, null, null, sessionIdUser);

        assertEquals(name, sampleQueryResult.first().getName());
    }

    @Test
    public void testAnnotate() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        variables.add(new Variable("AGE", "", Variable.VariableType.INTEGER, "", false, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        variables.add(new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "", false, false, Collections.<String>emptyList(), 0, "",
                "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Joe");
        annotations.put("AGE", 25);
        annotations.put("HEIGHT", 180);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_1), Long.toString(studyId), vs1.getName(),"annotation1",
                annotations, null, sessionIdUser);

        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().getAnnotationSet(Long.toString(s_1),
                Long.toString(studyId), "annotation1", sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());
        Map<String, Object> map = annotationSetQueryResult.first().getAnnotations().stream().collect(Collectors.toMap(Annotation::getName,
                Annotation::getValue));
        assertEquals(3, map.size());
        assertEquals("Joe", map.get("NAME"));
        assertEquals(25, map.get("AGE"));
        assertEquals(180.0, map.get("HEIGHT"));
    }

    @Test
    public void searchSamples() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        catalogManager.getStudyManager().createGroup(Long.toString(studyId), "myGroup", "user2,user3", sessionIdUser);
        catalogManager.getStudyManager().createGroup(Long.toString(studyId), "myGroup2", "user2,user3", sessionIdUser);
        catalogManager.getStudyManager().updateAcl(Long.toString(studyId), "@myGroup",
                new Study.StudyAclParams("", AclParams.Action.SET, null), sessionIdUser);
        catalogManager.getSampleManager().updateAcl("s_1", Long.toString(studyId), "@myGroup",
                new Sample.SampleAclParams("VIEW", AclParams.Action.SET, null, null, null), sessionIdUser);

        QueryResult<Sample> search = catalogManager.getSampleManager().search(Long.toString(studyId), new Query(), new QueryOptions(),
                sessionIdUser2);
        assertEquals(1, search.getNumResults());
    }

    @Test
    public void testSearchAnnotation() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("var_name", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0,
                "",
                "",
                null, Collections.<String, Object>emptyMap()));
        variables.add(new Variable("AGE", "", Variable.VariableType.INTEGER, "", false, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        variables.add(new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "", false, false, Collections.<String>emptyList(), 0, "",
                "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("var_name", "Joe");
        annotations.put("AGE", 25);
        annotations.put("HEIGHT", 180);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_1), Long.toString(studyId), vs1.getName(),"annotation1",
                annotations, null, sessionIdUser);

        QueryResult<AnnotationSet> annotQueryResult = catalogManager.getSampleManager().searchAnnotationSet(Long.toString(s_1),
                Long.toString(studyId), vs1.getName(), "var_name=Joe;AGE=25", sessionIdUser);
        assertEquals(1, annotQueryResult.getNumResults());

        annotQueryResult = catalogManager.getSampleManager().searchAnnotationSet(Long.toString(s_1),
                Long.toString(studyId), vs1.getName(), "var_name=Joe;AGE=23", sessionIdUser);
        assertEquals(0, annotQueryResult.getNumResults());
    }

    @Test
    public void testAnnotateMulti() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null,
                null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString
        (sampleId), null, Long.toString(vs1.getId()), "annotation1", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations = new HashMap<>();
        annotations.put("NAME", "Lucas");
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation2", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        assertEquals(2, catalogManager.getSample(sampleId, null, sessionIdUser).first().getAnnotationSets().size());
    }

    @Test
    public void testAnnotateUnique() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null,
                null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", true, false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString
        (sampleId), null, Long.toString(vs1.getId()), "annotation1", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NAME", "Lucas");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("unique");
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation2", annotations, null, sessionIdUser);
    }

    @Test
    public void testAnnotateIndividualUnique() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long individualId = catalogManager.createIndividual(study.getId(), "INDIVIDUAL_1", "", -1, -1, Individual.Sex.UNKNOWN, new
                QueryOptions(), sessionIdUser).first().getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", true, false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getIndividualManager().createAnnotationSet(
                Long.toString(individualId), null, Long.toString(vs1.getId()), "annotation1", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NAME", "Lucas");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("unique");
        catalogManager.getIndividualManager().createAnnotationSet(Long.toString(individualId), null, Long.toString(vs1.getId()),
                "annotation2", annotations, null, sessionIdUser);
    }

    @Test
    public void testAnnotateIncorrectType() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null,
                null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NUM", "", Variable.VariableType.DOUBLE, "", true, false, null, 0, "", "", null, Collections.<String,
                Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NUM", "5");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString
        (sampleId), null, Long.toString(vs1.getId()), "annotation1", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NUM", "6.8");
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString
        (vs1.getId()), "annotation2", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NUM", "five polong five");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation3", annotations, null, sessionIdUser);
    }

    @Test
    public void testAnnotateRange() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("RANGE_NUM", "", Variable.VariableType.DOUBLE, "", true, false, Arrays.asList("1:14", "16:22", "50:")
                , 0, "", "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("RANGE_NUM", "1");  // 1:14
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation1", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "14"); // 1:14
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation2", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "20");  // 16:20
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation3", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "100000"); // 50:
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation4", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "14.1");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation5", annotations, null, sessionIdUser);
    }

    @Test
    public void testAnnotateCategorical() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("COOL_NAME", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("LUKE", "LEIA",
                "VADER", "YODA"), 0, "", "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("COOL_NAME", "LUKE");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation1", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "LEIA");
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation2", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "VADER");
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString
        (vs1.getId()), "annotation3", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "YODA");
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString
        (vs1.getId()), "annotation4", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "SPOCK");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation5", annotations, null, sessionIdUser);
    }

    @Test
    public void testAnnotateNested() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId1 = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first()
                .getId();
        long sampleId2 = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_2", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first()
                .getId();
        long sampleId3 = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_3", "", "", null, false, null,
                null, new QueryOptions(), sessionIdUser).first()
                .getId();
        long sampleId4 = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_4", "", "", null, false, null,
                null, new QueryOptions(), sessionIdUser).first()
                .getId();
        long sampleId5 = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_5", "", "", null, false, null,
                null, new QueryOptions(), sessionIdUser).first()
                .getId();

        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, Collections
                .singleton
                (CatalogAnnotationsValidatorTest.nestedObject), sessionIdUser).first();

        QueryResult<AnnotationSet> annotationSetQueryResult;
        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("nestedObject", new QueryOptions("stringList", Arrays.asList("li", "lu")).append("object", new ObjectMap
                ("string", "my value").append("numberList", Arrays.asList(2, 3, 4))));
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId1), null, Long.toString(vs1.getId()), "annotation1", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("nestedObject", new QueryOptions("stringList", Arrays.asList("lo", "lu")).append("object", new ObjectMap
                ("string", "stringValue").append("numberList", Arrays.asList(3, 4, 5))));
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId2), null, Long.toString(vs1.getId()), "annotation1", annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

//        annotations.put("nestedObject", new QueryOptions("stringList", Arrays.asList("li", "lo", "lu")).append("object", new ObjectMap
// ("string", "my value").append("numberList", Arrays.asList(2, 3, 4))));
//        annotationSetQueryResult = catalogManager.annotateSample(sampleId3, "annotation1", vs1.getId(), annotations, null, sessionIdUser);
//        assertEquals(1, annotationSetQueryResult.getNumResults());
//
//        annotations.put("nestedObject", new QueryOptions("stringList", Arrays.asList("li", "lo", "lu")).append("object", new ObjectMap
// ("string", "my value").append("numberList", Arrays.asList(2, 3, 4))));
//        annotationSetQueryResult = catalogManager.annotateSample(sampleId4, "annotation1", vs1.getId(), annotations, null, sessionIdUser);
//        assertEquals(1, annotationSetQueryResult.getNumResults());

        List<Sample> samples;
        Query query = new Query(SampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), vs1.getId());
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "li");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        //query = new Query(CatalogSampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), vs1.getId());
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "LL");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,li,LL");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.string", "my value");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.string", "my value");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList" , "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.numberList", "7");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.numberList", "3");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.numberList" , "5");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.string", "stringValue");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.remove(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.string");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.numberList", "2,5");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.numberList", "0");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());


        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".unexisting", "lo,lu,LL");
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found in variableSet");
        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
    }

//    @Test
//    public void testQuerySampleAnnotationFail1() throws CatalogException {
//        Query query = new Query();
//        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ":nestedObject.stringList", "lo,lu,LL");
//
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("annotation:nestedObject does not exist");
//        QueryResult<Sample> search = catalogManager.getSampleManager().search(Long.toString(studyId), query, null, sessionIdUser);
//        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
//    }

//    @Test
//    public void testQuerySampleAnnotationFail2() throws CatalogException {
//        Query query = new Query();
//        query.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key(), "nestedObject.stringList:lo,lu,LL");
//
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("Wrong annotation query");
//        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
//    }

    @Test
    public void testIteratorSamples() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");

        Query query = new Query();

        DBIterator<Sample> iterator = catalogManager.getSampleManager().iterator(studyId, query, null, sessionIdUser);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertEquals(9, count);
    }

    @Test
    public void testQuerySamples() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();

        VariableSet variableSet = study.getVariableSets().get(0);

        List<Sample> samples;
        Query query = new Query();

        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(9, samples.size());

        query = new Query(VARIABLE_SET_ID.key(), variableSet.getId());
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(8, samples.size());

        query = new Query(ANNOTATION_SET_NAME.key(), "annot2");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION_SET_NAME.key(), "noExist");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query = new Query("annotation.NAME", "s_1,s_2,s_3");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query("annotation.AGE", ">30");
        query.append(VARIABLE_SET_ID.key(), variableSet.getId());
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query("annotation.AGE", ">30");
        query.append(VARIABLE_SET_ID.key(), variableSet.getId());
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query("annotation.AGE", ">30").append("annotation.ALIVE", "true");
        query.append(VARIABLE_SET_ID.key(), variableSet.getId());
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());
    }

    @Test
    public void testUpdateAnnotation() throws CatalogException {

        long studyId = catalogManager.getStudyId("user@1000G:phase1", sessionIdUser);
        Study study = catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), sessionIdUser).first();
        Individual ind = new Individual().setName("INDIVIDUAL_1").setSex(Individual.Sex.UNKNOWN);
        ind = catalogManager.getIndividualManager().create(Long.toString(study.getId()), ind, QueryOptions.empty(), sessionIdUser).first();
        Sample sample = catalogManager.getSample(s_1, null, sessionIdUser).first();

        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);
        catalogManager.getIndividualManager().createAnnotationSet(Long.toString(ind.getId()), Long.toString(studyId),
                Long.toString(annotationSet.getVariableSetId()), annotationSet.getName(),
                annotationSet.getAnnotations().stream().collect(Collectors.toMap(Annotation::getName, Annotation::getValue)),
                Collections.emptyMap(), sessionIdUser);

        // First update
        ObjectMap updateAnnotation = new ObjectMap("NAME", "SAMPLE1")
                .append("AGE", 38)
                .append("HEIGHT", null)
                .append("EXTRA", "extra");
        catalogManager.getIndividualManager().updateAnnotationSet(Long.toString(ind.getId()), Long.toString(studyId),
                annotationSet.getName(), updateAnnotation, sessionIdUser);
        catalogManager.getSampleManager().updateAnnotationSet(Long.toString(s_1), Long.toString(studyId),
                annotationSet.getName(), updateAnnotation, sessionIdUser);

        Consumer<AnnotationSet> check = as -> {
            Map<String, Object> annotations = as.getAnnotations().stream()
                    .collect(Collectors.toMap(Annotation::getName, Annotation::getValue));

            assertEquals(6, annotations.size());
            assertEquals("SAMPLE1", annotations.get("NAME"));
            assertEquals(38.0, annotations.get("AGE"));
            assertEquals(1.5, annotations.get("HEIGHT"));   //Default value
            assertEquals("extra", annotations.get("EXTRA"));
        };

        sample = catalogManager.getSample(s_1, null, sessionIdUser).first();
        ind = catalogManager.getIndividual(ind.getId(), null, sessionIdUser).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));

        // Call again to the update to check that nothing changed
        catalogManager.getIndividualManager().updateAnnotationSet(Long.toString(ind.getId()), Long.toString(studyId),
                annotationSet.getName(), updateAnnotation, sessionIdUser);
        check.accept(ind.getAnnotationSets().get(0));

        updateAnnotation = new ObjectMap("NAME", "SAMPLE 1").append("EXTRA", null);
        catalogManager.getIndividualManager().updateAnnotationSet(Long.toString(ind.getId()), Long.toString(studyId),
                annotationSet.getName(), updateAnnotation, sessionIdUser);
        catalogManager.getSampleManager().updateAnnotationSet(Long.toString(s_1), Long.toString(studyId),
                annotationSet.getName(), updateAnnotation, sessionIdUser);

        check = as -> {
            Map<String, Object> annotations = as.getAnnotations().stream()
                    .collect(Collectors.toMap(Annotation::getName, Annotation::getValue));

            assertEquals(5, annotations.size());
            assertEquals("SAMPLE 1", annotations.get("NAME"));
            assertEquals(false, annotations.containsKey("EXTRA"));
        };

        sample = catalogManager.getSample(s_1, null, sessionIdUser).first();
        ind = catalogManager.getIndividual(ind.getId(), null, sessionIdUser).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));
    }

    @Test
    public void testUpdateAnnotationFail() throws CatalogException {

        Sample sample = catalogManager.getSample(s_1, null, sessionIdUser).first();
        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);

        thrown.expect(CatalogException.class); //Can not delete required fields
        catalogManager.getSampleManager().updateAnnotationSet(Long.toString(s_1), null, annotationSet.getName(), new ObjectMap("NAME", null), sessionIdUser);

    }

    @Test
    public void getVariableSetSummary() throws CatalogException {

        long studyId = catalogManager.getStudyId("user@1000G:phase1", sessionIdUser);
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();

        long variableSetId = study.getVariableSets().get(0).getId();

        QueryResult<VariableSetSummary> variableSetSummary = catalogManager.getStudyManager()
                .getVariableSetSummary(Long.toString(studyId), Long.toString(variableSetId), sessionIdUser);

        assertEquals(1, variableSetSummary.getNumResults());
        VariableSetSummary summary = variableSetSummary.first();

        assertEquals(5, summary.getSamples().size());

        // PHEN
        int i;
        for (i = 0; i < summary.getSamples().size(); i++) {
            if ("PHEN".equals(summary.getSamples().get(i).getName())) {
                break;
            }
        }
        List<FeatureCount> annotations = summary.getSamples().get(i).getAnnotations();
        assertEquals("PHEN", summary.getSamples().get(i).getName());
        assertEquals(2, annotations.size());

        for (i = 0; i < annotations.size(); i++) {
            if ("CONTROL".equals(annotations.get(i).getName())) {
                break;
            }
        }
        assertEquals("CONTROL", annotations.get(i).getName());
        assertEquals(5, annotations.get(i).getCount());

        for (i = 0; i < annotations.size(); i++) {
            if ("CASE".equals(annotations.get(i).getName())) {
                break;
            }
        }
        assertEquals("CASE", annotations.get(i).getName());
        assertEquals(3, annotations.get(i).getCount());

    }

    @Test
    public void testModifySample() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1", sessionIdUser);
        long sampleId1 = catalogManager.getSampleManager()
                .create("user@1000G:phase1", "SAMPLE_1", "", "", null, false, null, null, new QueryOptions(),
                        sessionIdUser).first().getId();
        long individualId = catalogManager.createIndividual(studyId, "Individual1", "", 0, 0, Individual.Sex.MALE, new QueryOptions(),
                sessionIdUser).first().getId();

        Sample sample = catalogManager.getSampleManager()
                .update(sampleId1, new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), individualId), null, sessionIdUser)
                .first();

        assertEquals(individualId, sample.getIndividual().getId());
    }

    @Test
    public void getSharedProject() throws CatalogException, IOException {
        catalogManager.getUserManager().create("dummy", "dummy", "asd@asd.asd", "dummy", "", 50000L,
                Account.GUEST, QueryOptions.empty());
        catalogManager.getStudyManager().updateAcl("user@1000G:phase1", "dummy",
                new Study.StudyAclParams(StudyAclEntry.StudyPermissions.VIEW_STUDY.name(), AclParams.Action.SET, ""), sessionIdUser);

        QueryResult<Session> login = catalogManager.getUserManager().login("dummy", "dummy", "oo");
        QueryResult<Project> queryResult = catalogManager.getProjectManager().getSharedProjects("dummy", QueryOptions.empty(),
                login.first().getId());
        assertEquals(1, queryResult.getNumResults());

        catalogManager.getStudyManager().updateAcl("user@1000G:phase1", "*",
                new Study.StudyAclParams(StudyAclEntry.StudyPermissions.VIEW_STUDY.name(), AclParams.Action.SET, ""), sessionIdUser);
        queryResult = catalogManager.getProjectManager().getSharedProjects("*", QueryOptions.empty(), null);
        assertEquals(1, queryResult.getNumResults());
    }

    @Test
    public void smartResolutorStudyAliasFromAnonymousUser() throws CatalogException {
        catalogManager.getStudyManager().updateAcl("user@1000G:phase1", "*",
                new Study.StudyAclParams(StudyAclEntry.StudyPermissions.VIEW_STUDY.name(), AclParams.Action.SET, ""), sessionIdUser);
        long id = catalogManager.getStudyManager().getId("*", "1000G:phase1");
        assertTrue(id > 0);
    }

    @Test
    public void testCreateSampleWithIndividual() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1", sessionIdUser);
        long individualId = catalogManager.createIndividual(studyId, "Individual1", "", 0, 0, Individual.Sex.MALE, new QueryOptions(),
                sessionIdUser).first().getId();
        long sampleId1 = catalogManager.getSampleManager()
                .create("user@1000G:phase1", "SAMPLE_1", "", "", null, false, new Individual().setId(individualId), null, new QueryOptions(),
                        sessionIdUser).first().getId();
        Sample sample = catalogManager.getSampleManager().get(sampleId1, QueryOptions.empty(), sessionIdUser).first();

        assertEquals(individualId, sample.getIndividual().getId());

        // Create sample linking to individual based on the individual name
        long sampleId2 = catalogManager.getSampleManager()
                .create("user@1000G:phase1", "SAMPLE_2", "", "", null, false, new Individual().setName("Individual1"), null, new QueryOptions(),
                        sessionIdUser).first().getId();
        sample = catalogManager.getSampleManager().get(sampleId2, QueryOptions.empty(), sessionIdUser).first();
        assertEquals(individualId, sample.getIndividual().getId());
    }

    @Test
    public void testModifySampleBadIndividual() throws CatalogException {
        long sampleId1 = catalogManager.getSampleManager().create("user@1000G:phase1", "SAMPLE_1", "", "", null, new QueryOptions(),
                sessionIdUser).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogManager.getSampleManager()
                .update(sampleId1, new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), 4), null, sessionIdUser);
    }

    @Test
    public void testModifySampleUnknownIndividual() throws CatalogException {
        long sampleId1 = catalogManager.getSampleManager().create("user@1000G:phase1", "SAMPLE_1", "", "", null, new QueryOptions(),
                sessionIdUser).first().getId();

        catalogManager.getSampleManager()
                .update(sampleId1, new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), -1), null, sessionIdUser);

        Sample sample = catalogManager.getSampleManager()
                .update(sampleId1, new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), -2), null, sessionIdUser).first();
        assertEquals(-1, sample.getIndividual().getId());
    }

    @Test
    public void testDeleteSample() throws CatalogException, IOException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        long sampleId = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_1", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first().getId();

        List<QueryResult<Sample>> queryResult = catalogManager.getSampleManager()
                .delete(Long.toString(sampleId), Long.toString(studyId), new QueryOptions(), sessionIdUser);
        assertEquals(sampleId, queryResult.get(0).first().getId());

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), sampleId)
                .append(SampleDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED);

        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyId, query, new QueryOptions(), sessionIdUser);
//        QueryResult<Sample> sample = catalogManager.getSample(sampleId, new QueryOptions(), sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertTrue(sampleQueryResult.first().getName().contains(".DELETED"));
    }

    /*
     * Cohort methods
     *
     */


    @Test
    public void testCreateCohort() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        Sample sampleId1 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_1", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_2", "", "", null, false, null, null,
                new QueryOptions(), sessionIdUser).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_3", "", "", null, false, null, null,
                new QueryOptions(), sessionIdUser).first();
        Cohort myCohort = catalogManager.getCohortManager().create(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(sampleId1,
                sampleId2, sampleId3), null, null, sessionIdUser).first();

        assertEquals("MyCohort", myCohort.getName());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId1.getId()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId2.getId()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId3.getId()));
    }

//    @Test
//    public void createIndividualWithSamples() throws CatalogException {
//        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().create("user@1000G:phase1", "sample1", "", "", null,
//                null, QueryOptions.empty(), sessionIdUser);
//
//        Sample oldSample = new Sample().setId(sampleQueryResult.first().getId());
//        Sample newSample = new Sample().setName("sample2");
//        ServerUtils.IndividualParameters individualParameters = new ServerUtils.IndividualParameters()
//                .setName("individual").setSamples(Arrays.asList(oldSample, newSample));
//
//        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
//        // We create the individual together with the samples
//        QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().create("user@1000G:phase1",
//                individualParameters, QueryOptions.empty(), sessionIdUser);
//
//        assertEquals(1, individualQueryResult.getNumResults());
//        assertEquals("individual", individualQueryResult.first().getName());
//
//        AbstractManager.MyResourceIds resources = catalogManager.getSampleManager().getIds("sample1,sample2", Long.toString(studyId),
//                sessionIdUser);
//
//        assertEquals(2, resources.getResourceIds().size());
//        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), resources.getResourceIds());
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key());
//        sampleQueryResult = catalogManager.getSampleManager().get(studyId, query, options, sessionIdUser);
//
//        assertEquals(2, sampleQueryResult.getNumResults());
//        for (Sample sample : sampleQueryResult.getResult()) {
//            assertEquals(individualQueryResult.first().getId(), sample.getIndividual().getId());
//        }
//    }

    @Test
    public void testGetAllCohorts() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        Sample sampleId1 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_1", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_2", "", "", null, false, null, null,
                new QueryOptions(), sessionIdUser).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_3", "", "", null, false, null, null,
                new QueryOptions(), sessionIdUser).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_4", "", "", null, false, null, null,
                new QueryOptions(), sessionIdUser).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_5", "", "", null, false, null, null,
                new QueryOptions(), sessionIdUser).first();
        Cohort myCohort1 = catalogManager.getCohortManager().create(studyId, "MyCohort1", Study.Type.FAMILY, "", Arrays.asList(sampleId1,
                sampleId2, sampleId3), null, null, sessionIdUser).first();
        Cohort myCohort2 = catalogManager.getCohortManager().create(studyId, "MyCohort2", Study.Type.FAMILY, "", Arrays.asList(sampleId1,
                sampleId2, sampleId3, sampleId4), null, null, sessionIdUser).first();
        Cohort myCohort3 = catalogManager.getCohortManager().create(studyId, "MyCohort3", Study.Type.CASE_CONTROL, "", Arrays.asList
                (sampleId3, sampleId4), null, null, sessionIdUser).first();
        catalogManager.getCohortManager().create(studyId, "MyCohort4", Study.Type.TRIO, "", Arrays.asList(sampleId5, sampleId3), null,
                null, sessionIdUser).first();

        long numResults;
        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId1.getId()),
                new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                sampleId1.getId() + "," + sampleId5.getId()), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(3, numResults);

//        numResults = catalogManager.getAllCohorts(studyId, new QueryOptions(CatalogSampleDBAdaptor.CohortFilterOption.samples.toString
// (), sampleId3 + "," + sampleId4), sessionIdUser).getNumResults();
//        assertEquals(2, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(),
                "MyCohort2"), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(),
                "~MyCohort."), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(4, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.TYPE.key(),
                Study.Type.FAMILY), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.TYPE.key(),
                "CASE_CONTROL"), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(),
                myCohort1.getId() + "," + myCohort2.getId() + "," + myCohort3.getId()), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(3, numResults);
    }

    @Test
    public void testCreateCohortFail() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        thrown.expect(CatalogException.class);
        List<Sample> sampleList = Arrays.asList(new Sample().setId(23L), new Sample().setId(4L), new Sample().setId(5L));
        catalogManager.getCohortManager().create(studyId, "MyCohort", Study.Type.FAMILY, "", sampleList, null, null,
                sessionIdUser);
    }

    @Test
    public void testCreateCohortAlreadyExisting() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        Sample sampleId1 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_1", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first();
        catalogManager.getCohortManager().create(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(sampleId1), null, null,
                sessionIdUser).first();


        thrown.expect(CatalogDBException.class);
        catalogManager.getCohortManager().create(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(sampleId1), null, null,
                sessionIdUser).first();
    }

    @Test
    public void testUpdateCohort() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        Sample sampleId1 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_1", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_2", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_3", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_4", "", "", null, false, null, null,
                new QueryOptions(), sessionIdUser).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_5", "", "", null, false, null, null,
                new QueryOptions(), sessionIdUser).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(sampleId1,
                sampleId2, sampleId3), null, null, sessionIdUser).first();


        assertEquals("MyCohort", myCohort.getName());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId1.getId()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId2.getId()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId3.getId()));

        Cohort myModifiedCohort = catalogManager.modifyCohort(myCohort.getId(),
                new ObjectMap("samples", Arrays.asList(sampleId1.getId(), sampleId3.getId(), sampleId4.getId(), sampleId5.getId()))
                        .append("name", "myModifiedCohort"), new QueryOptions(), sessionIdUser).first();

        assertEquals("myModifiedCohort", myModifiedCohort.getName());
        assertEquals(4, myModifiedCohort.getSamples().size());
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId1.getId()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId3.getId()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId4.getId()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId5.getId()));
    }

    /*                    */
    /* Test util methods  */
    /*                    */

    @Test
    public void testDeleteCohort() throws CatalogException, IOException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1", sessionIdUser);

        Sample sampleId1 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_1", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_2", "", "", null, false, null, null, new QueryOptions(), sessionIdUser).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_3", "", "", null, false, null, null,
                new QueryOptions(), sessionIdUser).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(sampleId1,
                sampleId2, sampleId3), null, null, sessionIdUser).first();


        assertEquals("MyCohort", myCohort.getName());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId1.getId()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId2.getId()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()).contains(sampleId3.getId()));

        Cohort myDeletedCohort = catalogManager.getCohortManager().delete(Long.toString(myCohort.getId()), Long.toString(studyId), null,
                sessionIdUser).get(0).first();

        assertEquals(myCohort.getId(), myDeletedCohort.getId());

        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.ID.key(), myCohort.getId())
                .append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Cohort.CohortStatus.READY);
        Cohort cohort = catalogManager.getCohortManager().get(studyId, query, null, sessionIdUser).first();
        assertEquals(Status.TRASHED, cohort.getStatus().getName());
    }

    /**
     * Individual methods
     * ***************************
     */

    @Test
    public void testAnnotateIndividual() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        VariableSet variableSet = study.getVariableSets().get(0);

        long individualId1 = catalogManager.createIndividual(studyId, "INDIVIDUAL_1", "", -1, -1, null, new QueryOptions(), sessionIdUser)
                .first().getId();
        long individualId2 = catalogManager.createIndividual(studyId, "INDIVIDUAL_2", "", -1, -1, null, new QueryOptions(), sessionIdUser)
                .first().getId();
        long individualId3 = catalogManager.createIndividual(studyId, "INDIVIDUAL_3", "", -1, -1, null, new QueryOptions(), sessionIdUser)
                .first().getId();

        catalogManager.getIndividualManager().createAnnotationSet(Long.toString(individualId1), null, Long.toString(variableSet.getId()),
                "annot1", new ObjectMap("NAME", "INDIVIDUAL_1").append("AGE", 5).append("PHEN", "CASE").append("ALIVE", true),
                null, sessionIdUser);
        catalogManager.getIndividualManager().createAnnotationSet(Long.toString(individualId2), null, Long.toString(variableSet.getId()), "annot1", new ObjectMap("NAME", "INDIVIDUAL_2").append
                ("AGE", 15).append("PHEN", "CONTROL").append("ALIVE", true), null, sessionIdUser);
        catalogManager.getIndividualManager().createAnnotationSet(Long.toString(individualId3), null, Long.toString(variableSet.getId()), "annot1", new ObjectMap("NAME", "INDIVIDUAL_3").append
                ("AGE", 25).append("PHEN", "CASE").append("ALIVE", true), null, sessionIdUser);

        List<String> individuals;
        individuals = catalogManager.getAllIndividuals(studyId, new Query(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                        variableSet.getId())
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION.key() + ".NAME", "~^INDIVIDUAL_"),
                null, sessionIdUser).getResult().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_1", "INDIVIDUAL_2", "INDIVIDUAL_3")));

        individuals = catalogManager.getAllIndividuals(studyId, new Query(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                        variableSet.getId())
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION.key() + ".AGE", ">10"),
                null, sessionIdUser).getResult().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_2", "INDIVIDUAL_3")));

        individuals = catalogManager.getAllIndividuals(studyId, new Query(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                        variableSet.getId())
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION.key() + ".AGE", ">10")
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION.key() + ".PHEN", "CASE"),
                null, sessionIdUser).getResult().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_3")));
    }

    @Test
    public void testUpdateIndividualInfo() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();

        IIndividualManager individualManager = catalogManager.getIndividualManager();
        QueryResult<Individual> individualQueryResult = individualManager.create(study.getId(), "Test", null, -1, -1, Individual.Sex.UNDETERMINED, "", "",
                "", "", "19870214", Individual.KaryotypicSex.UNKNOWN, Individual.LifeStatus.ALIVE, Individual.AffectationStatus.AFFECTED,
                QueryOptions.empty(), sessionIdUser);
        assertEquals(1, individualQueryResult.getNumResults());
        assertEquals("Test", individualQueryResult.first().getName());
        assertEquals("19870214", individualQueryResult.first().getDateOfBirth());

        QueryResult<Individual> update = individualManager.update(individualQueryResult.first().getId(), new ObjectMap
                (IndividualDBAdaptor.QueryParams.DATE_OF_BIRTH.key(), null), QueryOptions.empty(), sessionIdUser);
        assertEquals("", update.first().getDateOfBirth());

        update = individualManager.update(individualQueryResult.first().getId(),
                new ObjectMap(IndividualDBAdaptor.QueryParams.DATE_OF_BIRTH.key(), "19870214"), QueryOptions.empty(), sessionIdUser);
        assertEquals("19870214", update.first().getDateOfBirth());

        // Wrong date of birth format
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Invalid date of birth format");
        individualManager.update(individualQueryResult.first().getId(),
                new ObjectMap(IndividualDBAdaptor.QueryParams.DATE_OF_BIRTH.key(), "198421"), QueryOptions.empty(), sessionIdUser);
    }
}