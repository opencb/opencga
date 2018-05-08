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

package org.opencb.opencga.catalog.managers;

import com.mongodb.BasicDBObject;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;

import javax.naming.NamingException;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

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
    private String project1;
    private String project2;
    private long studyUid;
    private String studyFqn;
    private long studyUid2;
    private String studyFqn2;

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

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null, null);
        catalogManager.getUserManager().create("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null, null);
        catalogManager.getUserManager().create("user3", "User3 Name", "user.2@e.mail", PASSWORD, "ACME", null, Account.FULL, null, null);

        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD);
        sessionIdUser2 = catalogManager.getUserManager().login("user2", PASSWORD);
        sessionIdUser3 = catalogManager.getUserManager().login("user3", PASSWORD);

        project1 = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        project2 = catalogManager.getProjectManager().create("pmp", "Project Management Project", "life art intelligent system", "myorg",
                "Homo sapiens", null, null, "GRCh38", new QueryOptions(), sessionIdUser2).first().getId();
        catalogManager.getProjectManager().create("p1", "project 1", "", "", "Homo sapiens", null, null, "GRCh38", new QueryOptions(),
                sessionIdUser3).first();

        Study study = catalogManager.getStudyManager().create(project1, "phase1", "Phase 1", Study.Type.TRIO, null, "Done",
                null, null, null, null, null, null, null, null, sessionIdUser).first();
        studyUid = study.getUid();
        studyFqn = study.getFqn();

        study = catalogManager.getStudyManager().create(project1, "phase3", "Phase 3", Study.Type.CASE_CONTROL, null, "d", null, null,
                null, null, null, null, null, null, sessionIdUser).first();
        studyUid2 = study.getUid();
        studyFqn2 = study.getFqn();

        catalogManager.getStudyManager().create(project2, "s1", "Study 1", Study.Type.CONTROL_SET, null, "", null, null, null, null,
                null, null, null, null, sessionIdUser2);

        catalogManager.getFileManager().createFolder(studyFqn2, Paths.get("data/test/folder/").toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getFileManager().createFolder(studyFqn, Paths.get("analysis/").toString(), null, true, null, QueryOptions.empty(),
                sessionIdUser);
        catalogManager.getFileManager().createFolder(studyFqn2, Paths.get("analysis/").toString(), null, true, null, QueryOptions.empty(),
                sessionIdUser);

        testFolder = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/test/folder/").toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser).first();
        ObjectMap attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("numValue", 5);
        catalogManager.getFileManager().update(studyFqn, testFolder.getPath(), new ObjectMap("attributes", attributes), new QueryOptions(),
                sessionIdUser);

        QueryResult<File> queryResult2 = catalogManager.getFileManager().create(studyFqn, File.Type.FILE, File.Format.PLAIN, File.Bioformat
                .NONE, testFolder.getPath() + "test_1K.txt.gz", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);

        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(1000).getBytes()), queryResult2.first(), sessionIdUser, false, false, true);

        File fileTest1k = catalogManager.getFileManager().get(studyFqn, queryResult2.first().getPath(), null, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("name", "fileTest1k");
        attributes.put("numValue", "10");
        attributes.put("boolean", false);
        catalogManager.getFileManager().update(studyFqn, fileTest1k.getPath(), new ObjectMap("attributes", attributes), new QueryOptions(),
                sessionIdUser);

        QueryResult<File> queryResult1 = catalogManager.getFileManager().create(studyFqn, File.Type.FILE, File.Format.PLAIN, File.Bioformat.DATAMATRIX_EXPRESSION, testFolder.getPath() + "test_0.5K.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(500).getBytes()), queryResult1.first(), sessionIdUser, false, false, true);
        File fileTest05k = catalogManager.getFileManager().get(studyFqn, queryResult1.first().getPath(), null, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "valuable");
        attributes.put("name", "fileTest05k");
        attributes.put("numValue", 5);
        attributes.put("boolean", true);
        catalogManager.getFileManager().update(studyFqn, fileTest05k.getPath(), new ObjectMap("attributes", attributes), new QueryOptions(),
                sessionIdUser);

        QueryResult<File> queryResult = catalogManager.getFileManager().create(studyFqn, File.Type.FILE, File.Format.IMAGE, File.Bioformat.NONE, testFolder.getPath() + "test_0.1K.png", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(100).getBytes()), queryResult.first(), sessionIdUser, false, false, true);
        File test01k = catalogManager.getFileManager().get(studyFqn, queryResult.first().getPath(), null, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "other");
        attributes.put("name", "test01k");
        attributes.put("numValue", 50);
        attributes.put("nested", new ObjectMap("num1", 45).append("num2", 33).append("text", "HelloWorld"));
        catalogManager.getFileManager().update(studyFqn, test01k.getPath(), new ObjectMap("attributes", attributes), new QueryOptions(),
                sessionIdUser);

        List<Variable> variables = new ArrayList<>();
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
        VariableSet vs = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs", true, false, "", null, variables,
                sessionIdUser).first();

        Sample sample = new Sample().setId("s_1");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", "s_1").append("AGE", 6).append("ALIVE", true).append("PHEN", "CONTROL"))));
        catalogManager.getSampleManager().create(studyFqn, sample, new QueryOptions(), sessionIdUser).first();

        sample.setId("s_2");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", "s_2").append("AGE", 10).append("ALIVE", false).append("PHEN", "CASE"))));
        catalogManager.getSampleManager().create(studyFqn, sample, new QueryOptions(), sessionIdUser).first();

        sample.setId("s_3");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", "s_3").append("AGE", 15).append("ALIVE", true).append("PHEN", "CONTROL"))));
        catalogManager.getSampleManager().create(studyFqn, sample, new QueryOptions(), sessionIdUser).first();

        sample.setId("s_4");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", "s_4").append("AGE", 22).append("ALIVE", false).append("PHEN", "CONTROL"))));
        catalogManager.getSampleManager().create(studyFqn, sample, new QueryOptions(), sessionIdUser).first();

        sample.setId("s_5");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", "s_5").append("AGE", 29).append("ALIVE", true).append("PHEN", "CASE"))));
        catalogManager.getSampleManager().create(studyFqn, sample, new QueryOptions(), sessionIdUser).first();

        sample.setId("s_6");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot2", vs.getId(),
                new ObjectMap("NAME", "s_6").append("AGE", 38).append("ALIVE", true).append("PHEN", "CONTROL"))));
        catalogManager.getSampleManager().create(studyFqn, sample, new QueryOptions(), sessionIdUser).first();

        sample.setId("s_7");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot2", vs.getId(),
                new ObjectMap("NAME", "s_7").append("AGE", 46).append("ALIVE", false).append("PHEN", "CASE"))));
        catalogManager.getSampleManager().create(studyFqn, sample, new QueryOptions(), sessionIdUser).first();

        sample.setId("s_8");
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot2", vs.getId(),
                new ObjectMap("NAME", "s_8").append("AGE", 72).append("ALIVE", true).append("PHEN", "CONTROL"))));
        catalogManager.getSampleManager().create(studyFqn, sample, new QueryOptions(), sessionIdUser).first();

        sample.setId("s_9");
        sample.setAnnotationSets(Collections.emptyList());
        catalogManager.getSampleManager().create(studyFqn, sample, new QueryOptions(), sessionIdUser).first();

        catalogManager.getFileManager().update(studyFqn, test01k.getPath(),
                new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(), Arrays.asList("s_1", "s_2", "s_3", "s_4", "s_5")), new QueryOptions(),
                sessionIdUser);
    }

    @After
    public void tearDown() throws Exception {
    }

    public CatalogManager getTestCatalogManager() {
        return catalogManager;
    }

    @Test
    public void testAdminUserExists() throws Exception {
        String token = catalogManager.getUserManager().login("admin", "admin");
        assertEquals("admin" ,catalogManager.getUserManager().getUserId(token));
    }

    @Test
    public void testCreateExistingUser() throws Exception {
        thrown.expect(CatalogException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null, null);
    }

    @Test
    public void testLogin() throws Exception {
        catalogManager.getUserManager().login("user", PASSWORD);

        thrown.expect(CatalogAuthenticationException.class);
        thrown.expectMessage(allOf(containsString("Incorrect"), containsString("password")));
        catalogManager.getUserManager().login("user", "fakePassword");
    }

    @Test
    public void testGetUserInfo() throws CatalogException {
        QueryResult<User> user = catalogManager.getUserManager().get("user", null, new QueryOptions(), sessionIdUser);
        System.out.println("user = " + user);
        QueryResult<User> userVoid = catalogManager.getUserManager().get("user", user.first().getLastModified(), new QueryOptions(),
                sessionIdUser);
        System.out.println("userVoid = " + userVoid);
        assertTrue(userVoid.getResult().isEmpty());
        try {
            catalogManager.getUserManager().get("user", null, new QueryOptions(), sessionIdUser2);
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

        User userPre = catalogManager.getUserManager().get("user", null, new QueryOptions(), sessionIdUser).first();
        System.out.println("userPre = " + userPre);
        Thread.sleep(10);

        catalogManager.getUserManager().update("user", params, null, sessionIdUser);
        catalogManager.getUserManager().update("user", new ObjectMap("email", newEmail), null, sessionIdUser);
        catalogManager.getUserManager().changePassword("user", PASSWORD, newPassword);
        new QueryResult("changePassword", 0, 0, 0, "", "", Collections.emptyList());

        List<User> userList = catalogManager.getUserManager().get("user", userPre.getLastModified(), new QueryOptions(QueryOptions
                .INCLUDE, Arrays.asList(UserDBAdaptor.QueryParams.PASSWORD.key(), UserDBAdaptor.QueryParams.NAME.key(), UserDBAdaptor.QueryParams
                .EMAIL.key(), UserDBAdaptor.QueryParams.ATTRIBUTES.key())), sessionIdUser).getResult();
        if (userList.isEmpty()) {
            fail("Error. LastModified should have changed");
        }
        User userPost = userList.get(0);
        System.out.println("userPost = " + userPost);
        assertTrue(!userPre.getLastModified().equals(userPost.getLastModified()));
        assertEquals(userPost.getName(), newName);
        assertEquals(userPost.getEmail(), newEmail);
        assertEquals(null, userPost.getPassword());

        catalogManager.getUserManager().login("user", newPassword);
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(userPost.getAttributes().get(entry.getKey()), entry.getValue());
        }

        catalogManager.getUserManager().changePassword("user", newPassword, PASSWORD);
        new QueryResult("changePassword", 0, 0, 0, "", "", Collections.emptyList());
        catalogManager.getUserManager().login("user", PASSWORD);

        try {
            params = new ObjectMap();
            params.put("password", "1234321");
            catalogManager.getUserManager().update("user", params, null, sessionIdUser);
            fail("Expected exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        try {
            catalogManager.getUserManager().update("user", params, null, sessionIdUser2);
            fail("Expected exception");
        } catch (CatalogException e) {
            System.out.println(e);
        }

    }

    private String getAdminToken() throws CatalogException, IOException {
        return catalogManager.getUserManager().login("admin", "admin");
    }

    @Ignore
    @Test
    public void importLdapUsers() throws CatalogException, NamingException, IOException {
        // Action only for admins
        ObjectMap params = new ObjectMap()
                .append("users", "pfurio,imedina");

        QueryResult<User> ldapImportResult = catalogManager.getUserManager().importFromExternalAuthOrigin("ldap", Account.GUEST, params,
                getAdminToken());

        assertEquals(2, ldapImportResult.getNumResults());
    }

    // To make this test work we will need to add a correct user and password to be able to login
    @Ignore
    @Test
    public void loginNotRegisteredUsers() throws CatalogException, NamingException, IOException {
        // Action only for admins
        catalogManager.getStudyManager().createGroup(studyFqn, "ldap", "", sessionIdUser);
        catalogManager.getStudyManager().syncGroupWith(studyFqn, "ldap", new Group.Sync("ldap", "bio"), sessionIdUser);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), "@ldap", new Study.StudyAclParams("",
                AclParams.Action.SET, "view_only"), sessionIdUser);
        String token = catalogManager.getUserManager().login("user", "password");

        QueryResult<Study> studyQueryResult = catalogManager.getStudyManager().get(String.valueOf((Long) studyUid), QueryOptions.empty(),
                token);
        assertEquals(1, studyQueryResult.getNumResults());

        // We remove the permissions for group ldap
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), "@ldap", new Study.StudyAclParams("",
                AclParams.Action.RESET, ""), sessionIdUser);
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudyManager().get(String.valueOf((Long) studyUid), QueryOptions.empty(), token);
    }

    @Ignore
    @Test
    public void importLdapGroups() throws CatalogException, NamingException, IOException {
        // Action only for admins
        ObjectMap params = new ObjectMap()
                .append("group", "bio")
                .append("study", "user@1000G:phase1")
                .append("study-group", "test");
        catalogManager.getUserManager().importFromExternalAuthOrigin("ldap", Account.GUEST, params, getAdminToken());

        QueryResult<Group> test = catalogManager.getStudyManager().getGroup("user@1000G:phase1", "test", sessionIdUser);
        assertEquals(1, test.getNumResults());
        assertEquals("@test", test.first().getName());
        assertTrue(test.first().getUserIds().size() > 0);

        params.put("study-group", "test1");
        try {
            catalogManager.getUserManager().importFromExternalAuthOrigin("ldap", Account.GUEST, params, getAdminToken());
            fail("Should not be possible creating another group containing the same users that belong to a different group");
        } catch (CatalogException e) {
            System.out.println(e.getMessage());
        }

        params = new ObjectMap()
                .append("group", "bioo")
                .append("study", "user@1000G:phase1")
                .append("study-group", "test2");
        catalogManager.getUserManager().importFromExternalAuthOrigin("ldap", Account.GUEST, params, getAdminToken());

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

        catalogManager.getProjectManager().create(projectAlias, "Project", "", "", "Homo sapiens", null, null, "GRCh38", new
                QueryOptions(), sessionIdUser);

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.getProjectManager().create(projectAlias, "Project", "", "", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser);
    }

    @Test
    public void testModifyProject() throws CatalogException {
        String newProjectName = "ProjectName " + StringUtils.randomString(10);
        String projectId = catalogManager.getUserManager().get("user", null, new QueryOptions(), sessionIdUser).first().getProjects().get(0)
                .getId();

        ObjectMap options = new ObjectMap();
        options.put("name", newProjectName);
        ObjectMap attributes = new ObjectMap("myBoolean", true);
        attributes.put("value", 6);
        attributes.put("object", new ObjectMap("id", 1234));
        options.put("attributes", attributes);

        catalogManager.getProjectManager().update(projectId, options, null, sessionIdUser);
        QueryResult<Project> result = catalogManager.getProjectManager().get(projectId, null, sessionIdUser);
        Project project = result.first();
        System.out.println(result);

        assertEquals(newProjectName, project.getName());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(project.getAttributes().get(entry.getKey()), entry.getValue());
        }

        options = new ObjectMap();
        options.put(ProjectDBAdaptor.QueryParams.ID.key(), "newProjectId");
        catalogManager.getProjectManager().update(projectId, options, null, sessionIdUser);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getProjectManager().update(projectId, options, null, sessionIdUser2);

    }

    /**
     * Study methods
     * ***************************
     */

    @Test
    public void testModifyStudy() throws Exception {
        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user");
        String studyId =  catalogManager.getStudyManager().get(query, null, sessionIdUser).first().getId();

        String newName = "Phase 1 " + StringUtils.randomString(20);
        String newDescription = StringUtils.randomString(500);

        ObjectMap parameters = new ObjectMap();
        parameters.put("name", newName);
        parameters.put("description", newDescription);
        BasicDBObject attributes = new BasicDBObject("key", "value");
        parameters.put("attributes", attributes);
        catalogManager.getStudyManager().update(studyId, parameters, null, sessionIdUser);

        QueryResult<Study> result = catalogManager.getStudyManager().get(studyId, null, sessionIdUser);
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
        String projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(projectId, "study_1", "study_1", Study.Type.CASE_CONTROL, "creationDate",
                "description", new Status(), null, null, null, null, null, null, null, sessionIdUser);

        catalogManager.getStudyManager().create(projectId, "study_2", "study_2", Study.Type.CASE_CONTROL, "creationDate",
                "description", new Status(), null, null, null, null, null, null, null, sessionIdUser);

        catalogManager.getStudyManager().create(projectId, "study_3", "study_3", Study.Type.CASE_CONTROL, "creationDate",
                "description", new Status(), null, null, null, null, null, null, null, sessionIdUser);

        String study_4 = catalogManager.getStudyManager().create(projectId, "study_4", "study_4", Study.Type.CASE_CONTROL,
        "creationDate", "description", new Status(), null, null, null, null, null, null, null, sessionIdUser).first().getId();

        assertEquals(new HashSet<>(Collections.emptyList()), catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams
                .GROUP_USER_IDS.key(), "user2"), null, sessionIdUser).getResult().stream().map(Study::getId)
                .collect(Collectors.toSet()));

//        catalogManager.getStudyManager().createGroup(Long.toString(study_4), "admins", "user3", sessionIdUser);
        catalogManager.getStudyManager().updateGroup(study_4, "admins", new GroupParams("user3", GroupParams.Action.SET),
                sessionIdUser);
        assertEquals(new HashSet<>(Arrays.asList("study_4")), catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams
                .GROUP_USER_IDS.key(), "user3"), null, sessionIdUser).getResult().stream().map(Study::getId)
                .collect(Collectors.toSet()));

        assertEquals(new HashSet<>(Arrays.asList("phase1", "phase3", "study_1", "study_2", "study_3", "study_4")),
                catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, sessionIdUser)
                .getResult().stream().map(Study::getId).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("phase1", "phase3", "study_1", "study_2", "study_3", "study_4")),
                catalogManager.getStudyManager().get(new Query(), null, sessionIdUser).getResult().stream().map(Study::getId)
                        .collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("study_1", "study_2", "study_3", "study_4")), catalogManager.getStudyManager().get(new
                Query(StudyDBAdaptor.QueryParams.ID.key(), "~^study"), null, sessionIdUser).getResult().stream()
                .map(Study::getId).collect(Collectors.toSet()));
        assertEquals(Collections.singleton("s1"), catalogManager.getStudyManager().get(new Query(), null, sessionIdUser2).getResult()
                .stream()
                .map(Study::getId).collect(Collectors.toSet()));
    }

    @Test
    public void testGetId() throws CatalogException {
        // Create another study with alias phase3
        catalogManager.getStudyManager().create(project2, "phase3", "Phase 3", Study.Type.CASE_CONTROL, null, "d", null, null, null,
                null, null, null, null, null, sessionIdUser2);

        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        List<Long> uids = catalogManager.getStudyManager().resolveIds(Arrays.asList("*"), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Collections.emptyList(), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Collections.emptyList(), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList("1000G:*"), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(userId + "@1000G:*"), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(userId + "@1000G:phase1", userId + "@1000G:phase3"), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(userId + "@1000G:phase1", "phase3"), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        uids = catalogManager.getStudyManager().resolveIds(Arrays.asList(userId + "@1000G:phase3", studyFqn), userId)
                .stream()
                .map(Study::getUid)
                .collect(Collectors.toList());
        assertTrue(uids.contains(studyUid) && uids.contains(studyUid2));

        try {
            catalogManager.getStudyManager().resolveId(null, userId);
            fail("This method should fail because it should find several studies");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("More than one study"));
        }

        Study study = catalogManager.getStudyManager().resolveId("phase3", userId);
        assertEquals(studyUid2, study.getUid());
    }

    @Test
    public void testGetOnlyStudyUserAnonymousCanSee() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        try {
            studyManager.resolveIds(Collections.emptyList(), "*");
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        // Create another study with alias phase3
        QueryResult<Study> study = catalogManager.getStudyManager().create(String.valueOf(project2), "phase3", "Phase 3", Study.Type
        .CASE_CONTROL, null, "d", null, null, null, null, null, null, null, null, sessionIdUser2);
        try {
            studyManager.resolveIds(Collections.emptyList(), "*");
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        catalogManager.getStudyManager().updateGroup("phase3", "@members", new GroupParams("*", GroupParams.Action.ADD), sessionIdUser2);

        List<Study> studies = studyManager.resolveIds(Collections.emptyList(), "*");
        assertEquals(1, studies.size());
        assertEquals(study.first().getUid(), studies.get(0).getUid());
    }

    @Test
    public void testGetSelectedStudyUserAnonymousCanSee() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        try {
            studyManager.resolveIds(Collections.singletonList("phase3"), "*");
            fail("This should throw an exception. No studies should be found for user anonymous");
        } catch (CatalogException e) {
        }

        // Create another study with alias phase3
        QueryResult<Study> study = catalogManager.getStudyManager().create(project2, "phase3", "Phase 3",
                Study.Type.CASE_CONTROL, null, "d", null, null, null, null, null, null, null, null, sessionIdUser2);
        catalogManager.getStudyManager().updateGroup("phase3", "@members", new GroupParams("*", GroupParams.Action.ADD), sessionIdUser2);

        List<Study> studies = studyManager.resolveIds(Collections.singletonList("phase3"), "*");
        assertEquals(1, studies.size());
        assertEquals(study.first().getUid(), studies.get(0).getUid());
    }

    @Test
    public void testUpdateGroupInfo() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        studyManager.createGroup(studyFqn, "group1", "", sessionIdUser);
        studyManager.createGroup(studyFqn, "group2", "", sessionIdUser);

        Group.Sync syncFrom = new Group.Sync("auth", "aaa");
        studyManager.syncGroupWith(studyFqn, "group2", syncFrom, sessionIdUser);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Cannot modify already existing sync information");
        studyManager.syncGroupWith(studyFqn, "group2", syncFrom, sessionIdUser);
    }

    @Test
    public void testCreatePermissionRules() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList("user2", "user3"),
                Arrays.asList("VIEW", "UPDATE"));
        QueryResult<PermissionRule> permissionRulesQueryResult = catalogManager.getStudyManager().createPermissionRule(
                studyFqn, Study.Entry.SAMPLES, rules, sessionIdUser);
        assertEquals(1, permissionRulesQueryResult.getNumResults());
        assertEquals("rules1", permissionRulesQueryResult.first().getId());
        assertEquals(1, permissionRulesQueryResult.first().getQuery().size());
        assertEquals(2, permissionRulesQueryResult.first().getMembers().size());
        assertEquals(2, permissionRulesQueryResult.first().getPermissions().size());

        // Add new permission rules object
        rules.setId("rules2");
        permissionRulesQueryResult = catalogManager.getStudyManager().createPermissionRule(studyFqn, Study.Entry.SAMPLES, rules,
                sessionIdUser);
        assertEquals(1, permissionRulesQueryResult.getNumResults());
        assertEquals(rules, permissionRulesQueryResult.first());
    }

    @Test
    public void testUpdatePermissionRulesIncorrectPermission() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList("user2", "user3"),
                Arrays.asList("VV", "UPDATE"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Detected unsupported");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Study.Entry.SAMPLES, rules, sessionIdUser);
    }

    @Test
    public void testUpdatePermissionRulesNonExistingUser() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList("user2", "user20"),
                Arrays.asList("VIEW", "UPDATE"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Study.Entry.SAMPLES, rules, sessionIdUser);
    }

    @Test
    public void testUpdatePermissionRulesNonExistingGroup() throws CatalogException {
        PermissionRule rules = new PermissionRule("rules1", new Query("a", "b"), Arrays.asList("user2", "@group"),
                Arrays.asList("VIEW", "UPDATE"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().createPermissionRule(studyFqn, Study.Entry.SAMPLES, rules, sessionIdUser);
    }

    @Test
    public void removeAllPermissionsToMember() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        // Assign permissions to study
        QueryResult<Group> groupQueryResult = studyManager.updateGroup(studyFqn, "@members",
                new GroupParams("user2,user3", GroupParams.Action.ADD), sessionIdUser);
        assertEquals(2, groupQueryResult.first().getUserIds().size());
        assertEquals("@members", groupQueryResult.first().getName());

        // Obtain all samples from study
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, new Query(), QueryOptions
                .empty(), sessionIdUser);
        assertTrue(sampleQueryResult.getNumResults() > 0);

        // Assign permissions to all the samples
        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams("VIEW,UPDATE", AclParams.Action.SET, null, null, null);
        List<String> sampleIds = sampleQueryResult.getResult().stream()
                .map(Sample::getId)
                .collect(Collectors.toList());
        List<QueryResult<SampleAclEntry>> sampleAclResult = catalogManager.getSampleManager().updateAcl(studyFqn,
                sampleIds, "user2,user3", sampleAclParams, sessionIdUser);
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
        groupQueryResult = studyManager.updateGroup(studyFqn, "@members", new GroupParams("user2,user3",
                GroupParams.Action.REMOVE), sessionIdUser);
        assertEquals(0, groupQueryResult.first().getUserIds().size());

        // Get sample permissions for those members
        for (Sample sample : sampleQueryResult.getResult()) {
            long sampleUid = sample.getUid();
            QueryResult<SampleAclEntry> sampleAcl =
                    catalogManager.getAuthorizationManager().getSampleAcl(studyUid, sampleUid, "user", "user2");
            assertEquals(0, sampleAcl.getNumResults());
            sampleAcl = catalogManager.getAuthorizationManager().getSampleAcl(studyUid, sampleUid, "user", "user3");
            assertEquals(0, sampleAcl.getNumResults());
        }
    }

    @Test
    public void removeUsersFromStudies() throws CatalogException {
        StudyManager studyManager = catalogManager.getStudyManager();

        // Assign permissions to study
        QueryResult<Group> groupQueryResult = studyManager.updateGroup(studyFqn, "@members",
                new GroupParams("user2,user3", GroupParams.Action.ADD), sessionIdUser);
        assertEquals(2, groupQueryResult.first().getUserIds().size());
        assertEquals("@members", groupQueryResult.first().getName());

        // Obtain all samples from study
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, new Query(), QueryOptions
                .empty(), sessionIdUser);
        assertTrue(sampleQueryResult.getNumResults() > 0);

        // Assign permissions to all the samples
        Sample.SampleAclParams sampleAclParams = new Sample.SampleAclParams("VIEW,UPDATE", AclParams.Action.SET, null, null, null);
        List<String> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());

        List<QueryResult<SampleAclEntry>> sampleAclResult = catalogManager.getSampleManager().updateAcl(studyFqn,
                sampleIds, "user2,user3", sampleAclParams, sessionIdUser);
        assertEquals(sampleIds.size(), sampleAclResult.size());
        for (QueryResult<SampleAclEntry> sampleAclEntryQueryResult : sampleAclResult) {
            assertEquals(2, sampleAclEntryQueryResult.getNumResults());
            for (SampleAclEntry sampleAclEntry : sampleAclEntryQueryResult.getResult()) {
                assertTrue(sampleAclEntry.getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW));
                assertTrue(sampleAclEntry.getPermissions().contains(SampleAclEntry.SamplePermissions.UPDATE));
                assertTrue(Arrays.asList("user2", "user3").contains(sampleAclEntry.getMember()));
            }
        }

        catalogManager.getStudyManager().updateGroup(studyFqn, "@members",
                new GroupParams("user2,user3", GroupParams.Action.REMOVE), sessionIdUser);

        String userId1 = catalogManager.getUserManager().getUserId(sessionIdUser);
        Study study3 = catalogManager.getStudyManager().resolveId(studyFqn, userId1);

        QueryResult<StudyAclEntry> studyAcl = catalogManager.getAuthorizationManager().getStudyAcl(userId1, study3.getUid(), "user2");
        assertEquals(0, studyAcl.getNumResults());
        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        Study study1 = catalogManager.getStudyManager().resolveId(studyFqn, userId);
        studyAcl = catalogManager.getAuthorizationManager().getStudyAcl(userId, study1.getUid(), "user3");
        assertEquals(0, studyAcl.getNumResults());

        groupQueryResult = catalogManager.getStudyManager().getGroup(studyFqn, null, sessionIdUser);
        for (Group group : groupQueryResult.getResult()) {
            assertTrue(!group.getUserIds().contains("user2"));
            assertTrue(!group.getUserIds().contains("user3"));
        }

        for (Sample sample : sampleQueryResult.getResult()) {
            QueryResult<SampleAclEntry> sampleAcl =
                    catalogManager.getAuthorizationManager().getSampleAcl(studyUid, sample.getUid(), "user", "user2");
            assertEquals(0, sampleAcl.getNumResults());
            sampleAcl = catalogManager.getAuthorizationManager().getSampleAcl(studyUid, sample.getUid(), "user", "user3");
            assertEquals(0, sampleAcl.getNumResults());
        }
    }

    /**
     * Job methods
     * ***************************
     */

    @Test
    public void testCreateJob() throws CatalogException, IOException {
        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user");
        String studyId = catalogManager.getStudyManager().get(query, null, sessionIdUser).first().getId();

        File outDir = catalogManager.getFileManager().createFolder(studyId, Paths.get("jobs", "myJob").toString(),
                null, true, null, QueryOptions.empty(), sessionIdUser).first();

        catalogManager.getJobManager().create(studyId,new Job().setId("myJob").setToolId("samtool").setDescription("description")
                        .setOutDir(outDir).setExecution("echo \"Hello world!\"").setStatus(new Job.JobStatus(Job.JobStatus.PREPARED)),
                null, sessionIdUser);

        catalogManager.getJobManager().create(studyId, new Job().setId("myReadyJob").setToolId("samtool").setDescription("description")
                        .setOutDir(outDir).setExecution("echo \"Hello world!\"").setStatus(new Job.JobStatus(Job.JobStatus.READY)), null,
                sessionIdUser);

        catalogManager.getJobManager().create(studyId, new Job().setId("myQueuedJob").setToolId("samtool").setDescription("description")
                        .setOutDir(outDir).setExecution("echo \"Hello world!\"").setStatus(new Job.JobStatus(Job.JobStatus.QUEUED)), null,
                sessionIdUser);

        catalogManager.getJobManager().create(studyId, new Job().setId("myErrorJob").setToolId("samtool").setDescription("description")
                        .setOutDir(outDir).setExecution("echo \"Hello world!\"").setStatus(new Job.JobStatus(Job.JobStatus.ERROR)), null,
                sessionIdUser);

        query = new Query()
                .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Arrays.asList(Job.JobStatus.PREPARED, Job.JobStatus.QUEUED,
                        Job.JobStatus.RUNNING, Job.JobStatus.DONE));
        QueryResult<Job> unfinishedJobs = catalogManager.getJobManager().get(String.valueOf(studyId), query, null, sessionIdUser);
        assertEquals(2, unfinishedJobs.getNumResults());

        QueryResult<Job> allJobs = catalogManager.getJobManager().get(String.valueOf(studyId), (Query) null, null, sessionIdUser);
        assertEquals(4, allJobs.getNumResults());
    }

    @Test
    public void testGetAllJobs() throws CatalogException {
        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user");
        String studyId = catalogManager.getStudyManager().get(query, null, sessionIdUser).first().getId();

        catalogManager.getJobManager().create(studyId, new Job().setId("myErrorJob").setToolId("samtools"), null, sessionIdUser);

        QueryResult<Job> allJobs = catalogManager.getJobManager().get(studyId, (Query) null, null, sessionIdUser);

        assertEquals(1, allJobs.getNumTotalResults());
        assertEquals(1, allJobs.getNumResults());
    }

    /**
     * VariableSet methods
     * ***************************
     */

    @Test
    public void testCreateVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().get("user@1000G:phase1", null, sessionIdUser).first();
        long variableSetNum = study.getVariableSets().size();

        List<Variable> variables = new ArrayList<>();
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
        QueryResult<VariableSet> queryResult = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs1", true, false, "",
                null, variables, sessionIdUser);

        assertEquals(1, queryResult.getResult().size());

        study = catalogManager.getStudyManager().get(study.getId(), null, sessionIdUser).first();
        assertEquals(variableSetNum + 1, study.getVariableSets().size());
    }

    @Test
    public void testCreateRepeatedVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().get("user@1000G:phase1", null, sessionIdUser).first();

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
        catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs1", true, false, "", null, variables, sessionIdUser);
    }

    @Test
    public void testDeleteVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().resolveId("1000G:phase1", "user");

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs1", true, false, "", null, variables,
                sessionIdUser).first();

        VariableSet vs1_deleted = catalogManager.getStudyManager().deleteVariableSet(studyFqn, Long.toString(vs1.getUid()),
                sessionIdUser).first();

        assertEquals(vs1.getUid(), vs1_deleted.getUid());

        thrown.expect(CatalogException.class);    //VariableSet does not exist
        thrown.expectMessage("not found");
        catalogManager.getStudyManager().getVariableSet(studyFqn, vs1.getId(), null, sessionIdUser);
    }

    @Test
    public void testGetAllVariableSet() throws CatalogException {
        Study study = catalogManager.getStudyManager().resolveId("1000G:phase1", "user");

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs1", true, false, "Cancer", null, variables,
                sessionIdUser).first();
        VariableSet vs2 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs2", true, false, "Virgo", null, variables,
                sessionIdUser).first();
        VariableSet vs3 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs3", true, false, "Piscis", null, variables,
                sessionIdUser).first();
        VariableSet vs4 = catalogManager.getStudyManager().createVariableSet(study.getFqn(), "vs4", true, false, "Aries", null, variables,
                sessionIdUser).first();

        long numResults;
        numResults = catalogManager.getStudyManager().searchVariableSets(study.getFqn(),
                new Query(StudyDBAdaptor.VariableSetParams.ID.key(), "vs1"), QueryOptions.empty(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getStudyManager().searchVariableSets(study.getFqn(),
                new Query(StudyDBAdaptor.VariableSetParams.ID.key(), "vs1,vs2"), QueryOptions.empty(), sessionIdUser)
                .getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getStudyManager().searchVariableSets(study.getFqn(),
                new Query(StudyDBAdaptor.VariableSetParams.ID.key(), "VS1"), QueryOptions.empty(), sessionIdUser).getNumResults();
        assertEquals(0, numResults);

        numResults = catalogManager.getStudyManager().searchVariableSets(study.getFqn(),
                new Query(StudyDBAdaptor.VariableSetParams.UID.key(), vs1.getId()), QueryOptions.empty(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);
//        numResults = catalogManager.getStudyManager().searchVariableSets(studyFqn,
//                new Query(StudyDBAdaptor.VariableSetParams.ID.key(), vs1.getId() + "," + vs3.getId()), QueryOptions.empty(),
//                sessionIdUser).getNumResults();

        numResults = catalogManager.getStudyManager().searchVariableSets(study.getFqn(),
                new Query(StudyDBAdaptor.VariableSetParams.UID.key(), vs3.getId()), QueryOptions.empty(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);
    }

    @Test
    public void testDeleteVariableSetInUse() throws CatalogException {
        String sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"),
                new QueryOptions(), sessionIdUser).first().getId();
        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "", null,
                        Collections.emptyMap()),
                new Variable("AGE", "", "", Variable.VariableType.DOUBLE, null, false, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.emptyMap())
        );
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", true, false, "", null, variables,
                sessionIdUser).first();

        Map<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "LINUS");
        catalogManager.getSampleManager().update(studyFqn, sampleId1, new ObjectMap()
                        .append(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(), Collections.singletonList(new ObjectMap()
                                .append(AnnotationSetManager.ID, "annotationId")
                                .append(AnnotationSetManager.VARIABLE_SET_ID, vs1.getId())
                                .append(AnnotationSetManager.ANNOTATIONS, annotations))
                        ),
                QueryOptions.empty(), sessionIdUser);

        try {
            catalogManager.getStudyManager().deleteVariableSet(studyFqn, Long.toString(vs1.getUid()), sessionIdUser).first();
        } finally {
            VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyFqn, vs1.getId(), null, sessionIdUser).first();
            assertEquals(vs1.getUid(), variableSet.getUid());

            thrown.expect(CatalogDBException.class); //Expect the exception from the try
        }
    }

    /**
     * Sample methods
     * ***************************
     */

    /*
     * Cohort methods
     *
     */

    @Test
    public void testCreateCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), new QueryOptions(),
                sessionIdUser).first();
        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort")
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), null, sessionIdUser).first();

        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));
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
//        long studyUid = catalogManager.getStudyManager().getId("user", "1000G:phase1");
//        // We create the individual together with the samples
//        QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().create("user@1000G:phase1",
//                individualParameters, QueryOptions.empty(), sessionIdUser);
//
//        assertEquals(1, individualQueryResult.getNumResults());
//        assertEquals("individual", individualQueryResult.first().getName());
//
//        AbstractManager.MyResourceIds resources = catalogManager.getSampleManager().getIds("sample1,sample2", studyFqn,
//                sessionIdUser);
//
//        assertEquals(2, resources.getResourceIds().size());
//        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), resources.getResourceIds());
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key());
//        sampleQueryResult = catalogManager.getSampleManager().get(studyUid, query, options, sessionIdUser);
//
//        assertEquals(2, sampleQueryResult.getNumResults());
//        for (Sample sample : sampleQueryResult.getResult()) {
//            assertEquals(individualQueryResult.first().getId(), sample.getIndividual().getId());
//        }
//    }

    @Test
    public void testGetAllCohorts() throws CatalogException {
        String studyId = "1000G:phase1";

        Sample sampleId1 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_2"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_3"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_4"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_5"), new QueryOptions(),
                sessionIdUser).first();
        Cohort myCohort1 = catalogManager.getCohortManager().create(studyId,
                new Cohort().setId("MyCohort1").setType(Study.Type.FAMILY).setSamples(Arrays.asList(sampleId1,sampleId2, sampleId3)),
                null, sessionIdUser).first();
        Cohort myCohort2 = catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort2").setType(Study.Type.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3, sampleId4)), null, sessionIdUser).first();
        Cohort myCohort3 = catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort3")
                .setType(Study.Type.CASE_CONTROL).setSamples(Arrays.asList(sampleId3, sampleId4)), null, sessionIdUser).first();
        catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort4").setType(Study.Type.TRIO)
                .setSamples(Arrays.asList(sampleId5, sampleId3)), null, sessionIdUser).first();

        long numResults;
        numResults = catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId1.getId()),
                new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId1.getId()
                + "," + sampleId5.getId()), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(3, numResults);

        numResults = catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(), "MyCohort2"), new
                QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(), "~MyCohort."), new
                QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(4, numResults);

        numResults = catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.TYPE.key(), Study.Type.FAMILY),
                new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.TYPE.key(), "CASE_CONTROL"),
                new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getCohortManager().get(studyId, new Query(CohortDBAdaptor.QueryParams.UID.key(), myCohort1.getUid() +
                "," + myCohort2.getUid() + "," + myCohort3.getUid()), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(3, numResults);
    }

    @Test
    public void testCreateCohortFail() throws CatalogException {
        thrown.expect(CatalogException.class);
        List<Sample> sampleList = Arrays.asList(new Sample().setUid(23L), new Sample().setUid(4L), new Sample().setUid(5L));
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Study.Type.FAMILY).setSamples(sampleList),
                null, sessionIdUser);
    }

    @Test
    public void testCreateCohortAlreadyExisting() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Study.Type.FAMILY)
                .setSamples(Collections.singletonList(sampleId1)), null, sessionIdUser).first();


        thrown.expect(CatalogDBException.class);
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Study.Type.FAMILY)
                .setSamples(Collections.singletonList(sampleId1)), null, sessionIdUser).first();
    }

    @Test
    public void testUpdateCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_4"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_5"), new QueryOptions(),
                sessionIdUser).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Study.Type.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), null, sessionIdUser).first();


        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));

        Cohort myModifiedCohort = catalogManager.getCohortManager().update(studyFqn, myCohort.getId(),
                new ObjectMap("samples", Arrays.asList(sampleId1.getId(), sampleId3.getId(), sampleId4.getId(), sampleId5.getId()))
                        .append(CohortDBAdaptor.QueryParams.ID.key(), "myModifiedCohort"), new QueryOptions(), sessionIdUser).first();

        assertEquals("myModifiedCohort", myModifiedCohort.getId());
        assertEquals(4, myModifiedCohort.getSamples().size());
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId4.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId5.getUid()));
    }

    /*                    */
    /* Test util methods  */
    /*                    */

    @Test
    public void testDeleteCohort() throws CatalogException, IOException {
        String studyId = "user@1000G:phase1";

        Sample sampleId1 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_2"), new QueryOptions(),
                sessionIdUser).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyId, new Sample().setId("SAMPLE_3"), new QueryOptions(),
                sessionIdUser).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyId, new Cohort().setId("MyCohort").setType(Study.Type.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), null, sessionIdUser).first();

        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));

        WriteResult deleteResult = catalogManager.getCohortManager().delete(studyId,
                new Query(CohortDBAdaptor.QueryParams.UID.key(), myCohort.getUid()), null, sessionIdUser);

        assertEquals(1, deleteResult.getNumModified());

        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.UID.key(), myCohort.getUid())
                .append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Cohort.CohortStatus.READY);
        Cohort cohort = catalogManager.getCohortManager().get(studyId, query, null, sessionIdUser).first();
        assertEquals(Status.DELETED, cohort.getStatus().getName());
    }

    /**
     * Individual methods
     * ***************************
     */

    @Test
    public void testAnnotateIndividual() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, null, sessionIdUser).first();
        VariableSet variableSet = study.getVariableSets().get(0);

        String individualId1 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_1")
                .setKaryotypicSex(Individual.KaryotypicSex.UNKNOWN).setLifeStatus(Individual.LifeStatus.UNKNOWN)
                        .setAffectationStatus(Individual.AffectationStatus.UNKNOWN), new QueryOptions(), sessionIdUser)
                .first().getId();
        String individualId2 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_2")
                .setKaryotypicSex(Individual.KaryotypicSex.UNKNOWN).setLifeStatus(Individual.LifeStatus.UNKNOWN)
                        .setAffectationStatus(Individual.AffectationStatus.UNKNOWN), new QueryOptions(), sessionIdUser)
                .first().getId();
        String individualId3 = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_3")
                .setKaryotypicSex(Individual.KaryotypicSex.UNKNOWN).setLifeStatus(Individual.LifeStatus.UNKNOWN)
                        .setAffectationStatus(Individual.AffectationStatus.UNKNOWN), new QueryOptions(), sessionIdUser)
                .first().getId();

        catalogManager.getIndividualManager().update(studyFqn, individualId1, new ObjectMap()
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key(), Collections.singletonList(new ObjectMap()
                                .append(AnnotationSetManager.ID, "annot1")
                                .append(AnnotationSetManager.VARIABLE_SET_ID, variableSet.getId())
                                .append(AnnotationSetManager.ANNOTATIONS, new ObjectMap("NAME", "INDIVIDUAL_1").append("AGE", 5)
                                        .append("PHEN", "CASE").append("ALIVE", true)))
                        ),
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getIndividualManager().update(studyFqn, individualId2, new ObjectMap()
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key(), Collections.singletonList(new ObjectMap()
                                .append(AnnotationSetManager.ID, "annot1")
                                .append(AnnotationSetManager.VARIABLE_SET_ID, variableSet.getId())
                                .append(AnnotationSetManager.ANNOTATIONS, new ObjectMap("NAME", "INDIVIDUAL_2").append("AGE", 15)
                                        .append("PHEN", "CONTROL").append("ALIVE", true)))
                        ),
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getIndividualManager().update(studyFqn, individualId3, new ObjectMap()
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key(), Collections.singletonList(new ObjectMap()
                                .append(AnnotationSetManager.ID, "annot1")
                                .append(AnnotationSetManager.VARIABLE_SET_ID, variableSet.getId())
                                .append(AnnotationSetManager.ANNOTATIONS, new ObjectMap("NAME", "INDIVIDUAL_3").append("AGE", 25)
                                        .append("PHEN", "CASE").append("ALIVE", true)))
                        ),
                QueryOptions.empty(), sessionIdUser);

        List<String> individuals;
        individuals = catalogManager.getIndividualManager().get(studyFqn,
                new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), variableSet.getId() + ":NAME=~^INDIVIDUAL_"), null,
                sessionIdUser)
                .getResult().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_1", "INDIVIDUAL_2", "INDIVIDUAL_3")));

        individuals = catalogManager.getIndividualManager().get(studyFqn,
                new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), variableSet.getId() + ":AGE>10"), null, sessionIdUser)
                .getResult().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_2", "INDIVIDUAL_3")));

        individuals = catalogManager.getIndividualManager().get(studyFqn,
                new Query(IndividualDBAdaptor.QueryParams.ANNOTATION.key(), variableSet.getId() + ":AGE>10;" + variableSet.getId()
                        + ":PHEN=CASE"), null, sessionIdUser)
                .getResult().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_3")));
    }

    @Test
    public void testUpdateIndividualInfo() throws CatalogException {
        IndividualManager individualManager = catalogManager.getIndividualManager();
        QueryResult<Individual> individualQueryResult = individualManager.create(studyFqn, new Individual().setId("Test")
                        .setDateOfBirth("19870214"), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, individualQueryResult.getNumResults());
        assertEquals("Test", individualQueryResult.first().getId());
        assertEquals("19870214", individualQueryResult.first().getDateOfBirth());

        QueryResult<Individual> update = individualManager.update(studyFqn, individualQueryResult.first().getId(),
                new ObjectMap(IndividualDBAdaptor.QueryParams.DATE_OF_BIRTH.key() , null),
                QueryOptions.empty(), sessionIdUser);
        assertEquals("", update.first().getDateOfBirth());

        update = individualManager.update(studyFqn, individualQueryResult.first().getId(),
                new ObjectMap(IndividualDBAdaptor.QueryParams.DATE_OF_BIRTH.key(), "19870214"), QueryOptions.empty(), sessionIdUser);
        assertEquals("19870214", update.first().getDateOfBirth());

        // Wrong date of birth format
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Invalid date of birth format");
        individualManager.update(studyFqn, individualQueryResult.first().getId(),
                new ObjectMap(IndividualDBAdaptor.QueryParams.DATE_OF_BIRTH.key(), "198421"), QueryOptions.empty(), sessionIdUser);
    }
}