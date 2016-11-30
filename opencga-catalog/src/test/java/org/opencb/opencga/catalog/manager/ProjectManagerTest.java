package org.opencb.opencga.catalog.manager;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Created by pfurio on 28/11/16.
 */
public class ProjectManagerTest extends GenericTest {

    public final static String PASSWORD = "asdf";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    protected String sessionIdUser;
    protected String sessionIdUser2;
    protected String sessionIdUser3;
    private long project1;
    private long project2;
    private long project3;
    private long studyId;
    private long studyId2;
    private long studyId3;

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, null);
        catalogManager.createUser("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", null, null);
        catalogManager.createUser("user3", "User3 Name", "user.2@e.mail", PASSWORD, "ACME", null, null);

        sessionIdUser = catalogManager.login("user", PASSWORD, "127.0.0.1").first().getString("sessionId");
        sessionIdUser2 = catalogManager.login("user2", PASSWORD, "127.0.0.1").first().getString("sessionId");
        sessionIdUser3 = catalogManager.login("user3", PASSWORD, "127.0.0.1").first().getString("sessionId");

        project1 = catalogManager.createProject("Project about some genomes", "1000G", "", "ACME", null, sessionIdUser).first().getId();
        project2 = catalogManager.createProject("Project Management Project", "pmp", "life art intelligent system", "myorg", null,
                sessionIdUser2).first().getId();
        project3 = catalogManager.createProject("project 1", "p1", "", "", null, sessionIdUser3).first().getId();

        studyId = catalogManager.createStudy(project1, "Phase 1", "phase1", Study.Type.TRIO, "Done", sessionIdUser).first().getId();
        studyId2 = catalogManager.createStudy(project1, "Phase 3", "phase3", Study.Type.CASE_CONTROL, "d", sessionIdUser).first().getId();
        studyId3 = catalogManager.createStudy(project2, "Study 1", "s1", Study.Type.CONTROL_SET, "", sessionIdUser2).first().getId();
    }

    @Test
    public void getSharedProjects() throws CatalogException {
        QueryResult<Project> queryResult = catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
        assertEquals(0, queryResult.getNumResults());

        // Create a new study in project2 with some dummy permissions for user
        long s2 = catalogManager.createStudy(project2, "Study 2", "s2", Study.Type.CONTROL_SET, "", sessionIdUser2).first().getId();
        catalogManager.createStudyAcls(Long.toString(s2), "user", StudyAclEntry.StudyPermissions.VIEW_STUDY.toString(), null,
                sessionIdUser2);

        queryResult = catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
        assertEquals(1, queryResult.getNumResults());
        assertEquals(1, queryResult.first().getStudies().size());
        assertEquals("s2", queryResult.first().getStudies().get(0).getAlias());

        // Add permissions to a group were user belongs
        catalogManager.getStudyManager().createGroup(Long.toString(studyId3), "@member", "user", sessionIdUser2);
        catalogManager.createStudyAcls(Long.toString(studyId3), "@member", StudyAclEntry.StudyPermissions.VIEW_STUDY.toString(), null,
                sessionIdUser2);

        queryResult = catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
        assertEquals(1, queryResult.getNumResults());
        assertEquals(2, queryResult.first().getStudies().size());

        // Add permissions to user in a study of user3
        long s3 = catalogManager.createStudy(project3, "StudyProject3", "s3", Study.Type.CONTROL_SET, "", sessionIdUser3).first().getId();
        catalogManager.createStudyAcls(Long.toString(s3), "user", StudyAclEntry.StudyPermissions.VIEW_STUDY.toString(), null,
                sessionIdUser3);

        queryResult = catalogManager.getProjectManager().getSharedProjects("user", null, sessionIdUser);
        assertEquals(2, queryResult.getNumResults());
        for (Project project : queryResult.getResult()) {
            if (project.getId() == project2) {
                assertEquals(2, project.getStudies().size());
            } else {
                assertEquals(1, project.getStudies().size());
            }
        }
    }
}
