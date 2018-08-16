package org.opencb.opencga.catalog.stats.solr;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;

import java.io.IOException;
import java.util.Collections;

public class AbstractSolrManagerTest extends GenericTest {

    public final static String PASSWORD = "password";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public SolrExternalResource solrExternalResource = new SolrExternalResource();

    protected CatalogManager catalogManager;
    protected CatalogSolrManager catalogSolrManager;

    protected String sessionIdOwner;
    protected String sessionIdAdmin;
    protected String sessionIdUser;
    protected String sessionIdUser2;
    protected String sessionIdUser3;

    protected String studyFqn;

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = solrExternalResource.getCatalogManager();
        setUpCatalogManager(catalogManager);

        catalogSolrManager = new CatalogSolrManager(catalogManager);
        catalogSolrManager.setSolrClient(solrExternalResource.getSolrClient());
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {

        catalogManager.getUserManager().create("owner", "Owner", "owner@mail.com", PASSWORD, "", null, Account.FULL, null, null);
        catalogManager.getUserManager().create("admin1", "Admin", "admin@mail.com", PASSWORD, "", null, Account.GUEST, null, null);
        catalogManager.getUserManager().create("user1", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.GUEST, null, null);
        catalogManager.getUserManager().create("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", null, Account.GUEST, null, null);
        catalogManager.getUserManager().create("user3", "User3 Name", "user.2@e.mail", PASSWORD, "ACME", null, Account.GUEST, null, null);

        sessionIdOwner = catalogManager.getUserManager().login("owner", PASSWORD);
        sessionIdAdmin = catalogManager.getUserManager().login("admin1", PASSWORD);
        sessionIdUser = catalogManager.getUserManager().login("user1", PASSWORD);
        sessionIdUser2 = catalogManager.getUserManager().login("user2", PASSWORD);
        sessionIdUser3 = catalogManager.getUserManager().login("user3", PASSWORD);

        Project project = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdOwner).first();
        studyFqn = catalogManager.getStudyManager().create(project.getFqn(), "phase1", null, "Phase 1", Study.Type.TRIO, null, "Done", null,
                null, null, null, null, null, null, null, sessionIdOwner).first().getFqn();

        catalogManager.getStudyManager().updateGroup(studyFqn, "@admins", new GroupParams("admin1", GroupParams.Action.ADD), sessionIdOwner);
        catalogManager.getStudyManager().createGroup(studyFqn, "@study_allow", "user1", sessionIdAdmin);
        catalogManager.getStudyManager().createGroup(studyFqn, "@study_deny", "user2", sessionIdAdmin);

        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "@study_allow",
                new Study.StudyAclParams(null, AclParams.Action.ADD, "view_only"), sessionIdAdmin);

        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample1"), QueryOptions.empty(), sessionIdAdmin).first();
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample2"), QueryOptions.empty(), sessionIdAdmin).first();
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample3"), QueryOptions.empty(), sessionIdAdmin).first();

        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList("sample1"), "@study_deny,user3",
                new Sample.SampleAclParams("VIEW,VIEW_ANNOTATIONS", AclParams.Action.ADD, null, null, null), sessionIdAdmin);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList("sample2"), "@study_allow",
                new Sample.SampleAclParams("", AclParams.Action.SET, null, null, null), sessionIdAdmin);
    }

}
