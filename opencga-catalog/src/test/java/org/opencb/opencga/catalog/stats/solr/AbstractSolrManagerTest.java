/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.stats.solr;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.DBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclParams;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclParams;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

@Category(MediumTests.class)
public class AbstractSolrManagerTest extends GenericTest {

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

    protected Study study;
    protected String studyFqn;

    protected static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = solrExternalResource.getCatalogManager();
        setUpCatalogManager(catalogManager);

        catalogSolrManager = new CatalogSolrManager(catalogManager);
        catalogSolrManager.setSolrClient(solrExternalResource.getSolrClient());
    }

    @After
    public void after() throws CatalogException {
        solrExternalResource.after();
        catalogSolrManager.close();
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {

        catalogManager.getUserManager().create("owner", "Owner", "owner@mail.com", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, null);
        catalogManager.getUserManager().create("admin1", "Admin", "admin@mail.com", TestParamConstants.PASSWORD, "", null, Account.AccountType.GUEST, null);
        catalogManager.getUserManager().create("user1", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.GUEST, null);
        catalogManager.getUserManager().create("user2", "User2 Name", "mail2@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.GUEST, null);
        catalogManager.getUserManager().create("user3", "User3 Name", "user.2@e.mail", TestParamConstants.PASSWORD, "ACME", null, Account.AccountType.GUEST, null);

        sessionIdOwner = catalogManager.getUserManager().login("owner", TestParamConstants.PASSWORD).getToken();
        sessionIdAdmin = catalogManager.getUserManager().login("admin1", TestParamConstants.PASSWORD).getToken();
        sessionIdUser = catalogManager.getUserManager().login("user1", TestParamConstants.PASSWORD).getToken();
        sessionIdUser2 = catalogManager.getUserManager().login("user2", TestParamConstants.PASSWORD).getToken();
        sessionIdUser3 = catalogManager.getUserManager().login("user3", TestParamConstants.PASSWORD).getToken();

        Project project = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdOwner).first();
        studyFqn = catalogManager.getStudyManager().create(project.getFqn(), "phase1", null, "Phase 1", "Done", null,
                null, null, null, INCLUDE_RESULT, sessionIdOwner).first().getFqn();

        catalogManager.getStudyManager().updateGroup(studyFqn, "@admins", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("admin1")), sessionIdOwner);
        catalogManager.getStudyManager().createGroup(studyFqn, "@study_allow", Collections.singletonList("user1"), sessionIdAdmin);
        catalogManager.getStudyManager().createGroup(studyFqn, "@study_deny", Collections.singletonList("user2"), sessionIdAdmin);

        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "@study_allow",
                new StudyAclParams(null, "view_only"), ParamUtils.AclAction.ADD, sessionIdAdmin);

        study = catalogManager.getStudyManager().get("phase1", new QueryOptions(DBAdaptor.INCLUDE_ACLS, true), sessionIdOwner).first();

        // Samples
        Sample sample1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample1"), INCLUDE_RESULT,
                sessionIdAdmin).first();
        Sample sample2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample2"), INCLUDE_RESULT,
                sessionIdAdmin).first();
        Sample sample3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample3"), INCLUDE_RESULT,
                sessionIdAdmin).first();

        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList("sample1"), "@study_deny,user3",
                new SampleAclParams(null, null, null, null, "VIEW,VIEW_ANNOTATIONS"), ParamUtils.AclAction.ADD, sessionIdAdmin);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList("sample2"), "@study_allow",
                new SampleAclParams(null, null, null, null, ""), ParamUtils.AclAction.SET, sessionIdAdmin);

        // Cohorts
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("cohort1").setSamples(Arrays.asList(sample1, sample2, sample3)),
                QueryOptions.empty(), sessionIdAdmin);
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("cohort2").setSamples(Arrays.asList(sample1, sample2)),
                QueryOptions.empty(), sessionIdAdmin);
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("cohort3").setSamples(Arrays.asList(sample2, sample3)),
                QueryOptions.empty(), sessionIdAdmin);

        catalogManager.getCohortManager().updateAcl(studyFqn, Collections.singletonList("cohort1"), "@study_deny,user3",
                new AclParams("VIEW,VIEW_ANNOTATIONS"), ParamUtils.AclAction.ADD, sessionIdAdmin);
        catalogManager.getCohortManager().updateAcl(studyFqn, Collections.singletonList("cohort2"), "@study_allow",
                new AclParams(""), ParamUtils.AclAction.SET, sessionIdAdmin);
    }

}
