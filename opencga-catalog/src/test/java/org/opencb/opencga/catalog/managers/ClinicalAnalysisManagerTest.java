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

package org.opencb.opencga.catalog.managers;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.commons.Software;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalUpdateParams;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.user.Account;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ClinicalAnalysisManagerTest extends GenericTest {

    public final static String PASSWORD = "asdf";
    public final static String STUDY = "user@1000G:phase1";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    private FamilyManager familyManager;
    protected String sessionIdUser;

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.AccountType.FULL, null);
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD);

        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null, sessionIdUser);

    }

    @After
    public void tearDown() throws Exception {
    }

    private DataResult<Family> createDummyFamily() throws CatalogException {
        Phenotype disease1 = new Phenotype("dis1", "Disease 1", "HPO");
        Phenotype disease2 = new Phenotype("dis2", "Disease 2", "HPO");

        Individual father = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual mother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setId("mother").setPhenotypes(Arrays.asList(new Phenotype("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setId("child1")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT"), new Phenotype("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setSamples(Arrays.asList(
                        new Sample().setId("sample1"),
                        new Sample().setId("sample2"),
                        new Sample().setId("sample3"),
                        new Sample().setId("sample4")
                ))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setId("child3")
                .setPhenotypes(Arrays.asList(new Phenotype("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setParentalConsanguinity(true);

        Family family = new Family("family", "family", Arrays.asList(disease1, disease2), null,
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother), "", -1,
                Collections.emptyList(), Collections.emptyMap());

        return familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    private DataResult<ClinicalAnalysis> createDummyEnvironment(boolean createFamily) throws CatalogException {

        createDummyFamily();
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000")
                .setProband(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))));

        if (createFamily) {
            clinicalAnalysis.setFamily(new Family().setId("family")
                    .setMembers(Arrays.asList(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))))));
        }

        return catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createClinicalAnalysisTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true);

        assertEquals(1, dummyEnvironment.getNumResults());
        assertEquals(0, dummyEnvironment.first().getInterpretations().size());

        assertEquals("family", dummyEnvironment.first().getFamily().getId());
        assertEquals(1, dummyEnvironment.first().getFamily().getMembers().size());

        assertEquals("child1", dummyEnvironment.first().getFamily().getMembers().get(0).getId());
        assertEquals(1, dummyEnvironment.first().getFamily().getMembers().get(0).getSamples().size());
        assertEquals("sample2", dummyEnvironment.first().getFamily().getMembers().get(0).getSamples().get(0).getId());

        assertNotNull(dummyEnvironment.first().getProband());
        assertEquals("child1", dummyEnvironment.first().getProband().getId());

        assertEquals(1, dummyEnvironment.first().getProband().getSamples().size());
        assertEquals("sample2", dummyEnvironment.first().getProband().getSamples().get(0).getId());

        assertEquals(catalogManager.getSampleManager().get(STUDY, "sample2", SampleManager.INCLUDE_SAMPLE_IDS, sessionIdUser)
                .first().getUid(), dummyEnvironment.first().getProband().getSamples().get(0).getUid());
    }

    @Test
    public void checkFamilyMembersOrder() throws CatalogException {
        DataResult<Family> dummyFamily = createDummyFamily();

        // Remove all samples from the dummy family to avoid errors
        for (Individual member : dummyFamily.first().getMembers()) {
            member.setSamples(null);
        }

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000")
                .setProband(new Individual().setId("child1"));
        clinicalAnalysis.setFamily(dummyFamily.first());
        DataResult<ClinicalAnalysis> clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().create(STUDY,
                clinicalAnalysis, QueryOptions.empty(), sessionIdUser);

        assertEquals("child1", clinicalAnalysisDataResult.first().getFamily().getMembers().get(0).getId());
        assertEquals("father", clinicalAnalysisDataResult.first().getFamily().getMembers().get(1).getId());
        assertEquals("mother", clinicalAnalysisDataResult.first().getFamily().getMembers().get(2).getId());
        assertEquals("child2", clinicalAnalysisDataResult.first().getFamily().getMembers().get(3).getId());
        assertEquals("child3", clinicalAnalysisDataResult.first().getFamily().getMembers().get(4).getId());
    }

    @Test
    public void createInterpretationTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true);

        Interpretation i = new Interpretation()
                .setId("interpretationId")
                .setDescription("description")
                .setClinicalAnalysisId(dummyEnvironment.first().getId())
                .setSoftware(new Software("name", "version", "repo", "commit", "web", Collections.emptyMap()))
                .setAnalyst(new Analyst("user2", "mail@mail.com", "company"))
                .setComments(Collections.singletonList(new Comment("author", "type", "comment 1", "date")))
                .setPrimaryFindings(Collections.emptyList());

        DataResult<Interpretation> interpretationDataResult = catalogManager.getInterpretationManager()
                .create(STUDY, dummyEnvironment.first().getId(), i, QueryOptions.empty(), sessionIdUser);
        System.out.println(interpretationDataResult.first());

        DataResult<ClinicalAnalysis> clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().get(STUDY,
                dummyEnvironment.first().getId(), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, clinicalAnalysisDataResult.first().getInterpretations().size());
        assertEquals("interpretationId", clinicalAnalysisDataResult.first().getInterpretations().get(0).getId());
        assertEquals("description", clinicalAnalysisDataResult.first().getInterpretations().get(0).getDescription());

        clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().get(STUDY,
                dummyEnvironment.first().getId(), new QueryOptions(QueryOptions.INCLUDE, "interpretations.id"), sessionIdUser);
        assertEquals(1, clinicalAnalysisDataResult.first().getInterpretations().size());
        assertEquals("interpretationId", clinicalAnalysisDataResult.first().getInterpretations().get(0).getId());
        assertEquals(null, clinicalAnalysisDataResult.first().getInterpretations().get(0).getDescription());
    }

    @Test
    public void createClinicalAnalysisNoFamilyTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(false);

        assertEquals(1, dummyEnvironment.getNumResults());
        assertEquals(0, dummyEnvironment.first().getInterpretations().size());

        assertEquals(catalogManager.getIndividualManager().get(STUDY, "child1", IndividualManager.INCLUDE_INDIVIDUAL_IDS, sessionIdUser)
                .first().getUid(), dummyEnvironment.first().getProband().getUid());
        assertEquals(1, dummyEnvironment.first().getProband().getSamples().size());
        assertEquals(catalogManager.getSampleManager().get(STUDY, "sample2", SampleManager.INCLUDE_SAMPLE_IDS, sessionIdUser)
                .first().getUid(), dummyEnvironment.first().getProband().getSamples().get(0).getUid());
    }

    @Test
    public void updateSubjectsNoFamilyTest() throws CatalogException {
        createDummyEnvironment(false);

        ClinicalUpdateParams updateParams = new ClinicalUpdateParams().setProband(
                new ClinicalUpdateParams.ProbandParam()
                        .setId("child1")
                        .setSamples(Collections.singletonList(new ClinicalUpdateParams.SampleParams().setId("sample2"))));
        DataResult<ClinicalAnalysis> updateResult = catalogManager.getClinicalAnalysisManager().update(STUDY, "analysis", updateParams,
                QueryOptions.empty(), sessionIdUser);
        assertEquals(1, updateResult.getNumUpdated());

        ClinicalAnalysis analysis = catalogManager.getClinicalAnalysisManager().get(STUDY, "analysis", QueryOptions.empty(), sessionIdUser).first();

        assertEquals(catalogManager.getIndividualManager().get(STUDY, "child1", IndividualManager.INCLUDE_INDIVIDUAL_IDS, sessionIdUser)
                .first().getUid(), analysis.getProband().getUid());
        assertEquals(1, analysis.getProband().getSamples().size());
        assertEquals(catalogManager.getSampleManager().get(STUDY, "sample2", SampleManager.INCLUDE_SAMPLE_IDS, sessionIdUser)
                .first().getUid(), analysis.getProband().getSamples().get(0).getUid());
    }

    @Test
    public void updateSubjectsAndFamilyTest() throws CatalogException {
        createDummyEnvironment(false);

        ClinicalUpdateParams updateParams = new ClinicalUpdateParams()
                .setProband(new ClinicalUpdateParams.ProbandParam()
                        .setId("child1")
                        .setSamples(Collections.singletonList(new ClinicalUpdateParams.SampleParams().setId("sample2"))))
                .setFamily(new ClinicalUpdateParams.FamilyParam()
                        .setId("family")
                        .setMembers(Collections.singletonList(new ClinicalUpdateParams.ProbandParam()
                                .setId("child1")
                                .setSamples(Collections.singletonList(new ClinicalUpdateParams.SampleParams().setId("sample2"))))));
        DataResult<ClinicalAnalysis> updateResult = catalogManager.getClinicalAnalysisManager().update(STUDY, "analysis", updateParams,
                QueryOptions.empty(), sessionIdUser);
        assertEquals(1, updateResult.getNumUpdated());

        ClinicalAnalysis analysis = catalogManager.getClinicalAnalysisManager().get(STUDY, "analysis", QueryOptions.empty(), sessionIdUser).first();

        assertEquals(catalogManager.getFamilyManager().get(STUDY, "family", FamilyManager.INCLUDE_FAMILY_IDS, sessionIdUser)
                .first().getUid(), analysis.getFamily().getUid());
        assertEquals(catalogManager.getIndividualManager().get(STUDY, "child1", IndividualManager.INCLUDE_INDIVIDUAL_IDS, sessionIdUser)
                .first().getUid(), analysis.getProband().getUid());
        assertEquals(1, analysis.getProband().getSamples().size());
        assertEquals(catalogManager.getSampleManager().get(STUDY, "sample2", SampleManager.INCLUDE_SAMPLE_IDS, sessionIdUser)
                .first().getUid(), analysis.getProband().getSamples().get(0).getUid());
    }

}
