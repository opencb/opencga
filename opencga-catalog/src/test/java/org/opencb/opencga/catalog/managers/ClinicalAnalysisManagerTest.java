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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.clinical.Comment;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.interpretation.Analyst;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;

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
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null, sessionIdUser);
    }

    @After
    public void tearDown() throws Exception {
    }

    private DataResult<Family> createDummyFamily() throws CatalogException {
        Disorder disease1 = new Disorder("dis1", "Disease 1", "HPO", null, "", null);
        Disorder disease2 = new Disorder("dis2", "Disease 2", "HPO", null, "", null);

        Individual father = new Individual().setId("father").setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null)));
        Individual mother = new Individual().setId("mother").setDisorders(Arrays.asList(new Disorder("dis2", "dis2", "OT", null, "", null)));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null)))
                .setSamples(Arrays.asList(new Sample().setId("sample1")));
        Individual relMother = new Individual().setId("mother").setDisorders(Arrays.asList(new Disorder("dis2", "dis2", "OT", null, "", null)))
                .setSamples(Arrays.asList(new Sample().setId("sample3")));

        Individual relChild1 = new Individual().setId("child1")
                .setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null), new Disorder("dis2", "dis2", "OT", null, "", null)))
                .setFather(father)
                .setMother(mother)
                .setSamples(Arrays.asList(
                        new Sample().setId("sample2"),
                        new Sample().setId("sample4")
                ))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null)))
                .setFather(father)
                .setMother(mother)
                .setSamples(Arrays.asList(
                        new Sample().setId("sample5"),
                        new Sample().setId("sample6")
                ))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setId("child3")
                .setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null)))
                .setFather(father)
                .setMother(mother)
                .setSamples(Arrays.asList(
                        new Sample().setId("sample7"),
                        new Sample().setId("sample8")
                ))
                .setParentalConsanguinity(true);

        Family family = new Family("family", "family", null, Arrays.asList(disease1, disease2),
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother), "", -1,
                Collections.emptyList(), Collections.emptyMap());

        return familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    private DataResult<ClinicalAnalysis> createDummyEnvironment(boolean createFamily) throws CatalogException {

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis" + RandomStringUtils.randomAlphanumeric(3))
                .setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setProband(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))));

        if (createFamily) {
            createDummyFamily();
        }
        clinicalAnalysis.setFamily(new Family().setId("family")
                .setMembers(Arrays.asList(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))))));


        return catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createSingleClinicalAnalysisTestWithoutDisorder() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(STUDY, individual, QueryOptions.empty(), sessionIdUser);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual);
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, clinical.getNumResults());
    }

    @Test
    public void createClinicalAnalysisTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true);

        assertEquals(1, dummyEnvironment.getNumResults());
        assertEquals(0, dummyEnvironment.first().getSecondaryInterpretations().size());

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
    public void searchClinicalAnalysisByProband() throws CatalogException {
        createDummyEnvironment(true);
        createDummyEnvironment(false);

        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(STUDY,
                new Query(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), "^chil"), QueryOptions.empty(), sessionIdUser);
        assertEquals(2, search.getNumResults());
    }

    @Test
    public void deleteClinicalAnalysisTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true);
        OpenCGAResult delete = catalogManager.getClinicalAnalysisManager().delete(STUDY,
                Collections.singletonList(dummyEnvironment.first().getId()), null, sessionIdUser);
        assertEquals(1, delete.getNumDeleted());

        OpenCGAResult<ClinicalAnalysis> clinicalResult  = catalogManager.getClinicalAnalysisManager().get(STUDY,
                Collections.singletonList(dummyEnvironment.first().getId()),
                new Query(ClinicalAnalysisDBAdaptor.QueryParams.DELETED.key(), true), new QueryOptions(), false, sessionIdUser);
        assertEquals(1, clinicalResult.getNumResults());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(), new QueryOptions(), sessionIdUser);
    }

    @Test
    public void updateDisorder() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true);

        ClinicalUpdateParams updateParams = new ClinicalUpdateParams()
                .setDisorder(new DisorderReferenceParam("dis1"));

        catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(), updateParams, QueryOptions.empty(),
                sessionIdUser);
        OpenCGAResult<ClinicalAnalysis> result1 = catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(),
                new QueryOptions(), sessionIdUser);

        assertEquals("dis1", result1.first().getDisorder().getId());
        assertEquals("OT", result1.first().getDisorder().getSource());

        updateParams = new ClinicalUpdateParams()
                .setDisorder(new DisorderReferenceParam("non_existing"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("proband disorders");
        catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(), updateParams, QueryOptions.empty(),
                sessionIdUser);
    }

    @Test
    public void deleteClinicalAnalysisWithInterpretation() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true);
        Interpretation interpretation = new Interpretation();
        interpretation.setId("myInterpretation");
        catalogManager.getInterpretationManager().create(STUDY, dummyEnvironment.first().getId(), interpretation, true, new QueryOptions(),
                sessionIdUser);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("forbidden");
        catalogManager.getClinicalAnalysisManager().delete(STUDY, Collections.singletonList(dummyEnvironment.first().getId()), null,
                sessionIdUser);
    }

    @Test
    public void checkFamilyMembersOrder() throws CatalogException {
        DataResult<Family> dummyFamily = createDummyFamily();

        // Remove all samples from the dummy family to avoid errors
        for (Individual member : dummyFamily.first().getMembers()) {
            member.setSamples(null);
        }

        // Leave only sample2 for child1 in family
        for (Individual member : dummyFamily.first().getMembers()) {
            if (member.getId().equals("child1")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample2")));
            } else if (member.getId().equals("child2")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample5")));
            } else if (member.getId().equals("child3")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample7")));
            }
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

//    @Test
//    public void createInterpretationTest() throws CatalogException {
//        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true);
//
//        InterpretationMethod method = new InterpretationMethod("method", Collections.emptyMap(), Collections.emptyList(),
//                Collections.singletonList(new Software("name", "version", "repo", "commit", "web", Collections.emptyMap())));
//
//        Interpretation i = new Interpretation();
//        i.setId("interpretationId");
//        i.setDescription("description");
//        i.setClinicalAnalysisId(dummyEnvironment.first().getId());
//        i.setMethod(method);
//        i.setAnalyst(new Analyst("user2", "mail@mail.com", "company"));
//        i.setComments(Collections.singletonList(new Comment("author", "type", "comment 1", "date")));
//        i.setPrimaryFindings(Collections.emptyList());
//
//        DataResult<Interpretation> interpretationDataResult = catalogManager.getInterpretationManager()
//                .create(STUDY, dummyEnvironment.first().getId(), i, true, QueryOptions.empty(), sessionIdUser);
//        System.out.println(interpretationDataResult.first());
//
//        DataResult<ClinicalAnalysis> clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().get(STUDY,
//                dummyEnvironment.first().getId(), QueryOptions.empty(), sessionIdUser);
//        assertEquals(0, clinicalAnalysisDataResult.first().getSecondaryInterpretations().size());
//        assertEquals("interpretationId", clinicalAnalysisDataResult.first().getInterpretation().getId());
//        assertEquals("description", clinicalAnalysisDataResult.first().getInterpretation().getDescription());
//
//        clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().get(STUDY,
//                dummyEnvironment.first().getId(), new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
//                        ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION_ID.key(),
//                        ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key())), sessionIdUser);
//        assertEquals(0, clinicalAnalysisDataResult.first().getSecondaryInterpretations().size());
//        assertEquals("interpretationId", clinicalAnalysisDataResult.first().getInterpretation().getId());
//        assertEquals(null, clinicalAnalysisDataResult.first().getInterpretation().getDescription());
//    }

    @Test
    public void updateInterpretationTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true);

        InterpretationMethod method = new InterpretationMethod("method", Collections.emptyMap(), Collections.emptyList(),
                Collections.singletonList(new Software("name", "version", "repo", "commit", "web", Collections.emptyMap())));

        InterpretationUpdateParams i = new InterpretationUpdateParams();
        i.setId("interpretationId");
        i.setDescription("description");
        i.setClinicalAnalysisId(dummyEnvironment.first().getId());
        i.setMethods(Collections.singletonList(method));
        i.setAnalyst(new Analyst("user2", "mail@mail.com", "company"));
        i.setComments(Collections.singletonList(new Comment("author", "type", "comment 1", "date")));
        i.setPrimaryFindings(Collections.emptyList());

        DataResult<Interpretation> interpretationDataResult = catalogManager.getInterpretationManager()
                .update(STUDY, dummyEnvironment.first().getId(), i, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, interpretationDataResult.getNumUpdated());

        DataResult<ClinicalAnalysis> clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().get(STUDY,
                dummyEnvironment.first().getId(), QueryOptions.empty(), sessionIdUser);
        assertEquals(0, clinicalAnalysisDataResult.first().getSecondaryInterpretations().size());
        assertEquals("interpretationId", clinicalAnalysisDataResult.first().getInterpretation().getId());
        assertEquals("description", clinicalAnalysisDataResult.first().getInterpretation().getDescription());

        clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().get(STUDY,
                dummyEnvironment.first().getId(), new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION_ID.key(),
                        ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key())), sessionIdUser);
        assertEquals(0, clinicalAnalysisDataResult.first().getSecondaryInterpretations().size());
        assertEquals("interpretationId", clinicalAnalysisDataResult.first().getInterpretation().getId());
        assertEquals(null, clinicalAnalysisDataResult.first().getInterpretation().getDescription());
    }

    @Test
    public void sampleNotFoundInMember() throws CatalogException {
        DataResult<Family> dummyFamily = createDummyFamily();

        // Remove all samples from the dummy family to avoid errors
        for (Individual member : dummyFamily.first().getMembers()) {
            member.setSamples(null);
        }

        // Leave only sample2 for child1 in family
        for (Individual member : dummyFamily.first().getMembers()) {
            if (member.getId().equals("child1")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample2")));
            } else if (member.getId().equals("child2")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample2")));
            } else if (member.getId().equals("child3")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample2")));
            }
        }

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000")
                .setProband(new Individual().setId("child1"));
        clinicalAnalysis.setFamily(dummyFamily.first());
        thrown.expect(CatalogException.class);
        thrown.expectMessage("could not be found in member");
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void checkMoreThanOneSample() throws CatalogException {
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
        thrown.expect(CatalogException.class);
        thrown.expectMessage("More than one sample");
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createClinicalAnalysisWithoutFamily() throws CatalogException {
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setProband(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("missing");
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createClinicalAnalysisWithoutProband() throws CatalogException {
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000");

        thrown.expect(CatalogException.class);
        thrown.expectMessage("missing");
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createClinicalAnalysisWithoutType() throws CatalogException {
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description")
                .setDueDate("20180510100000");

        thrown.expect(CatalogException.class);
        thrown.expectMessage("missing");
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
    }

}
