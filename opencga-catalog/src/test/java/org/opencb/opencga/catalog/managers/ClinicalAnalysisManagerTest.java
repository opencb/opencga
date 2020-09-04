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
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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

        catalogManager.getUserManager().create("user2", "User Name2", "mail2@ebi.ac.uk", PASSWORD, "", null, Account.AccountType.GUEST, null);

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

    private DataResult<ClinicalAnalysis> createDummyEnvironment(boolean createFamily, boolean createDefaultInterpretation) throws CatalogException {

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis" + RandomStringUtils.randomAlphanumeric(3))
                .setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setProband(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))));

        if (createFamily) {
            createDummyFamily();
        }
        clinicalAnalysis.setFamily(new Family().setId("family")
                .setMembers(Arrays.asList(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))))));


        return catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, createDefaultInterpretation, QueryOptions.empty(), sessionIdUser);
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
    public void assignPermissions() throws CatalogException {
        ClinicalAnalysis clinicalAnalysis = createDummyEnvironment(true, false).first();
        catalogManager.getUserManager().create("external", "User Name", "external@mail.com", PASSWORD, "", null, Account.AccountType.GUEST, null);

        OpenCGAResult<Map<String, List<String>>> aclResult = catalogManager.getClinicalAnalysisManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getId()), "external", false, sessionIdUser);
        assertEquals(0, aclResult.getNumResults());

        aclResult = catalogManager.getFamilyManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getFamily().getId()), "external", false, sessionIdUser);
        assertEquals(0, aclResult.getNumResults());

        aclResult = catalogManager.getIndividualManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getProband().getId()), "external", false, sessionIdUser);
        assertEquals(0, aclResult.getNumResults());

        aclResult = catalogManager.getSampleManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getProband().getSamples().get(0).getId()), "external", false, sessionIdUser);
        assertEquals(0, aclResult.getNumResults());

        // Assign permissions to clinical analysis without propagating the permissions
        catalogManager.getClinicalAnalysisManager().updateAcl(STUDY, Collections.singletonList(clinicalAnalysis.getId()), "external",
                new AclParams(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.DELETE.name()), ParamUtils.AclAction.ADD, false, sessionIdUser);

        aclResult = catalogManager.getClinicalAnalysisManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getId()), "external", false, sessionIdUser);
        assertEquals(1, aclResult.getNumResults());
        assertEquals(3, aclResult.first().get("external").size());

        aclResult = catalogManager.getFamilyManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getFamily().getId()), "external", false, sessionIdUser);
        assertEquals(0, aclResult.getNumResults());

        aclResult = catalogManager.getIndividualManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getProband().getId()), "external", false, sessionIdUser);
        assertEquals(0, aclResult.getNumResults());

        aclResult = catalogManager.getSampleManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getProband().getSamples().get(0).getId()), "external", false, sessionIdUser);
        assertEquals(0, aclResult.getNumResults());

        // Assign permissions to clinical analysis PROPAGATING the permissions
        catalogManager.getClinicalAnalysisManager().updateAcl(STUDY, Collections.singletonList(clinicalAnalysis.getId()), "external",
                new AclParams(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.DELETE.name()), ParamUtils.AclAction.ADD, true, sessionIdUser);

        aclResult = catalogManager.getClinicalAnalysisManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getId()), "external", false, sessionIdUser);
        assertEquals(1, aclResult.getNumResults());
        assertEquals(3, aclResult.first().get("external").size());

        aclResult = catalogManager.getFamilyManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getFamily().getId()), "external", false, sessionIdUser);
        assertEquals(1, aclResult.getNumResults());
        assertEquals(2, aclResult.first().get("external").size());

        aclResult = catalogManager.getIndividualManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getProband().getId()), "external", false, sessionIdUser);
        assertEquals(1, aclResult.getNumResults());
        assertEquals(2, aclResult.first().get("external").size());

        aclResult = catalogManager.getSampleManager().getAcls(STUDY,
                Collections.singletonList(clinicalAnalysis.getProband().getSamples().get(0).getId()), "external", false, sessionIdUser);
        assertEquals(1, aclResult.getNumResults());
        assertEquals(2, aclResult.first().get("external").size());
    }

    @Test
    public void createClinicalAnalysisTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        assertEquals(1, dummyEnvironment.getNumResults());
        assertEquals(0, dummyEnvironment.first().getSecondaryInterpretations().size());
        assertNull(dummyEnvironment.first().getInterpretation());

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

        dummyEnvironment = createDummyEnvironment(false, true);
        assertEquals(1, dummyEnvironment.getNumResults());
        assertEquals(0, dummyEnvironment.first().getSecondaryInterpretations().size());
        assertNotNull(dummyEnvironment.first().getInterpretation());
        assertEquals(dummyEnvironment.first().getId() + ".1", dummyEnvironment.first().getInterpretation().getId());
    }

    @Test
    public void updateClinicalAnalysisTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        ClinicalUpdateParams updateParams = new ClinicalUpdateParams()
                .setDescription("My description")
                .setPriority(Enums.Priority.URGENT);

        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(),
                updateParams, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals("My description", ca.getDescription());
        assertEquals(Enums.Priority.URGENT, ca.getPriority());
    }

    @Test
    public void createInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        Interpretation interpretation = new Interpretation().setId("interpretation1");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation1", ca.getInterpretation().getId());

        interpretation = new Interpretation().setId("interpretation2");
        try {
            catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.PRIMARY,
                    QueryOptions.empty(), sessionIdUser);
            fail("There is already a primary interpretation in the clinical analysis. It should not allow this.");
        } catch (CatalogException e) {
            System.out.println("All good");
        }

        // Interpretation has been overrided
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation,
                ParamUtils.SaveInterpretationAs.PRIMARY_OVERWRITE, QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation2", ca.getInterpretation().getId());
        assertEquals(0, ca.getSecondaryInterpretations().size());
        assertEquals(0, catalogManager.getInterpretationManager().search(STUDY,
                new Query(InterpretationDBAdaptor.QueryParams.ID.key(), "interpretation1"), QueryOptions.empty(), sessionIdUser)
                .getNumResults());
        // Old interpretation was deleted
        assertEquals(1, catalogManager.getInterpretationManager().search(STUDY, new Query()
                .append(InterpretationDBAdaptor.QueryParams.ID.key(), "interpretation1")
                .append(InterpretationDBAdaptor.QueryParams.DELETED.key(), true), QueryOptions.empty(), sessionIdUser)
                .getNumResults());

        // Interpretation2 should be moved to secondary interpretations
        interpretation = new Interpretation().setId("interpretation3");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation,
                ParamUtils.SaveInterpretationAs.PRIMARY_OVERWRITE_AND_SAVE, QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation3", ca.getInterpretation().getId());
        assertEquals(1, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation2", ca.getSecondaryInterpretations().get(0).getId());

        // Interpretation4 should be added to secondary interpretations
        interpretation = new Interpretation().setId("interpretation4");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation3", ca.getInterpretation().getId());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(1).getId());

        interpretation = new Interpretation().setId("interpretation5");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, null, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void updateInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        Interpretation interpretation = new Interpretation().setId("interpretation1");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);

        interpretation.setId("interpretation2");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        interpretation.setId("interpretation3");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        interpretation.setId("interpretation4");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation1", ca.getInterpretation().getId());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(2).getId());

        InterpretationUpdateParams params = new InterpretationUpdateParams().setAnalyst(new ClinicalAnalystParam("user2"));
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation1",
                params, null, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation().getAnalyst());
        assertEquals("user2", ca.getInterpretation().getAnalyst().getId());
        assertEquals(2, ca.getInterpretation().getVersion());

        // Update a secondary interpretation
        params = new InterpretationUpdateParams()
                .setDescription("my description");
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation3", params, null, QueryOptions.empty(),
                sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals("my description", ca.getSecondaryInterpretations().get(1).getDescription());
        assertEquals(2, ca.getSecondaryInterpretations().get(1).getVersion());

        // Scalate secondary interpretation to primary and delete
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation3", params,
                ParamUtils.SaveInterpretationAs.PRIMARY_OVERWRITE, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation3", ca.getInterpretation().getId());
        assertEquals(2, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(1).getId());

        // Scalate secondary interpretation to primary and keep
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation4", params,
                ParamUtils.SaveInterpretationAs.PRIMARY_OVERWRITE_AND_SAVE, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation4", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(2, ca.getSecondaryInterpretations().get(1).getVersion());

        // Scalate secondary interpretation to primary
        params = new InterpretationUpdateParams();
        try {
            catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation3", params,
                    ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
            fail("There is already a primary one. It should fail");
        } catch (CatalogException e) {
            System.out.println("all good");
        }

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation4", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(2).getId());
        assertEquals(1, ca.getSecondaryInterpretations().get(2).getVersion());

        // Scalate to primary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation2", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation3", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(1).getId());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation2", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation3", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals("interpretation2", ca.getSecondaryInterpretations().get(2).getId());
        assertEquals(1, ca.getSecondaryInterpretations().get(2).getVersion());

        // Scalate to primary and keep
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY_OVERWRITE_AND_SAVE, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation2", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation3", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(1).getId());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation2", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation3", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals("interpretation2", ca.getSecondaryInterpretations().get(2).getId());
        assertEquals(1, ca.getSecondaryInterpretations().get(2).getVersion());

        // Scalate to primary and overwrite
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY_OVERWRITE, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation2", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation3", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(1).getId());
    }

    @Test
    public void deleteInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        Interpretation interpretation = new Interpretation().setId("interpretation1");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);

        interpretation.setId("interpretation2");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        interpretation.setId("interpretation3");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        interpretation.setId("interpretation4");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        // We update interpretation 1 so a new version is generated
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation1", new InterpretationUpdateParams()
                        .setDescription("my description"), null, QueryOptions.empty(), sessionIdUser);

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals("interpretation1", ca.getInterpretation().getId());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(2).getId());

        OpenCGAResult delete = catalogManager.getInterpretationManager().delete(STUDY, Arrays.asList("interpretation1", "interpretation3"),
                new ObjectMap(), sessionIdUser);
        assertEquals(2, delete.getNumDeleted());

        Query query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList("interpretation1", "interpretation3"))
                .append(Constants.ALL_VERSIONS, true);
        OpenCGAResult<Interpretation> search = catalogManager.getInterpretationManager().search(STUDY, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(0, search.getNumResults());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNull(ca.getInterpretation());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals("interpretation2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals("interpretation4", ca.getSecondaryInterpretations().get(1).getId());

        query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList("interpretation1", "interpretation3"));
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().search(STUDY, query, QueryOptions.empty(),
                sessionIdUser);
        assertEquals(0, result.getNumResults());

        query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList("interpretation1", "interpretation3"))
                .append(InterpretationDBAdaptor.QueryParams.DELETED.key(), true);
        result = catalogManager.getInterpretationManager().search(STUDY, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void searchClinicalAnalysisByProband() throws CatalogException {
        createDummyEnvironment(true, false);
        createDummyEnvironment(false, false);

        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(STUDY,
                new Query(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), "^chil"), QueryOptions.empty(), sessionIdUser);
        assertEquals(2, search.getNumResults());
    }

    @Test
    public void deleteClinicalAnalysisTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);
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
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

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
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);
        Interpretation interpretation = new Interpretation();
        interpretation.setId("myInterpretation");
        catalogManager.getInterpretationManager().create(STUDY, dummyEnvironment.first().getId(), interpretation,
                ParamUtils.SaveInterpretationAs.PRIMARY, new QueryOptions(), sessionIdUser);

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

    @Test
    public void testQueries() throws CatalogException {
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
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);

        QueryOptions includeClinicalIds = ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS;
        // Query by members
        Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.MEMBER.key(), "child3");
        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(STUDY, query, includeClinicalIds, sessionIdUser);
        assertEquals(1, search.getNumResults());

        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.MEMBER.key(), "child1");
        search = catalogManager.getClinicalAnalysisManager().search(STUDY, query, includeClinicalIds, sessionIdUser);
        assertEquals(1, search.getNumResults());

        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.MEMBER.key(), "child4");
        search = catalogManager.getClinicalAnalysisManager().search(STUDY, query, includeClinicalIds, sessionIdUser);
        assertEquals(0, search.getNumResults());

        // Query by samples
        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE.key(), "sample2");
        search = catalogManager.getClinicalAnalysisManager().search(STUDY, query, includeClinicalIds, sessionIdUser);
        assertEquals(1, search.getNumResults());

        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE.key(), "sample5");
        search = catalogManager.getClinicalAnalysisManager().search(STUDY, query, includeClinicalIds, sessionIdUser);
        assertEquals(1, search.getNumResults());

        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE.key(), "sample4");
        search = catalogManager.getClinicalAnalysisManager().search(STUDY, query, includeClinicalIds, sessionIdUser);
        assertEquals(0, search.getNumResults());

        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), "family");
        search = catalogManager.getClinicalAnalysisManager().search(STUDY, query, includeClinicalIds, sessionIdUser);
        assertEquals(1, search.getNumResults());
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
