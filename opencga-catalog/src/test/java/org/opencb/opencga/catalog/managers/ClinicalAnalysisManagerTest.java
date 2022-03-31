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
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.common.Status;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.FlagAnnotation;
import org.opencb.opencga.core.models.common.FlagValue;
import org.opencb.opencga.core.models.common.StatusParam;
import org.opencb.opencga.core.models.common.StatusValue;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileReferenceParam;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsent;
import org.opencb.opencga.core.models.study.configuration.*;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ClinicalAnalysisManagerTest extends GenericTest {

    public final static String STUDY = "user@1000G:phase1";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    protected String sessionIdUser;
    private FamilyManager familyManager;

    private static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, null);
        sessionIdUser = catalogManager.getUserManager().login("user", TestParamConstants.PASSWORD).getToken();

        catalogManager.getUserManager().create("user2", "User Name2", "mail2@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.GUEST,
                null);

        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null, sessionIdUser);
    }

    @After
    public void tearDown() throws Exception {
    }

    private Family getDummyFamily() {
        Disorder disease1 = new Disorder("dis1", "Disease 1", "HPO", null, "", null);
        Disorder disease2 = new Disorder("dis2", "Disease 2", "HPO", null, "", null);

        Individual father = new Individual().setId("father").setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "",
                null)));
        Individual mother = new Individual().setId("mother").setDisorders(Arrays.asList(new Disorder("dis2", "dis2", "OT", null, "",
                null)));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father").setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "",
                        null)))
                .setSamples(Collections.singletonList(new Sample().setId("sample1")));
        Individual relMother = new Individual().setId("mother").setDisorders(Arrays.asList(new Disorder("dis2", "dis2", "OT", null, "",
                        null)))
                .setSamples(Arrays.asList(new Sample().setId("sample3")));

        Individual relChild1 = new Individual().setId("child1")
                .setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null), new Disorder("dis2", "dis2", "OT", null,
                        "", null)))
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

        return new Family("family", "family", null, Arrays.asList(disease1, disease2),
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother), "", -1,
                Collections.emptyList(), Collections.emptyMap());
    }

    private DataResult<Family> createDummyFamily() throws CatalogException {
        Family family = getDummyFamily();

        return familyManager.create(STUDY, family, INCLUDE_RESULT, sessionIdUser);
    }

    private DataResult<ClinicalAnalysis> createDummyEnvironment(boolean createFamily, boolean createDefaultInterpretation) throws CatalogException {

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setStatus(new Status().setId(ClinicalAnalysisStatus.READY_FOR_INTERPRETATION))
                .setId("analysis" + RandomStringUtils.randomAlphanumeric(3))
                .setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setProband(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))));

        if (createFamily) {
            createDummyFamily();
        }
        clinicalAnalysis.setFamily(new Family().setId("family")
                .setMembers(Arrays.asList(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))))));

        return catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, !createDefaultInterpretation,
                INCLUDE_RESULT, sessionIdUser);
    }

    private List<File> registerDummyFiles() throws CatalogException {
        List<File> files = new LinkedList<>();

        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        files.add(catalogManager.getFileManager().link(STUDY, new FileLinkParams(vcfFile, "", "", "", null, null, null,
                null), false, sessionIdUser).first());
        vcfFile = getClass().getResource("/biofiles/family.vcf").getFile();
        files.add(catalogManager.getFileManager().link(STUDY, new FileLinkParams(vcfFile, "", "", "", null, null, null,
                null), false, sessionIdUser).first());
        String bamFile = getClass().getResource("/biofiles/HG00096.chrom20.small.bam").getFile();
        files.add(catalogManager.getFileManager().link(STUDY, new FileLinkParams(bamFile, "", "", "", null, null, null,
                null), false, sessionIdUser).first());
        bamFile = getClass().getResource("/biofiles/NA19600.chrom20.small.bam").getFile();
        files.add(catalogManager.getFileManager().link(STUDY, new FileLinkParams(bamFile, "", "", "", null, null, null,
                null), false, sessionIdUser).first());

        return files;
    }

    @Test
    public void createAndUpdateClinicalAnalysisWithQualityControl() throws CatalogException, InterruptedException {
        Individual individual = new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2")));
        catalogManager.getIndividualManager().create(STUDY, individual, null, sessionIdUser);

        ClinicalComment comment = new ClinicalComment("user", "my comment", new ArrayList<>(), "");
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis" + RandomStringUtils.randomAlphanumeric(3))
                .setDescription("My description").setType(ClinicalAnalysis.Type.SINGLE)
                .setQualityControl(new ClinicalAnalysisQualityControl(ClinicalAnalysisQualityControl.QualityControlSummary.LOW,
                        Collections.singletonList(comment), Collections.emptyList()))
                .setProband(individual);

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, true, INCLUDE_RESULT,
                sessionIdUser).first();

        assertEquals(ClinicalAnalysisQualityControl.QualityControlSummary.LOW, ca.getQualityControl().getSummary());
        assertEquals("my comment", ca.getQualityControl().getComments().get(0).getMessage());
        assertEquals("user", ca.getQualityControl().getComments().get(0).getAuthor());
        assertNotNull(ca.getQualityControl().getComments().get(0).getDate());

        String date = ca.getQualityControl().getComments().get(0).getDate();

        // Sleep 1 second so the date field can be different
        Thread.sleep(1000);
        ClinicalAnalysisQualityControlUpdateParam qualityControlUpdateParam =
                new ClinicalAnalysisQualityControlUpdateParam(ClinicalAnalysisQualityControl.QualityControlSummary.HIGH,
                        Collections.singletonList("other"), Collections.emptyList());
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams().setQualityControl(qualityControlUpdateParam);

        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), updateParams, null, sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), null, sessionIdUser).first();

        assertEquals(ClinicalAnalysisQualityControl.QualityControlSummary.HIGH, ca.getQualityControl().getSummary());
        assertEquals("other", ca.getQualityControl().getComments().get(0).getMessage());
        assertEquals("user", ca.getQualityControl().getComments().get(0).getAuthor());
        assertNotNull(ca.getQualityControl().getComments().get(0).getDate());
        assertNotEquals(date, ca.getQualityControl().getComments().get(0).getDate());
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
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis,
                INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, clinical.getNumResults());
    }

    @Test
    public void createClinicalWithComments() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(STUDY, individual, QueryOptions.empty(), sessionIdUser);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setComments(Arrays.asList(
                        new ClinicalComment("", "My first comment", Arrays.asList("tag1", "tag2"), ""),
                        new ClinicalComment("", "My second comment", Arrays.asList("1tag", "2tag"), "")))
                .setProband(individual);
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis,
                INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, clinical.getNumResults());
        assertEquals(2, clinical.first().getComments().size());
        assertEquals("user", clinical.first().getComments().get(0).getAuthor());
        assertEquals("My first comment", clinical.first().getComments().get(0).getMessage());
        assertEquals(2, clinical.first().getComments().get(0).getTags().size());
        assertEquals("user", clinical.first().getComments().get(1).getAuthor());
        assertEquals("My second comment", clinical.first().getComments().get(1).getMessage());
        assertEquals(2, clinical.first().getComments().get(1).getTags().size());
        assertTrue(StringUtils.isNotEmpty(clinical.first().getComments().get(0).getDate()));
        assertTrue(StringUtils.isNotEmpty(clinical.first().getComments().get(1).getDate()));
        assertNotEquals(clinical.first().getComments().get(0).getDate(), clinical.first().getComments().get(1).getDate());
        assertEquals(Long.parseLong(clinical.first().getComments().get(0).getDate()) + 1,
                Long.parseLong(clinical.first().getComments().get(1).getDate()));
    }

    @Test
    public void createClinicalWithMissingSamplesInFamily() throws CatalogException {
        Family family = getDummyFamily();
        for (Individual member : family.getMembers()) {
            if (!member.getId().equals("child1")) {
                member.setSamples(Collections.emptyList());
            }
        }
        familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);

        // And only add sample to proband
        for (Individual member : family.getMembers()) {
            if (member.getId().equals("child1")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample2")));
            }
        }

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000")
                .setProband(new Individual().setId("child1"));
        clinicalAnalysis.setFamily(family);
        DataResult<ClinicalAnalysis> clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().create(STUDY,
                clinicalAnalysis, INCLUDE_RESULT, sessionIdUser);

        assertEquals("child1", clinicalAnalysisDataResult.first().getFamily().getMembers().get(0).getId());
        assertEquals("father", clinicalAnalysisDataResult.first().getFamily().getMembers().get(1).getId());
        assertEquals("mother", clinicalAnalysisDataResult.first().getFamily().getMembers().get(2).getId());
        assertEquals("child2", clinicalAnalysisDataResult.first().getFamily().getMembers().get(3).getId());
        assertEquals("child3", clinicalAnalysisDataResult.first().getFamily().getMembers().get(4).getId());
        assertEquals("sample2", clinicalAnalysisDataResult.first().getFamily().getMembers().get(0).getSamples().get(0).getId());
        assertTrue(clinicalAnalysisDataResult.first().getFamily().getMembers().get(1).getSamples().isEmpty());
        assertTrue(clinicalAnalysisDataResult.first().getFamily().getMembers().get(2).getSamples().isEmpty());
        assertTrue(clinicalAnalysisDataResult.first().getFamily().getMembers().get(3).getSamples().isEmpty());
        assertTrue(clinicalAnalysisDataResult.first().getFamily().getMembers().get(4).getSamples().isEmpty());
    }

    @Test
    public void updateClinicalComments() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(STUDY, individual, QueryOptions.empty(), sessionIdUser);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setComments(Collections.singletonList(new ClinicalComment("", "My first comment", Arrays.asList("tag1", "tag2"), "")))
                .setProband(individual);

        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);

        List<ClinicalCommentParam> commentParamList = new ArrayList<>();
        commentParamList.add(new ClinicalCommentParam("My second comment", Arrays.asList("myTag")));
        commentParamList.add(new ClinicalCommentParam("My third comment", Arrays.asList("myTag2")));

        ObjectMap actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setComments(commentParamList), options, sessionIdUser);

        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(),
                QueryOptions.empty(), sessionIdUser);
        assertEquals(1, clinical.getNumResults());
        assertEquals(3, clinical.first().getComments().size());
        assertEquals("user", clinical.first().getComments().get(1).getAuthor());
        assertEquals("My second comment", clinical.first().getComments().get(1).getMessage());
        assertEquals(1, clinical.first().getComments().get(1).getTags().size());
        assertEquals("myTag", clinical.first().getComments().get(1).getTags().get(0));
        assertTrue(StringUtils.isNotEmpty(clinical.first().getComments().get(1).getDate()));

        assertEquals("user", clinical.first().getComments().get(2).getAuthor());
        assertEquals("My third comment", clinical.first().getComments().get(2).getMessage());
        assertEquals(1, clinical.first().getComments().get(2).getTags().size());
        assertEquals("myTag2", clinical.first().getComments().get(2).getTags().get(0));
        assertTrue(StringUtils.isNotEmpty(clinical.first().getComments().get(2).getDate()));

        // Replace second and third comment
        commentParamList = Arrays.asList(
                new ClinicalCommentParam("My updated second comment", Arrays.asList("myTag", "myOtherTag"),
                        clinical.first().getComments().get(1).getDate()),
                new ClinicalCommentParam("My also updated third comment", Arrays.asList("myTag2", "myOtherTag2"),
                        clinical.first().getComments().get(2).getDate())
        );
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveReplaceAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setComments(commentParamList), options, sessionIdUser);
        clinical = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, clinical.getNumResults());
        assertEquals(3, clinical.first().getComments().size());
        assertEquals("user", clinical.first().getComments().get(1).getAuthor());
        assertEquals("My updated second comment", clinical.first().getComments().get(1).getMessage());
        assertEquals(2, clinical.first().getComments().get(1).getTags().size());
        assertEquals("myTag", clinical.first().getComments().get(1).getTags().get(0));
        assertEquals("myOtherTag", clinical.first().getComments().get(1).getTags().get(1));
        assertTrue(StringUtils.isNotEmpty(clinical.first().getComments().get(1).getDate()));

        assertEquals("user", clinical.first().getComments().get(2).getAuthor());
        assertEquals("My also updated third comment", clinical.first().getComments().get(2).getMessage());
        assertEquals(2, clinical.first().getComments().get(2).getTags().size());
        assertEquals("myTag2", clinical.first().getComments().get(2).getTags().get(0));
        assertEquals("myOtherTag2", clinical.first().getComments().get(2).getTags().get(1));
        assertTrue(StringUtils.isNotEmpty(clinical.first().getComments().get(2).getDate()));

        // Remove first comment
        commentParamList = Arrays.asList(
                ClinicalCommentParam.of(clinical.first().getComments().get(0)),
                ClinicalCommentParam.of(clinical.first().getComments().get(2))
        );
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setComments(commentParamList), options, sessionIdUser);

        clinical = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, clinical.getNumResults());
        assertEquals(1, clinical.first().getComments().size());
        assertEquals("user", clinical.first().getComments().get(0).getAuthor());
        assertEquals("My updated second comment", clinical.first().getComments().get(0).getMessage());
        assertEquals(2, clinical.first().getComments().get(0).getTags().size());
        assertEquals("myTag", clinical.first().getComments().get(0).getTags().get(0));
        assertEquals("myOtherTag", clinical.first().getComments().get(0).getTags().get(1));
        assertTrue(StringUtils.isNotEmpty(clinical.first().getComments().get(0).getDate()));

        commentParamList = Arrays.asList(
                ClinicalCommentParam.of(clinical.first().getComments().get(0))
        );
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setComments(commentParamList), options, sessionIdUser);

        clinical = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, clinical.getNumResults());
        assertEquals(0, clinical.first().getComments().size());

        // Remove dummy comment with no date
        commentParamList = Collections.singletonList(new ClinicalCommentParam("", Collections.emptyList()));
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                    .setComments(commentParamList), options, sessionIdUser);
            fail("It should fail because the comment has no date");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("date"));
        }

        // Replace comment with no date
        commentParamList = Collections.singletonList(new ClinicalCommentParam("", Collections.emptyList()));
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveReplaceAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("date");
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setComments(commentParamList), options, sessionIdUser);
    }

    @Test
    public void createRepeatedInterpretationPrimaryFindings() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(STUDY, individual, QueryOptions.empty(), sessionIdUser);

        List<ClinicalVariant> findingList = new ArrayList<>();
        VariantAvro variantAvro = new VariantAvro("id1", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        ClinicalVariant cv = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "",
                ClinicalVariant.Status.NOT_REVIEWED, null);
        findingList.add(cv);
        findingList.add(cv);
        variantAvro = new VariantAvro("id2", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        cv = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "", ClinicalVariant.Status.NOT_REVIEWED,
                null);
        findingList.add(cv);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual)
                .setInterpretation(new Interpretation()
                        .setPrimaryFindings(findingList)
                );
        thrown.expect(CatalogException.class);
        thrown.expectMessage("repeated");
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createRepeatedInterpretationSecondaryFindings() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(STUDY, individual, QueryOptions.empty(), sessionIdUser);

        List<ClinicalVariant> findingList = new ArrayList<>();
        VariantAvro variantAvro = new VariantAvro("id1", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        ClinicalVariant cv = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "",
                ClinicalVariant.Status.NOT_REVIEWED, null);
        findingList.add(cv);
        findingList.add(cv);
        variantAvro = new VariantAvro("id2", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        cv = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "", ClinicalVariant.Status.NOT_REVIEWED,
                null);
        findingList.add(cv);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual)
                .setInterpretation(new Interpretation()
                        .setSecondaryFindings(findingList)
                );
        thrown.expect(CatalogException.class);
        thrown.expectMessage("repeated");
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void updatePrimaryFindings() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(STUDY, individual, QueryOptions.empty(), sessionIdUser);

        List<ClinicalVariant> findingList = new ArrayList<>();
        VariantAvro variantAvro = new VariantAvro("id1", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        ClinicalVariant cv1 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "",
                ClinicalVariant.Status.NOT_REVIEWED, null);
        findingList.add(cv1);
        variantAvro = new VariantAvro("id2", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariant cv2 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "",
                ClinicalVariant.Status.NOT_REVIEWED, null);
        findingList.add(cv2);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual)
                .setInterpretation(new Interpretation()
                        .setPrimaryFindings(findingList)
                );
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);

        Interpretation interpretation = catalogManager.getInterpretationManager().get(STUDY, clinicalAnalysis.getId() + ".1", QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(2, interpretation.getPrimaryFindings().size());
        assertEquals(2, interpretation.getStats().getPrimaryFindings().getNumVariants());
        assertEquals(2, (int) interpretation.getStats().getPrimaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Add new finding
        findingList = new ArrayList<>();
        variantAvro = new VariantAvro("id3", null, "chr3", 2, 3, "", "", "+", null, 1, null, null, null);
        evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method2");
        ClinicalVariant cv3 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "",
                ClinicalVariant.Status.NOT_REVIEWED, null);
        findingList.add(cv3);

        InterpretationUpdateParams updateParams = new InterpretationUpdateParams()
                .setPrimaryFindings(findingList);
        ObjectMap actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null, options,
                sessionIdUser);
        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(3, interpretation.getPrimaryFindings().size());
        assertEquals("method2", interpretation.getPrimaryFindings().get(2).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id3", interpretation.getPrimaryFindings().get(2).getId());
        assertEquals(3, interpretation.getStats().getPrimaryFindings().getNumVariants());
        assertEquals(3, (int) interpretation.getStats().getPrimaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Add existing finding
        cv3.setDiscussion("My discussion");
        try {
            catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null,
                    options, sessionIdUser);
            fail("It should not allow adding an already existing finding");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("repeated"));
        }

        // Remove findings
        updateParams = new InterpretationUpdateParams()
                .setPrimaryFindings(Arrays.asList(cv1, cv3));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null, options,
                sessionIdUser);
        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(1, interpretation.getPrimaryFindings().size());
        assertEquals("method", interpretation.getPrimaryFindings().get(0).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id2", interpretation.getPrimaryFindings().get(0).getId());
        assertEquals(1, interpretation.getStats().getPrimaryFindings().getNumVariants());
        assertEquals(1, (int) interpretation.getStats().getPrimaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Set findings
        updateParams = new InterpretationUpdateParams()
                .setPrimaryFindings(Arrays.asList(cv1, cv2, cv3));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null, options,
                sessionIdUser);
        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(3, interpretation.getPrimaryFindings().size());
        assertEquals("method", interpretation.getPrimaryFindings().get(0).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id1", interpretation.getPrimaryFindings().get(0).getId());
        assertEquals("method", interpretation.getPrimaryFindings().get(1).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id2", interpretation.getPrimaryFindings().get(1).getId());
        assertEquals("method2", interpretation.getPrimaryFindings().get(2).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id3", interpretation.getPrimaryFindings().get(2).getId());
        assertEquals(3, interpretation.getStats().getPrimaryFindings().getNumVariants());
        assertEquals(3, (int) interpretation.getStats().getPrimaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Replace findings
        cv2.setEvidences(Collections.singletonList(new ClinicalVariantEvidence().setInterpretationMethodName("AnotherMethodName")));
        cv3.setEvidences(Collections.singletonList(new ClinicalVariantEvidence().setInterpretationMethodName("YetAnotherMethodName")));

        updateParams = new InterpretationUpdateParams()
                .setPrimaryFindings(Arrays.asList(cv2, cv3));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null, options,
                sessionIdUser);
        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(3, interpretation.getPrimaryFindings().size());
        assertEquals("method", interpretation.getPrimaryFindings().get(0).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id1", interpretation.getPrimaryFindings().get(0).getId());
        assertEquals("AnotherMethodName", interpretation.getPrimaryFindings().get(1).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id2", interpretation.getPrimaryFindings().get(1).getId());
        assertEquals("YetAnotherMethodName",
                interpretation.getPrimaryFindings().get(2).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id3", interpretation.getPrimaryFindings().get(2).getId());
        assertEquals(3, interpretation.getStats().getPrimaryFindings().getNumVariants());
        assertEquals(3, (int) interpretation.getStats().getPrimaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Remove finding with missing id
        variantAvro = new VariantAvro("", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        cv1 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "", ClinicalVariant.Status.NOT_REVIEWED,
                null);

        updateParams = new InterpretationUpdateParams()
                .setPrimaryFindings(Collections.singletonList(cv1));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null,
                    options, sessionIdUser);
            fail("It should fail because finding id is missing");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("id"));
        }

        // Remove finding with missing id
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null,
                    options, sessionIdUser);
            fail("It should fail because finding id is missing");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("id"));
        }
    }

    @Test
    public void updateSecondaryFindings() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(STUDY, individual, QueryOptions.empty(), sessionIdUser);

        List<ClinicalVariant> findingList = new ArrayList<>();
        VariantAvro variantAvro = new VariantAvro("id1", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        ClinicalVariant cv1 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "",
                ClinicalVariant.Status.NOT_REVIEWED, null);
        findingList.add(cv1);
        variantAvro = new VariantAvro("id2", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariant cv2 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "",
                ClinicalVariant.Status.NOT_REVIEWED, null);
        findingList.add(cv2);

        ClinicalAnalysis ca = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual)
                .setInterpretation(new Interpretation()
                        .setSecondaryFindings(findingList)
                );
        catalogManager.getClinicalAnalysisManager().create(STUDY, ca, QueryOptions.empty(), sessionIdUser);

        Interpretation interpretation = catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(2, interpretation.getSecondaryFindings().size());
        assertNotNull(interpretation.getStats());
        assertEquals(2, interpretation.getStats().getSecondaryFindings().getNumVariants());
        assertEquals(2,
                (int) interpretation.getStats().getSecondaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Add new finding
        findingList = new ArrayList<>();
        variantAvro = new VariantAvro("id3", null, "chr3", 2, 3, "", "", "+", null, 1, null, null, null);
        evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method2");
        ClinicalVariant cv3 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "",
                ClinicalVariant.Status.NOT_REVIEWED, null);
        findingList.add(cv3);

        InterpretationUpdateParams updateParams = new InterpretationUpdateParams()
                .setSecondaryFindings(findingList);
        ObjectMap actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", updateParams, null, options,
                sessionIdUser);
        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(3, interpretation.getSecondaryFindings().size());
        assertEquals("method2", interpretation.getSecondaryFindings().get(2).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id3", interpretation.getSecondaryFindings().get(2).getId());
        assertNotNull(interpretation.getStats());
        assertEquals(3, interpretation.getStats().getSecondaryFindings().getNumVariants());
        assertEquals(3,
                (int) interpretation.getStats().getSecondaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Add existing finding
        try {
            catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", updateParams, null,
                    options, sessionIdUser);
            fail("It should not allow adding an already existing finding");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("repeated"));
        }

        // Remove findings
        updateParams = new InterpretationUpdateParams()
                .setSecondaryFindings(Arrays.asList(cv1, cv3));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", updateParams, null, options,
                sessionIdUser);
        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(1, interpretation.getSecondaryFindings().size());
        assertEquals("method", interpretation.getSecondaryFindings().get(0).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id2", interpretation.getSecondaryFindings().get(0).getId());
        assertNotNull(interpretation.getStats());
        assertEquals(1, interpretation.getStats().getSecondaryFindings().getNumVariants());
        assertEquals(1,
                (int) interpretation.getStats().getSecondaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Set findings
        updateParams = new InterpretationUpdateParams()
                .setSecondaryFindings(Arrays.asList(cv1, cv2, cv3));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", updateParams, null, options,
                sessionIdUser);
        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(3, interpretation.getSecondaryFindings().size());
        assertEquals("method", interpretation.getSecondaryFindings().get(0).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id1", interpretation.getSecondaryFindings().get(0).getId());
        assertEquals("method", interpretation.getSecondaryFindings().get(1).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id2", interpretation.getSecondaryFindings().get(1).getId());
        assertEquals("method2", interpretation.getSecondaryFindings().get(2).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id3", interpretation.getSecondaryFindings().get(2).getId());
        assertNotNull(interpretation.getStats());
        assertEquals(3, interpretation.getStats().getSecondaryFindings().getNumVariants());
        assertEquals(3,
                (int) interpretation.getStats().getSecondaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Replace findings
        cv2.setEvidences(Collections.singletonList(new ClinicalVariantEvidence().setInterpretationMethodName("AnotherMethodName")));
        cv3.setEvidences(Collections.singletonList(new ClinicalVariantEvidence().setInterpretationMethodName("YetAnotherMethodName")));

        updateParams = new InterpretationUpdateParams()
                .setSecondaryFindings(Arrays.asList(cv2, cv3));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", updateParams, null, options,
                sessionIdUser);
        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(3, interpretation.getSecondaryFindings().size());
        assertEquals("method", interpretation.getSecondaryFindings().get(0).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id1", interpretation.getSecondaryFindings().get(0).getId());
        assertEquals("AnotherMethodName", interpretation.getSecondaryFindings().get(1).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id2", interpretation.getSecondaryFindings().get(1).getId());
        assertEquals("YetAnotherMethodName",
                interpretation.getSecondaryFindings().get(2).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id3", interpretation.getSecondaryFindings().get(2).getId());
        assertNotNull(interpretation.getStats());
        assertEquals(3, interpretation.getStats().getSecondaryFindings().getNumVariants());
        assertEquals(3,
                (int) interpretation.getStats().getSecondaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Remove finding with missing id
        variantAvro = new VariantAvro("", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        cv1 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, "", ClinicalVariant.Status.NOT_REVIEWED,
                null);

        updateParams = new InterpretationUpdateParams()
                .setSecondaryFindings(Collections.singletonList(cv1));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", updateParams, null,
                    options, sessionIdUser);
            fail("It should fail because finding id is missing");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("id"));
        }

        // Remove finding with missing id
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", updateParams, null,
                    options, sessionIdUser);
            fail("It should fail because finding id is missing");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("id"));
        }
    }

    @Test
    public void updateInterpretationComments() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(STUDY, individual, QueryOptions.empty(), sessionIdUser);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual)
                .setInterpretation(new Interpretation()
                        .setComments(Collections.singletonList(new ClinicalComment("", "My first comment", Arrays.asList("tag1", "tag2"),
                                "")))
                );
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);

        List<ClinicalCommentParam> commentParamList = new ArrayList<>();
        commentParamList.add(new ClinicalCommentParam("My second comment", Arrays.asList("myTag")));
        commentParamList.add(new ClinicalCommentParam("My third comment", Arrays.asList("myTag2")));

        ObjectMap actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", new InterpretationUpdateParams()
                .setComments(commentParamList), null, options, sessionIdUser);

        OpenCGAResult<Interpretation> interpretation = catalogManager.getInterpretationManager().get(STUDY, clinicalAnalysis.getId() + ".1",
                QueryOptions.empty(), sessionIdUser);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(3, interpretation.first().getComments().size());
        assertEquals("user", interpretation.first().getComments().get(1).getAuthor());
        assertEquals("My second comment", interpretation.first().getComments().get(1).getMessage());
        assertEquals(1, interpretation.first().getComments().get(1).getTags().size());
        assertEquals("myTag", interpretation.first().getComments().get(1).getTags().get(0));
        assertTrue(StringUtils.isNotEmpty(interpretation.first().getComments().get(1).getDate()));

        assertEquals("user", interpretation.first().getComments().get(2).getAuthor());
        assertEquals("My third comment", interpretation.first().getComments().get(2).getMessage());
        assertEquals(1, interpretation.first().getComments().get(2).getTags().size());
        assertEquals("myTag2", interpretation.first().getComments().get(2).getTags().get(0));
        assertTrue(StringUtils.isNotEmpty(interpretation.first().getComments().get(2).getDate()));

        // Replace second and third comment
        commentParamList = Arrays.asList(
                new ClinicalCommentParam("My updated second comment", Arrays.asList("myTag", "myOtherTag"),
                        interpretation.first().getComments().get(1).getDate()),
                new ClinicalCommentParam("My also updated third comment", Arrays.asList("myTag2", "myOtherTag2"),
                        interpretation.first().getComments().get(2).getDate())
        );
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveReplaceAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", new InterpretationUpdateParams()
                .setComments(commentParamList), null, options, sessionIdUser);
        interpretation = catalogManager.getInterpretationManager().get(STUDY, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), sessionIdUser);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(3, interpretation.first().getComments().size());
        assertEquals("user", interpretation.first().getComments().get(1).getAuthor());
        assertEquals("My updated second comment", interpretation.first().getComments().get(1).getMessage());
        assertEquals(2, interpretation.first().getComments().get(1).getTags().size());
        assertEquals("myTag", interpretation.first().getComments().get(1).getTags().get(0));
        assertEquals("myOtherTag", interpretation.first().getComments().get(1).getTags().get(1));
        assertTrue(StringUtils.isNotEmpty(interpretation.first().getComments().get(1).getDate()));

        assertEquals("user", interpretation.first().getComments().get(2).getAuthor());
        assertEquals("My also updated third comment", interpretation.first().getComments().get(2).getMessage());
        assertEquals(2, interpretation.first().getComments().get(2).getTags().size());
        assertEquals("myTag2", interpretation.first().getComments().get(2).getTags().get(0));
        assertEquals("myOtherTag2", interpretation.first().getComments().get(2).getTags().get(1));
        assertTrue(StringUtils.isNotEmpty(interpretation.first().getComments().get(2).getDate()));

        // Remove first comment
        commentParamList = Arrays.asList(
                ClinicalCommentParam.of(interpretation.first().getComments().get(0)),
                ClinicalCommentParam.of(interpretation.first().getComments().get(2))
        );
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", new InterpretationUpdateParams()
                .setComments(commentParamList), null, options, sessionIdUser);

        interpretation = catalogManager.getInterpretationManager().get(STUDY, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), sessionIdUser);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(1, interpretation.first().getComments().size());
        assertEquals("user", interpretation.first().getComments().get(0).getAuthor());
        assertEquals("My updated second comment", interpretation.first().getComments().get(0).getMessage());
        assertEquals(2, interpretation.first().getComments().get(0).getTags().size());
        assertEquals("myTag", interpretation.first().getComments().get(0).getTags().get(0));
        assertTrue(StringUtils.isNotEmpty(interpretation.first().getComments().get(0).getDate()));

        commentParamList = Arrays.asList(
                ClinicalCommentParam.of(interpretation.first().getComments().get(0))
        );
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", new InterpretationUpdateParams()
                .setComments(commentParamList), null, options, sessionIdUser);

        interpretation = catalogManager.getInterpretationManager().get(STUDY, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), sessionIdUser);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(0, interpretation.first().getComments().size());

        // Remove dummy comment with no date
        commentParamList = Collections.singletonList(new ClinicalCommentParam("", Collections.emptyList()));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1",
                    new InterpretationUpdateParams()
                            .setComments(commentParamList), null, options, sessionIdUser);
            fail("It should fail because the comment has no date");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("date"));
        }

        // Replace comment with no date
        commentParamList = Collections.singletonList(new ClinicalCommentParam("", Collections.emptyList()));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveReplaceAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("date");
        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", new InterpretationUpdateParams()
                .setComments(commentParamList), null, options, sessionIdUser);
    }

    @Test
    public void assignPermissions() throws CatalogException {
        ClinicalAnalysis clinicalAnalysis = createDummyEnvironment(true, false).first();
        catalogManager.getUserManager().create("external", "User Name", "external@mail.com", TestParamConstants.PASSWORD, "", null,
                Account.AccountType.GUEST, null);

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
                new AclParams(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.DELETE.name()), ParamUtils.AclAction.ADD, false,
                sessionIdUser);

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
                new AclParams(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.DELETE.name()), ParamUtils.AclAction.ADD, true,
                sessionIdUser);

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

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setDescription("My description")
                .setPriority(new PriorityParam("URGENT"));

        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(),
                updateParams, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals("My description", ca.getDescription());
        assertEquals("URGENT", ca.getPriority().getId());
    }

    @Test
    public void updateCustomStatusTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(STUDY, QueryOptions.empty(), sessionIdUser).first();
        ClinicalAnalysisStudyConfiguration configuration = study.getInternal().getConfiguration().getClinical();

        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        StatusValue status = configuration.getStatus().get(dummyEnvironment.first().getType()).get(0);

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setStatus(new StatusParam(status.getId()));
        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(),
                updateParams, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(status.getId(), ca.getStatus().getId());
        assertEquals(status.getDescription(), ca.getStatus().getDescription());
        assertNotNull(ca.getStatus().getDate());
    }

    @Test
    public void updateCustomPriorityTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(STUDY, QueryOptions.empty(), sessionIdUser).first();
        ClinicalAnalysisStudyConfiguration configuration = study.getInternal().getConfiguration().getClinical();

        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        ClinicalPriorityValue priority = configuration.getPriorities().get(1);

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setPriority(new PriorityParam(priority.getId()));
        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(),
                updateParams, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(priority.getId(), ca.getPriority().getId());
        assertEquals(priority.getDescription(), ca.getPriority().getDescription());
        assertEquals(priority.getRank(), ca.getPriority().getRank());
        assertNotNull(ca.getPriority().getDate());
    }

    @Test
    public void updateCustomFlagTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(STUDY, QueryOptions.empty(), sessionIdUser).first();
        ClinicalAnalysisStudyConfiguration configuration = study.getInternal().getConfiguration().getClinical();

        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        FlagValue flag1 = configuration.getFlags().get(dummyEnvironment.first().getType()).get(1);
        FlagValue flag2 = configuration.getFlags().get(dummyEnvironment.first().getType()).get(3);
        FlagValue flag3 = configuration.getFlags().get(dummyEnvironment.first().getType()).get(4);

        ObjectMap actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key(), ParamUtils.BasicUpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setFlags(Arrays.asList(new FlagValueParam(flag1.getId()), new FlagValueParam(flag1.getId()),
                        new FlagValueParam(flag2.getId())));
        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(),
                updateParams, options, sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        updateParams = new ClinicalAnalysisUpdateParams()
                .setFlags(Arrays.asList(new FlagValueParam(flag2.getId()), new FlagValueParam(flag3.getId())));
        update = catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(), updateParams, options,
                sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(3, ca.getFlags().size());
        for (FlagAnnotation flag : ca.getFlags()) {
            FlagValue flagToCompare = null;
            if (flag.getId().equals(flag1.getId())) {
                flagToCompare = flag1;
            } else if (flag.getId().equals(flag2.getId())) {
                flagToCompare = flag2;
            } else if (flag.getId().equals(flag3.getId())) {
                flagToCompare = flag3;
            } else {
                fail("It should match one of those 3 flags");
            }
            assertEquals(flagToCompare.getDescription(), flag.getDescription());
            assertNotNull(flag.getDate());
        }

        // Set other flags
        flag1 = configuration.getFlags().get(dummyEnvironment.first().getType()).get(0);
        flag2 = configuration.getFlags().get(dummyEnvironment.first().getType()).get(2);

        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        updateParams = new ClinicalAnalysisUpdateParams()
                .setFlags(Arrays.asList(new FlagValueParam(flag1.getId()), new FlagValueParam(flag2.getId())));
        update = catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(), updateParams, options,
                sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(2, ca.getFlags().size());
        assertEquals(flag1.getId(), ca.getFlags().get(0).getId());
        assertEquals(flag1.getDescription(), ca.getFlags().get(0).getDescription());
        assertNotNull(ca.getFlags().get(0).getDate());

        assertEquals(flag2.getId(), ca.getFlags().get(1).getId());
        assertEquals(flag2.getDescription(), ca.getFlags().get(1).getDescription());
        assertNotNull(ca.getFlags().get(1).getDate());

        // Remove flag1
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key(), ParamUtils.BasicUpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        updateParams = new ClinicalAnalysisUpdateParams()
                .setFlags(Collections.singletonList(new FlagValueParam(flag1.getId())));
        update = catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(), updateParams, options,
                sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(1, ca.getFlags().size());
        assertEquals(flag2.getId(), ca.getFlags().get(0).getId());
        assertEquals(flag2.getDescription(), ca.getFlags().get(0).getDescription());
        assertNotNull(ca.getFlags().get(0).getDate());
    }

    @Test
    public void updateCustomConsentTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(STUDY, QueryOptions.empty(), sessionIdUser).first();
        ClinicalAnalysisStudyConfiguration configuration = study.getInternal().getConfiguration().getClinical();

        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        List<ClinicalConsent> consents = configuration.getConsent().getConsents();
        Map<String, ClinicalConsent> consentMap = new HashMap<>();
        for (ClinicalConsent consent : consents) {
            consentMap.put(consent.getId(), consent);
        }

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setConsent(new ClinicalConsentAnnotationParam(Collections.singletonList(
                        new ClinicalConsentAnnotationParam.ClinicalConsentParam(consents.get(1).getId(), ClinicalConsentParam.Value.YES))));
        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(),
                updateParams, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(consents.size(), ca.getConsent().getConsents().size());
        assertNotNull(ca.getConsent().getDate());

        for (ClinicalConsentParam consent : ca.getConsent().getConsents()) {
            assertTrue(consentMap.containsKey(consent.getId()));
            assertEquals(consentMap.get(consent.getId()).getDescription(), consent.getDescription());
            assertEquals(consentMap.get(consent.getId()).getName(), consent.getName());
            if (consent.getId().equals(consents.get(1).getId())) {
                assertEquals(ClinicalConsentParam.Value.YES, consent.getValue());
            } else {
                assertEquals(ClinicalConsentParam.Value.UNKNOWN, consent.getValue());
            }
        }
    }

    @Test
    public void updateInterpretationCustomStatusTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(STUDY, QueryOptions.empty(), sessionIdUser).first();
        InterpretationStudyConfiguration configuration = study.getInternal().getConfiguration().getClinical().getInterpretation();

        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, true);
        StatusValue status = configuration.getStatus().get(dummyEnvironment.first().getType()).get(0);

        InterpretationUpdateParams updateParams = new InterpretationUpdateParams()
                .setStatus(new StatusParam(status.getId()));
        OpenCGAResult<Interpretation> update = catalogManager.getInterpretationManager().update(STUDY, dummyEnvironment.first().getId(),
                dummyEnvironment.first().getInterpretation().getId(), updateParams, null, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, update.getNumUpdated());

        Interpretation interpretation = catalogManager.getInterpretationManager().get(STUDY,
                dummyEnvironment.first().getInterpretation().getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals(status.getId(), interpretation.getStatus().getId());
        assertEquals(status.getDescription(), interpretation.getStatus().getDescription());
        assertNotNull(interpretation.getStatus().getDate());
    }

    @Test
    public void createInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".1", ca.getInterpretation().getId());

        // Delete old interpretation and create a new primary one
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(),
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getInterpretationManager().delete(STUDY, ca.getId(), Collections.singletonList(ca.getId() + ".1"), sessionIdUser);

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(0, ca.getSecondaryInterpretations().size());
        assertEquals(0, catalogManager.getInterpretationManager().search(STUDY,
                        new Query(InterpretationDBAdaptor.QueryParams.ID.key(), ca.getId() + ".1"), QueryOptions.empty(), sessionIdUser)
                .getNumResults());
        // Old interpretation was deleted
        assertEquals(1, catalogManager.getInterpretationManager().search(STUDY, new Query()
                        .append(InterpretationDBAdaptor.QueryParams.ID.key(), ca.getId() + ".1")
                        .append(InterpretationDBAdaptor.QueryParams.DELETED.key(), true), QueryOptions.empty(), sessionIdUser)
                .getNumResults());

        // Interpretation2 should be moved to secondary interpretations
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(),
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".3", ca.getInterpretation().getId());
        assertEquals(1, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());

        // Interpretation4 should be added to secondary interpretations
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(),
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".3", ca.getInterpretation().getId());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing");
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), null, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void clearPrimaryInterpretation() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        Interpretation interpretation = new Interpretation()
                .setDescription("description")
                .setMethod(new InterpretationMethod("name", "", "", Collections.emptyList()))
                .setPrimaryFindings(Collections.singletonList(new ClinicalVariant(new VariantAvro("id", Collections.emptyList(), "chr1",
                        1, 2, "ref", "alt", "+", null, 1, null, null, null))))
                .setSecondaryFindings(Collections.singletonList(new ClinicalVariant(new VariantAvro("id", Collections.emptyList(), "chr1"
                        , 1, 2, "ref", "alt", "+", null, 1, null, null, null))))
                .setComments(Collections.singletonList(new ClinicalComment("me", "message", null, TimeUtils.getTime())));
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);

        Interpretation interpretationResult = catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1",
                QueryOptions.empty(), sessionIdUser).first();
        assertEquals(ca.getId() + ".1", interpretationResult.getId());
        assertEquals(1, interpretationResult.getVersion());
        assertEquals("description", interpretationResult.getDescription());
        assertNotNull(interpretationResult.getMethod());
        assertEquals("name", interpretationResult.getMethod().getName());
        assertEquals(1, interpretationResult.getPrimaryFindings().size());
        assertEquals(1, interpretationResult.getSecondaryFindings().size());
        assertEquals(1, interpretationResult.getComments().size());

        catalogManager.getInterpretationManager().clear(STUDY, ca.getId(), Collections.singletonList(ca.getId() + ".1"), sessionIdUser);
        interpretationResult = catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(ca.getId() + ".1", interpretationResult.getId());
        assertEquals(2, interpretationResult.getVersion());
        assertEquals("", interpretationResult.getDescription());
        assertNotNull(interpretationResult.getMethod());
        assertEquals("", interpretationResult.getMethod().getName());
        assertEquals(0, interpretationResult.getPrimaryFindings().size());
        assertEquals(0, interpretationResult.getSecondaryFindings().size());
        assertEquals(1, interpretationResult.getComments().size());
    }

    @Test
    public void clearSecondaryInterpretation() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        Interpretation interpretation = new Interpretation()
                .setDescription("description")
                .setMethod(new InterpretationMethod("name", "", "", Collections.emptyList()))
                .setPrimaryFindings(Collections.singletonList(new ClinicalVariant(new VariantAvro("id", Collections.emptyList(), "chr1",
                        1, 2, "ref", "alt", "+", null, 1, null, null, null))))
                .setSecondaryFindings(Collections.singletonList(new ClinicalVariant(new VariantAvro("id", Collections.emptyList(), "chr1"
                        , 1, 2, "ref", "alt", "+", null, 1, null, null, null))))
                .setComments(Collections.singletonList(new ClinicalComment("me", "message", null, TimeUtils.getTime())));
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        Interpretation interpretationResult = catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1",
                QueryOptions.empty(), sessionIdUser).first();
        assertEquals(ca.getId() + ".1", interpretationResult.getId());
        assertEquals(1, interpretationResult.getVersion());
        assertEquals("description", interpretationResult.getDescription());
        assertNotNull(interpretationResult.getMethod());
        assertEquals("name", interpretationResult.getMethod().getName());
        assertEquals(1, interpretationResult.getPrimaryFindings().size());
        assertEquals(1, interpretationResult.getSecondaryFindings().size());
        assertEquals(1, interpretationResult.getComments().size());

        catalogManager.getInterpretationManager().clear(STUDY, ca.getId(), Collections.singletonList(ca.getId() + ".1"), sessionIdUser);
        interpretationResult = catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(ca.getId() + ".1", interpretationResult.getId());
        assertEquals(2, interpretationResult.getVersion());
        assertEquals("", interpretationResult.getDescription());
        assertNotNull(interpretationResult.getMethod());
        assertEquals("", interpretationResult.getMethod().getName());
        assertEquals(0, interpretationResult.getPrimaryFindings().size());
        assertEquals(0, interpretationResult.getSecondaryFindings().size());
        assertEquals(1, interpretationResult.getComments().size());
    }

    @Test
    public void updateInterpretationFindingsTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);

        ClinicalVariant clinicalVariant = new ClinicalVariant();
        clinicalVariant.setId("variantId");
        clinicalVariant.setChromosome("chr1");
        clinicalVariant.setStart(2);
        clinicalVariant.setEnd(3);
        clinicalVariant.setLength(2);

        InterpretationUpdateParams params = new InterpretationUpdateParams()
                .setPrimaryFindings(Collections.singletonList(clinicalVariant));
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1",
                params, null, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("repeated");
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", params, null, QueryOptions.empty(),
                sessionIdUser);
    }

//    @Test
//    public void mergeInterpretationFindingsTest() throws CatalogException {
//        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();
//
//        Interpretation interpretation = new Interpretation().setId("interpretation1");
//        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.PRIMARY,
//                QueryOptions.empty(), sessionIdUser);
//
//        ClinicalVariant clinicalVariant = new ClinicalVariant();
//        clinicalVariant.setId("variantId");
//        clinicalVariant.setInterpretationMethodNames(Collections.singletonList("method1"));
//        clinicalVariant.setChromosome("chr1");
//        clinicalVariant.setStart(2);
//        clinicalVariant.setEnd(3);
//        clinicalVariant.setLength(2);
//
//        InterpretationUpdateParams params = new InterpretationUpdateParams()
//                .setMethods(Collections.singletonList(new InterpretationMethod("method1", Collections.emptyMap(), Collections.emptyList(),
//                        Collections.emptyList())))
//                .setPrimaryFindings(Collections.singletonList(clinicalVariant));
//        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), "interpretation1",
//                params, null, QueryOptions.empty(), sessionIdUser);
//        assertEquals(1, result.getNumUpdated());
//
//        List<ClinicalVariant> variantList = new ArrayList<>();
//        clinicalVariant.setInterpretationMethodNames(Collections.singletonList("method2"));
//        variantList.add(clinicalVariant);
//
//        clinicalVariant = new ClinicalVariant();
//        clinicalVariant.setId("variantId2");
//        clinicalVariant.setInterpretationMethodNames(Collections.singletonList("method2"));
//        clinicalVariant.setChromosome("chr2");
//        clinicalVariant.setStart(2);
//        clinicalVariant.setEnd(3);
//        clinicalVariant.setLength(2);
//        variantList.add(clinicalVariant);
//        Interpretation interpretationAux = new Interpretation()
//                .setPrimaryFindings(variantList)
//                .setMethods(
//                        Arrays.asList(
//                                new InterpretationMethod("method1", Collections.emptyMap(), Collections.emptyList(),
//                                        Collections.emptyList()),
//                                new InterpretationMethod("method2", Collections.emptyMap(), Collections.emptyList(),
//                                        Collections.emptyList()))
//                );
//        OpenCGAResult<Interpretation> merge = catalogManager.getInterpretationManager().merge(STUDY, ca.getId(), interpretation.getId(),
//                interpretationAux, Collections.emptyList(), sessionIdUser);
//        assertEquals(1, merge.getNumUpdated());
//
//        Interpretation first = catalogManager.getInterpretationManager().get(STUDY, interpretation.getId(), QueryOptions.empty(),
//                sessionIdUser).first();
//        assertEquals(2, first.getMethods().size());
//        assertEquals(2, first.getPrimaryFindings().size());
//        assertEquals(Arrays.asList("method1", "method2"), first.getPrimaryFindings().get(0).getInterpretationMethodNames());
//        assertEquals(Collections.singletonList("method2"), first.getPrimaryFindings().get(1).getInterpretationMethodNames());
//
//        clinicalVariant.setInterpretationMethodNames(Collections.singletonList("method3"));
//
//        clinicalVariant = new ClinicalVariant();
//        clinicalVariant.setId("variantId3");
//        clinicalVariant.setInterpretationMethodNames(Collections.singletonList("method3"));
//        clinicalVariant.setChromosome("chr2");
//        clinicalVariant.setStart(2);
//        clinicalVariant.setEnd(3);
//        clinicalVariant.setLength(2);
//        variantList.add(clinicalVariant);
//
//        interpretationAux = new Interpretation()
//                .setId("interpretationId2")
//                .setPrimaryFindings(variantList)
//                .setMethods(
//                        Arrays.asList(
//                                new InterpretationMethod("method1", Collections.emptyMap(), Collections.emptyList(),
//                                        Collections.emptyList()),
//                                new InterpretationMethod("method2", Collections.emptyMap(), Collections.emptyList(),
//                                        Collections.emptyList()))
//                );
//        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretationAux, ParamUtils.SaveInterpretationAs.SECONDARY,
//                QueryOptions.empty(), sessionIdUser);
//
//        merge = catalogManager.getInterpretationManager().merge(STUDY, ca.getId(), interpretation.getId(), interpretationAux.getId(),
//                Collections.singletonList("variantId3"), sessionIdUser);
//        assertEquals(1, merge.getNumUpdated());
//
//        first = catalogManager.getInterpretationManager().get(STUDY, interpretation.getId(), QueryOptions.empty(), sessionIdUser).first();
//        assertEquals(3, first.getMethods().size());
//        assertEquals(3, first.getPrimaryFindings().size());
//        assertEquals(Arrays.asList("method1", "method2"), first.getPrimaryFindings().get(0).getInterpretationMethodNames());
//        assertEquals(Collections.singletonList("method2"), first.getPrimaryFindings().get(1).getInterpretationMethodNames());
//        assertEquals(Collections.singletonList("method3"), first.getPrimaryFindings().get(2).getInterpretationMethodNames());
//    }

    @Test
    public void searchInterpretationVersion() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);

        InterpretationUpdateParams params = new InterpretationUpdateParams().setAnalyst(new ClinicalAnalystParam("user2"));
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1",
                params, null, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, InterpretationDBAdaptor.QueryParams.VERSION.key());
        result = catalogManager.getInterpretationManager().get(STUDY, Collections.singletonList(ca.getId() + ".1"),
                new Query(Constants.ALL_VERSIONS, true), options, false, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getInterpretationManager().get(STUDY, Collections.singletonList(ca.getId() + ".1"),
                new Query(InterpretationDBAdaptor.QueryParams.VERSION.key(), "1,2"), options, false, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getInterpretationManager().get(STUDY, Collections.singletonList(ca.getId() + ".1"),
                new Query(InterpretationDBAdaptor.QueryParams.VERSION.key(), "All"), options, false, sessionIdUser);
        assertEquals(2, result.getNumResults());

        try {
            catalogManager.getInterpretationManager().get(STUDY, Arrays.asList(ca.getId() + ".1", ca.getId() + ".2"),
                    new Query(Constants.ALL_VERSIONS, true), options, false, sessionIdUser);
            fail("The previous call should fail because it should not be possible to fetch all versions of multiple interpretations");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("multiple"));
        }

        try {
            catalogManager.getInterpretationManager().get(STUDY, Arrays.asList(ca.getId() + ".1", "interpretation2"),
                    new Query(InterpretationDBAdaptor.QueryParams.VERSION.key(), "1"), options, false, sessionIdUser);
            fail("The previous call should fail users cannot fetch a concrete version for multiple interpretations");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("multiple"));
        }
    }

    @Test
    public void revertInterpretationVersion() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);

        // version 2
        InterpretationUpdateParams params = new InterpretationUpdateParams().setAnalyst(new ClinicalAnalystParam("user2"));
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", params, null, QueryOptions.empty(),
                sessionIdUser);

        // version 3
        params = new InterpretationUpdateParams().setComments(Collections.singletonList(new ClinicalCommentParam("my first comment",
                Collections.singletonList("tag1"))));
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", params, null, QueryOptions.empty(),
                sessionIdUser);

        // version 4
        params = new InterpretationUpdateParams().setComments(Collections.singletonList(new ClinicalCommentParam("my second comment",
                Collections.singletonList("tag2"))));
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", params, null, QueryOptions.empty(),
                sessionIdUser);

        // Current status
        Interpretation interpretation =
                catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(4, interpretation.getVersion());
        assertEquals("user2", interpretation.getAnalyst().getId());
        assertEquals(2, interpretation.getComments().size());
        assertEquals(1, interpretation.getComments().get(0).getTags().size());
        assertEquals("tag1", interpretation.getComments().get(0).getTags().get(0));
        assertEquals(1, interpretation.getComments().get(1).getTags().size());
        assertEquals("tag2", interpretation.getComments().get(1).getTags().get(0));

        // TEST REVERT
        try {
            catalogManager.getInterpretationManager().revert(STUDY, ca.getId(), ca.getId() + ".1", 0, sessionIdUser);
            fail("A CatalogException should be raised pointing we cannot set to a version equal or inferior to 0");
        } catch (CatalogException e) {
        }

        try {
            catalogManager.getInterpretationManager().revert(STUDY, ca.getId(), ca.getId() + ".1", 5, sessionIdUser);
            fail("A CatalogException should be raised pointing we cannot set to a version above the current one");
        } catch (CatalogException e) {
        }

        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().revert(STUDY, ca.getId(), ca.getId() + ".1", 2,
                sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(5, interpretation.getVersion());
        assertEquals("user2", interpretation.getAnalyst().getId());
        assertEquals(0, interpretation.getComments().size());

        result = catalogManager.getInterpretationManager().revert(STUDY, ca.getId(), ca.getId() + ".1", 3, sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(6, interpretation.getVersion());
        assertEquals("user2", interpretation.getAnalyst().getId());
        assertEquals(1, interpretation.getComments().size());
        assertEquals(1, interpretation.getComments().get(0).getTags().size());
        assertEquals("tag1", interpretation.getComments().get(0).getTags().get(0));

        result = catalogManager.getInterpretationManager().revert(STUDY, ca.getId(), ca.getId() + ".1", 4, sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        interpretation =
                catalogManager.getInterpretationManager().get(STUDY, ca.getId() + ".1", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(7, interpretation.getVersion());
        assertEquals("user2", interpretation.getAnalyst().getId());
        assertEquals(2, interpretation.getComments().size());
        assertEquals(1, interpretation.getComments().get(0).getTags().size());
        assertEquals("tag1", interpretation.getComments().get(0).getTags().get(0));
        assertEquals(1, interpretation.getComments().get(1).getTags().size());
        assertEquals("tag2", interpretation.getComments().get(1).getTags().get(0));

        Query query = new Query(Constants.ALL_VERSIONS, true);
        result = catalogManager.getInterpretationManager().get(STUDY, Collections.singletonList(ca.getId() + ".1"), query,
                QueryOptions.empty(), false, sessionIdUser);
        assertEquals(7, result.getNumResults());

        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(8, clinicalAnalysis.getAudit().size());
        assertEquals(7, clinicalAnalysis.getInterpretation().getVersion());
    }

    @Test
    public void interpretationLockedTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation().setLocked(true),
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertTrue(ca.getInterpretation().isLocked());
        for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
            assertFalse(secondaryInterpretation.isLocked());
        }

        // Try to update interpretation 1
        try {
            catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getInterpretation().getId(),
                    new InterpretationUpdateParams().setDescription("blabla"), null, QueryOptions.empty(), sessionIdUser);
            fail("Interpretation is locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

        // Update interpretation 2
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("blabla"), null, QueryOptions.empty(), sessionIdUser);
        Interpretation interpretation2 = catalogManager.getInterpretationManager().get(STUDY,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals("blabla", interpretation2.getDescription());
        assertFalse(interpretation2.isLocked());

        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("bloblo").setLocked(true), null, QueryOptions.empty(), sessionIdUser);
        interpretation2 = catalogManager.getInterpretationManager().get(STUDY,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals("bloblo", interpretation2.getDescription());
        assertTrue(interpretation2.isLocked());

        // Try to update interpretation 2
        try {
            catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                    new InterpretationUpdateParams().setDescription("blabla"), null, QueryOptions.empty(), sessionIdUser);
            fail("Interpretation is locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

        // Try to lock again and update interpretation 2
        try {
            catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                    new InterpretationUpdateParams().setDescription("blabla").setLocked(true), null, QueryOptions.empty(), sessionIdUser);
            fail("Interpretation was already locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

        // Unlock and update interpretation 2
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("blabla").setLocked(false), null, QueryOptions.empty(), sessionIdUser);
        interpretation2 = catalogManager.getInterpretationManager().get(STUDY,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals("blabla", interpretation2.getDescription());
        assertFalse(interpretation2.isLocked());

        // Lock and update interpretation 2
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("bloblo").setLocked(true), null, QueryOptions.empty(), sessionIdUser);
        interpretation2 = catalogManager.getInterpretationManager().get(STUDY,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals("bloblo", interpretation2.getDescription());
        assertTrue(interpretation2.isLocked());

        // Lock case
        catalogManager.getClinicalAnalysisManager().update(STUDY, ca.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertTrue(ca.isLocked());
        assertTrue(ca.getInterpretation().isLocked());
        for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
            assertTrue(secondaryInterpretation.isLocked());
        }

        // Unlock interpretation 1
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getInterpretation().getId(),
                new InterpretationUpdateParams().setLocked(false), null, QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertTrue(ca.isLocked());
        assertFalse(ca.getInterpretation().isLocked());
        for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
            assertTrue(secondaryInterpretation.isLocked());
        }

        // Unlock case
        catalogManager.getClinicalAnalysisManager().update(STUDY, ca.getId(), new ClinicalAnalysisUpdateParams().setLocked(false),
                QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertFalse(ca.isLocked());
        assertFalse(ca.getInterpretation().isLocked());
        for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
            assertFalse(secondaryInterpretation.isLocked());
        }
    }


        @Test
    public void updateInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(5, ca.getAudit().size());
        assertEquals(ClinicalAudit.Action.CREATE_INTERPRETATION, ca.getAudit().get(4).getAction());
        assertEquals(ca.getId() + ".1", ca.getInterpretation().getId());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(2).getId());

        InterpretationUpdateParams params = new InterpretationUpdateParams().setAnalyst(new ClinicalAnalystParam("user2"));
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1",
                params, null, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals(6, ca.getAudit().size());
        assertEquals(ClinicalAudit.Action.UPDATE_INTERPRETATION, ca.getAudit().get(5).getAction());
        assertNotNull(ca.getInterpretation().getAnalyst());
        assertEquals("user2", ca.getInterpretation().getAnalyst().getId());
        assertEquals(2, ca.getInterpretation().getVersion());

        // Update a secondary interpretation
        params = new InterpretationUpdateParams()
                .setDescription("my description");
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".3", params, null, QueryOptions.empty(),
                sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals(7, ca.getAudit().size());
        assertEquals(ClinicalAudit.Action.UPDATE_INTERPRETATION, ca.getAudit().get(6).getAction());
        assertEquals("my description", ca.getSecondaryInterpretations().get(1).getDescription());
        assertEquals(2, ca.getSecondaryInterpretations().get(1).getVersion());

        // Scalate secondary interpretation to primary and delete
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".3", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        catalogManager.getInterpretationManager().delete(STUDY, ca.getId(), Collections.singletonList(ca.getId() + ".1"), sessionIdUser);

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals(10, ca.getAudit().size());
        assertEquals(ClinicalAudit.Action.UPDATE_INTERPRETATION, ca.getAudit().get(7).getAction());
        assertEquals(ClinicalAudit.Action.SWAP_INTERPRETATION, ca.getAudit().get(8).getAction());
        assertEquals(ClinicalAudit.Action.DELETE_INTERPRETATION, ca.getAudit().get(9).getAction());
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".3", ca.getInterpretation().getId());
        assertEquals(2, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());

        // Scalate secondary interpretation to primary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".4", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(12, ca.getAudit().size());
        assertEquals(ClinicalAudit.Action.UPDATE_INTERPRETATION, ca.getAudit().get(10).getAction());
        assertEquals(ClinicalAudit.Action.SWAP_INTERPRETATION, ca.getAudit().get(11).getAction());
        assertEquals(ca.getId() + ".4", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(2, ca.getSecondaryInterpretations().get(1).getVersion());

        // Scalate secondary interpretation to primary
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".3", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".3", ca.getInterpretation().getId());
        assertEquals(2, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(1, ca.getSecondaryInterpretations().get(1).getVersion());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".3", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(2).getId());
        assertEquals(2, ca.getSecondaryInterpretations().get(2).getVersion());

        // Scalate to primary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(2).getId());
        assertEquals(1, ca.getSecondaryInterpretations().get(2).getVersion());

        // Scalate to primary and keep
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(2).getId());
        assertEquals(1, ca.getSecondaryInterpretations().get(2).getVersion());

        // Scalate to primary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
    }

    @Test
    public void deleteInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        // We update interpretation 1 so a new version is generated
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1", new InterpretationUpdateParams()
                .setDescription("my description"), null, QueryOptions.empty(), sessionIdUser);

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".1", ca.getInterpretation().getId());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(2).getId());

        OpenCGAResult delete = catalogManager.getInterpretationManager().delete(STUDY, ca.getId(),
                Arrays.asList(ca.getId() + ".1", ca.getId() + ".3"), sessionIdUser);
        assertEquals(2, delete.getNumDeleted());

        Query query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList(ca.getId() + ".1", ca.getId() + ".3"))
                .append(Constants.ALL_VERSIONS, true);
        OpenCGAResult<Interpretation> search = catalogManager.getInterpretationManager().search(STUDY, query, QueryOptions.empty(),
                sessionIdUser);
        assertEquals(0, search.getNumResults());

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNull(ca.getInterpretation());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());

        query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList(ca.getId() + ".1", ca.getId() + ".3"));
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().search(STUDY, query, QueryOptions.empty(),
                sessionIdUser);
        assertEquals(0, result.getNumResults());

        query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList(ca.getId() + ".1", ca.getId() + ".3"))
                .append(InterpretationDBAdaptor.QueryParams.DELETED.key(), true);
        result = catalogManager.getInterpretationManager().search(STUDY, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void searchClinicalAnalysisByProband() throws CatalogException {
        createDummyEnvironment(true, false);
        createDummyEnvironment(false, false);

        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(STUDY,
                new Query(ParamConstants.CLINICAL_PROBAND_PARAM, "^chil"),
                new QueryOptions(QueryOptions.INCLUDE, ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_ID.key()), sessionIdUser);
        assertEquals(2, search.getNumResults());
        assertTrue(StringUtils.isNotEmpty(search.first().getProband().getId()));
        assertTrue(StringUtils.isEmpty(search.first().getProband().getName()));
    }

    @Test
    public void searchClinicalAnalysisByStatus() throws CatalogException {
        createDummyEnvironment(true, false);
        createDummyEnvironment(false, false);

        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(STUDY,
                new Query(ParamConstants.STATUS_PARAM, ClinicalAnalysisStatus.DONE),
                new QueryOptions(QueryOptions.INCLUDE, ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_ID.key()), sessionIdUser);
        assertEquals(0, search.getNumResults());

        search = catalogManager.getClinicalAnalysisManager().search(STUDY,
                new Query(ParamConstants.STATUS_PARAM, ClinicalAnalysisStatus.READY_FOR_INTERPRETATION),
                new QueryOptions(), sessionIdUser);
        assertEquals(2, search.getNumResults());
        for (ClinicalAnalysis result : search.getResults()) {
            assertEquals(ClinicalAnalysisStatus.READY_FOR_INTERPRETATION, result.getStatus().getId());
        }

        catalogManager.getClinicalAnalysisManager().update(STUDY, search.first().getId(),
                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam(ClinicalAnalysisStatus.REJECTED)), QueryOptions.empty(),
                sessionIdUser);
        search = catalogManager.getClinicalAnalysisManager().search(STUDY,
                new Query(ParamConstants.STATUS_PARAM, ClinicalAnalysisStatus.READY_FOR_INTERPRETATION),
                new QueryOptions(), sessionIdUser);
        assertEquals(1, search.getNumResults());
        for (ClinicalAnalysis result : search.getResults()) {
            assertEquals(ClinicalAnalysisStatus.READY_FOR_INTERPRETATION, result.getStatus().getId());
        }

        search = catalogManager.getClinicalAnalysisManager().search(STUDY,
                new Query(ParamConstants.STATUS_PARAM, ClinicalAnalysisStatus.REJECTED),
                new QueryOptions(), sessionIdUser);
        assertEquals(1, search.getNumResults());
        for (ClinicalAnalysis result : search.getResults()) {
            assertEquals(ClinicalAnalysisStatus.REJECTED, result.getStatus().getId());
        }
    }

    @Test
    public void deleteClinicalAnalysisTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);
        OpenCGAResult delete = catalogManager.getClinicalAnalysisManager().delete(STUDY,
                Collections.singletonList(dummyEnvironment.first().getId()), null, sessionIdUser);
        assertEquals(1, delete.getNumDeleted());

        OpenCGAResult<ClinicalAnalysis> clinicalResult = catalogManager.getClinicalAnalysisManager().get(STUDY,
                Collections.singletonList(dummyEnvironment.first().getId()),
                new Query(ClinicalAnalysisDBAdaptor.QueryParams.DELETED.key(), true), new QueryOptions(), false, sessionIdUser);
        assertEquals(1, clinicalResult.getNumResults());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(), new QueryOptions(), sessionIdUser);
    }

    @Test
    public void deleteClinicalAnalysisWithEmptyInterpretations() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getClinicalAnalysisManager().delete(STUDY, Collections.singletonList(ca.getId()), null, sessionIdUser);

        assertEquals(0,
                catalogManager.getClinicalAnalysisManager().search(STUDY, new Query(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(),
                        ca.getId()), QueryOptions.empty(), sessionIdUser).getNumResults());
        assertEquals(0,
                catalogManager.getInterpretationManager().search(STUDY, new Query(InterpretationDBAdaptor.QueryParams.ID.key(),
                        ca.getId() + ".1"), QueryOptions.empty(), sessionIdUser).getNumResults());
        assertEquals(0,
                catalogManager.getInterpretationManager().search(STUDY, new Query(InterpretationDBAdaptor.QueryParams.ID.key(),
                        ca.getId() + ".2"), QueryOptions.empty(), sessionIdUser).getNumResults());
        assertEquals(0,
                catalogManager.getInterpretationManager().search(STUDY, new Query(InterpretationDBAdaptor.QueryParams.ID.key(),
                        ca.getId() + ".3"), QueryOptions.empty(), sessionIdUser).getNumResults());
        assertEquals(0,
                catalogManager.getInterpretationManager().search(STUDY, new Query(InterpretationDBAdaptor.QueryParams.ID.key(),
                        ca.getId() + ".4"), QueryOptions.empty(), sessionIdUser).getNumResults());
    }

    @Test
    public void deleteClinicalAnalysisWithInterpretations() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        // Add finding to interpretation
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getId() + ".1",
                new InterpretationUpdateParams().setPrimaryFindings(Collections.singletonList(
                        new ClinicalVariant(VariantAvro.newBuilder()
                                .setChromosome("1")
                                .setStart(100)
                                .setEnd(100)
                                .setLength(1)
                                .setReference("C")
                                .setAlternate("T")
                                .setId("1:100:C:T")
                                .setType(VariantType.SNV)
                                .setStudies(Collections.emptyList())
                                .build())
                )), null, QueryOptions.empty(), sessionIdUser);

        try {
            catalogManager.getClinicalAnalysisManager().delete(STUDY, Collections.singletonList(ca.getId()), null, sessionIdUser);
            fail("It should not allow deleting Clinical Analyses with interpretations containing primary findings");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("findings"));
        }

        OpenCGAResult delete = catalogManager.getClinicalAnalysisManager().delete(STUDY, Collections.singletonList(ca.getId()),
                new QueryOptions(Constants.FORCE, true), sessionIdUser);
        assertEquals(1, delete.getNumDeleted());

        OpenCGAResult<ClinicalAnalysis> clinicalResult = catalogManager.getClinicalAnalysisManager().get(STUDY,
                Collections.singletonList(ca.getId()),
                new Query(ClinicalAnalysisDBAdaptor.QueryParams.DELETED.key(), true), new QueryOptions(), false, sessionIdUser);
        assertEquals(1, clinicalResult.getNumResults());

        assertEquals(0, catalogManager.getInterpretationManager().search(STUDY,
                new Query(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList(ca.getId() + ".1", ca.getId() + ".2",
                        ca.getId() + ".3", ca.getId() + ".4")), QueryOptions.empty(), sessionIdUser).getNumResults());

        // Old interpretations were deleted
        assertEquals(4, catalogManager.getInterpretationManager().search(STUDY, new Query()
                        .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList(ca.getId() + ".1", ca.getId() + ".2",
                                ca.getId() + ".3", ca.getId() + ".4"))
                        .append(InterpretationDBAdaptor.QueryParams.DELETED.key(), true), QueryOptions.empty(), sessionIdUser)
                .getNumResults());
    }

    @Test
    public void deleteLockedClinicalAnalysis() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getClinicalAnalysisManager().update(STUDY, ca.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), sessionIdUser);

        try {
            catalogManager.getClinicalAnalysisManager().delete(STUDY, Collections.singletonList(ca.getId()), null, sessionIdUser);
            fail("It should not allow deleting locked Clinical Analyses");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

        OpenCGAResult delete = catalogManager.getClinicalAnalysisManager().delete(STUDY, Collections.singletonList(ca.getId()),
                new QueryOptions(Constants.FORCE, true), sessionIdUser);
        assertEquals(1, delete.getNumDeleted());

        OpenCGAResult<ClinicalAnalysis> clinicalResult = catalogManager.getClinicalAnalysisManager().get(STUDY,
                Collections.singletonList(ca.getId()), new Query(ClinicalAnalysisDBAdaptor.QueryParams.DELETED.key(), true),
                new QueryOptions(), false, sessionIdUser);
        assertEquals(1, clinicalResult.getNumResults());
    }

    @Test
    public void searchByDisorderTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setDisorder(new DisorderReferenceParam("dis1"));
        catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(), updateParams, QueryOptions.empty(),
                sessionIdUser);

        createDummyEnvironment(false, false);

        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().search(STUDY,
                new Query(ParamConstants.CLINICAL_DISORDER_PARAM, "dis1"), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(dummyEnvironment.first().getId(), result.first().getId());

        result = catalogManager.getClinicalAnalysisManager().search(STUDY, new Query(), QueryOptions.empty(), sessionIdUser);
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void updateDisorder() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setDisorder(new DisorderReferenceParam("dis1"));

        catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(), updateParams, QueryOptions.empty(),
                sessionIdUser);
        OpenCGAResult<ClinicalAnalysis> result1 = catalogManager.getClinicalAnalysisManager().get(STUDY, dummyEnvironment.first().getId(),
                new QueryOptions(), sessionIdUser);

        assertEquals("dis1", result1.first().getDisorder().getId());
        assertEquals("OT", result1.first().getDisorder().getSource());

        updateParams = new ClinicalAnalysisUpdateParams()
                .setDisorder(new DisorderReferenceParam("non_existing"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("proband disorders");
        catalogManager.getClinicalAnalysisManager().update(STUDY, dummyEnvironment.first().getId(), updateParams, QueryOptions.empty(),
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
                clinicalAnalysis, INCLUDE_RESULT, sessionIdUser);

        assertTrue(clinicalAnalysisDataResult.first().getFamily().getMembers().stream().map(Individual::getId).collect(Collectors.toList())
                .containsAll(dummyFamily.first().getMembers().stream().map(Individual::getId).collect(Collectors.toList())));
        assertEquals(5, dummyFamily.first().getMembers().size());
        assertEquals(5, clinicalAnalysisDataResult.first().getFamily().getMembers().size());
    }

    @Test
    public void createClinicalAnalysisWithPanels() throws CatalogException {
        catalogManager.getPanelManager().importFromSource(STUDY, "cancer-gene-census", "", sessionIdUser);
        Panel panel = catalogManager.getPanelManager().search(STUDY, new Query(), QueryOptions.empty(), sessionIdUser).first();

        DataResult<Family> dummyFamily = createDummyFamily();
        // Leave only sample2 for child1 in family
        for (Individual member : dummyFamily.first().getMembers()) {
            if (member.getId().equals("child1")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample2")));
            } else if (member.getId().equals("child2")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample5")));
            } else if (member.getId().equals("child3")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample7")));
            } else {
                member.setSamples(null);
            }
        }

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000")
                .setPanels(Collections.singletonList(new Panel().setId(panel.getId())))
                .setFamily(dummyFamily.first())
                .setProband(new Individual().setId("child1"));

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, INCLUDE_RESULT,
                sessionIdUser).first();

        assertEquals(1, ca.getPanels().size());
        assertEquals(panel.getId(), ca.getPanels().get(0).getId());
        assertEquals(panel.getName(), ca.getPanels().get(0).getName());
        assertEquals(panel.getVersion(), ca.getPanels().get(0).getVersion());
        assertEquals(panel.getGenes().size(), ca.getPanels().get(0).getGenes().size());
    }

    @Test
    public void createInterpretationWithPanels() throws CatalogException {
        catalogManager.getPanelManager().importFromSource(STUDY, "cancer-gene-census", "", sessionIdUser);
        Panel panel = catalogManager.getPanelManager().search(STUDY, new Query(), QueryOptions.empty(), sessionIdUser).first();

        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        Interpretation interpretation = new Interpretation()
                .setPanels(Collections.singletonList(new Panel().setId(panel.getId())));

        interpretation = catalogManager.getInterpretationManager().create(STUDY, ca.getId(), interpretation,
                ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, sessionIdUser).first();
        interpretation = catalogManager.getInterpretationManager().get(STUDY, interpretation.getId(), QueryOptions.empty(), sessionIdUser)
                .first();

        assertEquals(1, interpretation.getPanels().size());
        assertEquals(panel.getId(), interpretation.getPanels().get(0).getId());
    }

    @Test
    public void updatePanelsInClinicalAnalysis() throws CatalogException {
        catalogManager.getPanelManager().importFromSource(STUDY, "cancer-gene-census", "", sessionIdUser);
        Panel panel = catalogManager.getPanelManager().search(STUDY, new Query(), QueryOptions.empty(), sessionIdUser).first();

        DataResult<Family> dummyFamily = createDummyFamily();
        // Leave only sample2 for child1 in family
        for (Individual member : dummyFamily.first().getMembers()) {
            if (member.getId().equals("child1")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample2")));
            } else if (member.getId().equals("child2")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample5")));
            } else if (member.getId().equals("child3")) {
                member.setSamples(Collections.singletonList(new Sample().setId("sample7")));
            } else {
                member.setSamples(null);
            }
        }

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000")
                .setFamily(dummyFamily.first())
                .setProband(new Individual().setId("child1"));

        // Create without a panel and update the panel
        catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser).first();
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setPanels(Collections.singletonList(new PanelReferenceParam(panel.getId()))),
                QueryOptions.empty(), sessionIdUser);

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(),
                sessionIdUser).first();

        assertEquals(1, ca.getPanels().size());
        assertEquals(panel.getId(), ca.getPanels().get(0).getId());
        assertEquals(panel.getName(), ca.getPanels().get(0).getName());
        assertEquals(panel.getVersion(), ca.getPanels().get(0).getVersion());
        assertEquals(panel.getGenes().size(), ca.getPanels().get(0).getGenes().size());
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
        Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.INDIVIDUAL.key(), "child3");
        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(STUDY, query, includeClinicalIds,
                sessionIdUser);
        assertEquals(1, search.getNumResults());

        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.INDIVIDUAL.key(), "child1");
        search = catalogManager.getClinicalAnalysisManager().search(STUDY, query, includeClinicalIds, sessionIdUser);
        assertEquals(1, search.getNumResults());

        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.INDIVIDUAL.key(), "child4");
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

    private List<Panel> createPanels(int nPanels) throws CatalogException {
        List<Panel> panelList = new ArrayList<>(nPanels);
        for (int i = 0; i < nPanels; i++) {
            panelList.add(catalogManager.getPanelManager().create(STUDY, new Panel().setId("panel" + i), INCLUDE_RESULT,
                    sessionIdUser).first());
        }
        return panelList;
    }

    @Test
    public void createClinicalAnalysisWithPanelsTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(STUDY,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, sessionIdUser).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanels(panels);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        for (Panel panel : result.first().getPanels()) {
            assertNotNull(panel.getName());
        }
        assertEquals(2, result.first().getInterpretation().getPanels().size());
        for (Panel panel : result.first().getInterpretation().getPanels()) {
            assertNotNull(panel.getId());
            assertNotNull(panel.getName());
        }
        assertFalse(result.first().isPanelLock());
    }

    @Test
    public void fetchInterpretationWithFullPanelInformationTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(STUDY,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, sessionIdUser).first();
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanels(panels);
        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, INCLUDE_RESULT, sessionIdUser);

        Interpretation interpretation = catalogManager.getInterpretationManager().search(STUDY,
                new Query(InterpretationDBAdaptor.QueryParams.CLINICAL_ANALYSIS_ID.key(), clinicalAnalysis.getId()), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(2, interpretation.getPanels().size());
        for (Panel panel : interpretation.getPanels()) {
            assertNotNull(panel.getId());
            assertNotNull(panel.getName());
        }
    }

    @Test
    public void updatePanelsActionTest() throws CatalogException {
        List<Panel> panels = createPanels(5);
        Individual proband = catalogManager.getIndividualManager().create(STUDY,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, sessionIdUser).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setPanels(panels.subList(0, 2))
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                options, sessionIdUser);
        result = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(3, result.first().getPanels().size());
        assertTrue(panels.subList(0, 3).stream().map(Panel::getId).collect(Collectors.toList()).containsAll(
                result.first().getPanels().stream().map(Panel::getId).collect(Collectors.toList())));

        actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(Arrays.asList(
                                new PanelReferenceParam(panels.get(0).getId()),
                                new PanelReferenceParam(panels.get(2).getId()))
                        ),
                options, sessionIdUser);
        result = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(1, result.first().getPanels().size());
        assertEquals(panels.get(1).getId(), result.first().getPanels().get(0).getId());

        actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(Arrays.asList(
                                new PanelReferenceParam(panels.get(3).getId()),
                                new PanelReferenceParam(panels.get(4).getId()))
                        ),
                options, sessionIdUser);
        result = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertTrue(panels.subList(3, 5).stream().map(Panel::getId).collect(Collectors.toList()).containsAll(
                result.first().getPanels().stream().map(Panel::getId).collect(Collectors.toList())));

    }

    @Test
    public void updateInterpretationPanelsActionTest() throws CatalogException {
        List<Panel> panels = createPanels(5);
        Individual proband = catalogManager.getIndividualManager().create(STUDY,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, sessionIdUser).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
//                .setPanels(panels.subList(0, 2))
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(0, result.first().getPanels().size());

        String intepretationId = result.first().getInterpretation().getId();
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        OpenCGAResult<Interpretation> interpretation = catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(),
                intepretationId, new InterpretationUpdateParams()
                        .setPanels(panels.subList(0, 2).stream().map((p) -> new PanelReferenceParam(p.getId())).collect(Collectors.toList())),
                null, options, sessionIdUser);
        assertEquals(2, interpretation.first().getPanels().size());

        actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.ADD);
        options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        interpretation = catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(),
                intepretationId, new InterpretationUpdateParams()
                        .setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                null, options, sessionIdUser);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(3, interpretation.first().getPanels().size());
        assertTrue(panels.subList(0, 3).stream().map(Panel::getId).collect(Collectors.toList()).containsAll(
                interpretation.first().getPanels().stream().map(Panel::getId).collect(Collectors.toList())));

        actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.REMOVE);
        options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        interpretation = catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(),
                intepretationId, new InterpretationUpdateParams()
                        .setPanels(Arrays.asList(
                                new PanelReferenceParam(panels.get(0).getId()),
                                new PanelReferenceParam(panels.get(2).getId()))
                        ),
                null, options, sessionIdUser);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(1, interpretation.first().getPanels().size());
        assertEquals(panels.get(1).getId(), interpretation.first().getPanels().get(0).getId());

        actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        interpretation = catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(),
                intepretationId, new InterpretationUpdateParams()
                        .setPanels(Arrays.asList(
                                new PanelReferenceParam(panels.get(3).getId()),
                                new PanelReferenceParam(panels.get(4).getId()))
                        ),
                null, options, sessionIdUser);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(2, interpretation.first().getPanels().size());
        assertTrue(panels.subList(3, 5).stream().map(Panel::getId).collect(Collectors.toList()).containsAll(
                interpretation.first().getPanels().stream().map(Panel::getId).collect(Collectors.toList())));

    }

    @Test
    public void updatePanelsAndPanelLockFromClinicalAnalysisTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(STUDY,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, sessionIdUser).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, true, INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(0, result.first().getPanels().size());
        assertFalse(result.first().isPanelLock());

        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(panels.stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList()))
                        .setPanelLock(true),
                QueryOptions.empty(), sessionIdUser);
        result = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertTrue(result.first().isPanelLock());
    }

    @Test
    public void updatePanelsAndPanelLockFromClinicalAnalysisWithInterpretationTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(STUDY,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, sessionIdUser).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, null, INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(0, result.first().getPanels().size());
        assertFalse(result.first().isPanelLock());


        thrown.expect(CatalogException.class);
        thrown.expectMessage("not allowed");
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(panels.stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList()))
                        .setPanelLock(true),
                QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void updatePanelsFromClinicalAnalysisWithPanelLockTest() throws CatalogException {
        List<Panel> panels = createPanels(3);
        Individual proband = catalogManager.getIndividualManager().create(STUDY,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, sessionIdUser).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanels(panels.subList(0, 2));

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertFalse(result.first().isPanelLock());

        Interpretation interpretation = catalogManager.getInterpretationManager().create(STUDY, clinicalAnalysis.getId(),
                new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, sessionIdUser).first();
        assertEquals(2, interpretation.getPanels().size());

        // Set panelLock to true
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setPanelLock(true), QueryOptions.empty(), sessionIdUser);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertTrue(clinicalAnalysis.isPanelLock());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("panelLock");
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void updatePanelLockWithDifferentPanels() throws CatalogException {
        List<Panel> panels = createPanels(3);
        Individual proband = catalogManager.getIndividualManager().create(STUDY,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, sessionIdUser).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanels(panels.subList(0, 2));

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertFalse(result.first().isPanelLock());

        Interpretation interpretation = catalogManager.getInterpretationManager().create(STUDY, clinicalAnalysis.getId(),
                new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, sessionIdUser).first();
        assertEquals(2, interpretation.getPanels().size());

        // Set panelLock to true
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setPanelLock(true), QueryOptions.empty(), sessionIdUser);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertTrue(clinicalAnalysis.isPanelLock());

        // Set panelLock to false
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setPanelLock(false), QueryOptions.empty(), sessionIdUser);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertFalse(clinicalAnalysis.isPanelLock());

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                null, options, sessionIdUser);
        interpretation = catalogManager.getInterpretationManager().get(STUDY, interpretation.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals(1, interpretation.getPanels().size());
        assertEquals(panels.get(2).getId(), interpretation.getPanels().get(0).getId());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("panels");
        // Set panelLock to true
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setPanelLock(true), QueryOptions.empty(), sessionIdUser);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertTrue(clinicalAnalysis.isPanelLock());
    }

    @Test
    public void updatePanelsFromInterpretationWithLockedCATest() throws CatalogException {
        List<Panel> panels = createPanels(3);
        Individual proband = catalogManager.getIndividualManager().create(STUDY,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, sessionIdUser).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanelLock(true)
                .setPanels(panels.subList(0, 2));

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertTrue(result.first().isPanelLock());

        Interpretation interpretation = catalogManager.getInterpretationManager().create(STUDY, clinicalAnalysis.getId(),
                new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, sessionIdUser).first();
        assertEquals(2, interpretation.getPanels().size());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("panelLock");
        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                null, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void updatePanelsFromInterpretationWithUnlockedCATest() throws CatalogException {
        List<Panel> panels = createPanels(3);
        Individual proband = catalogManager.getIndividualManager().create(STUDY,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, sessionIdUser).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanels(panels.subList(0, 2));

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertFalse(result.first().isPanelLock());

        Interpretation interpretation = catalogManager.getInterpretationManager().create(STUDY, clinicalAnalysis.getId(),
                new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, sessionIdUser).first();
        assertEquals(2, interpretation.getPanels().size());

        // Ensure this fails
        catalogManager.getInterpretationManager().update(STUDY, clinicalAnalysis.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                null, QueryOptions.empty(), sessionIdUser);
        interpretation = catalogManager.getInterpretationManager().get(STUDY, interpretation.getId(), QueryOptions.empty(),
                sessionIdUser).first();

        assertEquals(3, interpretation.getPanels().size());
    }

    @Test
    public void createClinicalAnalysisWithFiles() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(STUDY, individual, QueryOptions.empty(), sessionIdUser);

        // Register and associate files to sample "sample"
        List<File> files = registerDummyFiles();
        for (File file : files) {
            catalogManager.getFileManager().update(STUDY, file.getPath(),
                    new FileUpdateParams().setSampleIds(Collections.singletonList("sample")), QueryOptions.empty(), sessionIdUser);
        }

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual);
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis,
                INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, clinical.getNumResults());
        assertEquals(4, clinical.first().getFiles().size());
        for (File file : clinical.first().getFiles()) {
            assertNotNull(file.getPath());
            assertNotNull(file.getName());
        }
    }

    @Test
    public void updateClinicalAnalysisFiles() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(STUDY, individual, QueryOptions.empty(), sessionIdUser);

        // Register and associate files to sample "sample"
        List<File> files = registerDummyFiles();
        for (File file : files) {
            catalogManager.getFileManager().update(STUDY, file.getPath(),
                    new FileUpdateParams().setSampleIds(Collections.singletonList("sample")), QueryOptions.empty(), sessionIdUser);
        }
        List<FileReferenceParam> fileRefs = files.stream().map(f -> new FileReferenceParam(f.getPath())).collect(Collectors.toList());

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual);
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis,
                INCLUDE_RESULT, sessionIdUser);
        assertEquals(1, clinical.getNumResults());
        assertEquals(4, clinical.first().getFiles().size());

        // Remove first and last file
        ObjectMap actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setFiles(Arrays.asList(fileRefs.get(0), fileRefs.get(3))), options, sessionIdUser);
        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(),
                sessionIdUser).first();
        assertEquals(2, ca.getFiles().size());
        assertTrue(files.subList(1, 3).stream().map(File::getPath).collect(Collectors.toSet())
                .containsAll(ca.getFiles().stream().map(File::getPath).collect(Collectors.toSet())));

        // Add first file again
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), ParamUtils.BasicUpdateAction.ADD);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setFiles(Collections.singletonList(fileRefs.get(0))), options, sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals(3, ca.getFiles().size());
        assertTrue(files.subList(0, 3).stream().map(File::getPath).collect(Collectors.toSet())
                .containsAll(ca.getFiles().stream().map(File::getPath).collect(Collectors.toSet())));

        // Set file 3 and 4
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(STUDY, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setFiles(Arrays.asList(fileRefs.get(2), fileRefs.get(3))), options, sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, clinicalAnalysis.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals(2, ca.getFiles().size());
        assertTrue(files.subList(2, 4).stream().map(File::getPath).collect(Collectors.toSet())
                .containsAll(ca.getFiles().stream().map(File::getPath).collect(Collectors.toSet())));
    }
}
