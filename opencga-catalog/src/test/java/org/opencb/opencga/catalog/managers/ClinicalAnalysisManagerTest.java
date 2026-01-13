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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.clinical.*;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.FlagAnnotation;
import org.opencb.opencga.core.models.common.FlagValue;
import org.opencb.opencga.core.models.common.StatusParam;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyPermissions;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualPermissions;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelImportParams;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.study.configuration.*;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsent;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class ClinicalAnalysisManagerTest extends AbstractManagerTest {

    private FamilyManager familyManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        familyManager = catalogManager.getFamilyManager();
    }

    private Family getDummyFamily() {
        Disorder disease1 = new Disorder("dis1", "Disease 1", "HPO", null, "", null);
        Disorder disease2 = new Disorder("dis2", "Disease 2", "HPO", null, "", null);

        Individual father = new Individual().setId("father")
                .setSex(SexOntologyTermAnnotation.initMale())
                .setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null)));
        Individual mother = new Individual().setId("mother")
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setDisorders(Arrays.asList(new Disorder("dis2", "dis2", "OT", null, "",
                        null)));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setId("father")
                .setSex(SexOntologyTermAnnotation.initMale())
                .setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null)))
                .setSamples(Collections.singletonList(new Sample().setId("sample1")));
        Individual relMother = new Individual().setId("mother")
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setDisorders(Arrays.asList(new Disorder("dis2", "dis2", "OT", null, "", null)))
                .setSamples(Arrays.asList(new Sample().setId("sample3")));

        Individual relChild1 = new Individual().setId("child1")
                .setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null), new Disorder("dis2", "dis2", "OT", null,
                        "", null)))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initMale())
                .setSamples(Arrays.asList(
                        new Sample().setId("sample2"),
                        new Sample().setId("sample4")
                ))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setId("child2")
                .setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null)))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setSamples(Arrays.asList(
                        new Sample().setId("sample5"),
                        new Sample().setId("sample6")
                ))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setId("child3")
                .setDisorders(Arrays.asList(new Disorder("dis1", "dis1", "OT", null, "", null)))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initFemale())
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
        return familyManager.create(studyFqn, family, INCLUDE_RESULT, ownerToken);
    }

    private DataResult<ClinicalAnalysis> createDummyEnvironment(boolean createFamily, boolean createDefaultInterpretation) throws CatalogException {

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setStatus(new ClinicalStatus().setId("READY_FOR_INTERPRETATION"))
                .setId("analysis" + RandomStringUtils.randomAlphanumeric(3))
                .setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setProband(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))));

        if (createFamily) {
            createDummyFamily();
        }
        clinicalAnalysis.setFamily(new Family().setId("family")
                .setMembers(Arrays.asList(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))))));

        return catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, !createDefaultInterpretation,
                INCLUDE_RESULT, ownerToken);
    }

    private List<File> registerDummyFiles() throws CatalogException {
        List<File> files = new LinkedList<>();

        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        files.add(catalogManager.getFileManager().link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, null, null, null,
                null), false, ownerToken).first());
        vcfFile = getClass().getResource("/biofiles/family.vcf").getFile();
        files.add(catalogManager.getFileManager().link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, null, null, null,
                null), false, ownerToken).first());
        String bamFile = getClass().getResource("/biofiles/HG00096.chrom20.small.bam").getFile();
        files.add(catalogManager.getFileManager().link(studyFqn, new FileLinkParams(bamFile, "", "", "", null, null, null, null,
                null), false, ownerToken).first());
        bamFile = getClass().getResource("/biofiles/NA19600.chrom20.small.bam").getFile();
        files.add(catalogManager.getFileManager().link(studyFqn, new FileLinkParams(bamFile, "", "", "", null, null, null, null,
                null), false, ownerToken).first());

        return files;
    }

    @Test
    public void createAndTestStatusIdIsNotNull() throws CatalogException {
        createDummyFamily();
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis" + RandomStringUtils.randomAlphanumeric(3))
                .setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setProband(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))))
                .setFamily(new Family().setId("family")
                        .setMembers(Collections.singletonList(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))))));

        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, false,
                INCLUDE_RESULT, ownerToken);
        assertEquals(ClinicalStatusValue.ClinicalStatusType.NOT_STARTED, result.first().getStatus().getType());

        clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis" + RandomStringUtils.randomAlphanumeric(3))
                .setStatus(new ClinicalStatus())
                .setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setProband(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))))
                .setFamily(new Family().setId("family")
                        .setMembers(Collections.singletonList(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))))));
        result = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, false, INCLUDE_RESULT, ownerToken);
        assertEquals(ClinicalStatusValue.ClinicalStatusType.NOT_STARTED, result.first().getStatus().getType());

        String clinicalId = clinicalAnalysis.getId();
        assertThrows(CatalogException.class, () -> catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalId,
                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam(null)), INCLUDE_RESULT, ownerToken));
    }

    @Test
    public void createMultipleCasesSameFamily() throws CatalogException {
        ClinicalAnalysis case1 = createDummyEnvironment(true, true).first();
        createDummyEnvironment(false, true).first();
        createDummyEnvironment(false, true).first();
        createDummyEnvironment(false, true).first();
        createDummyEnvironment(false, true).first();

        catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(), new ClinicalAnalysisUpdateParams().setLocked(true), QueryOptions.empty(), ownerToken);
        // Update proband's sample
        catalogManager.getSampleManager().update(studyFqn, case1.getProband().getSamples().get(0).getId(),
                new SampleUpdateParams().setDescription("new description"), QueryOptions.empty(), ownerToken);

        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(studyFqn, new Query(), new QueryOptions(),
                ownerToken);
        assertEquals(5, search.getNumResults());
        for (ClinicalAnalysis casee : search.getResults()) {
            if (casee.getId().equals(case1.getId())) {
                assertEquals(1, casee.getProband().getVersion());
                assertEquals(1, casee.getProband().getSamples().get(0).getVersion());
                assertEquals(1, casee.getFamily().getVersion());
            } else {
                assertEquals(2, casee.getProband().getVersion());
                assertEquals(2, casee.getProband().getSamples().get(0).getVersion());
                assertEquals(2, casee.getFamily().getVersion());
            }
        }
    }

    @Test
    public void updateClinicalAnalystsTest() throws CatalogException {
        ClinicalAnalysis case1 = createDummyEnvironment(true, true).first();

        catalogManager.getUserManager().create(new User().setId("u1").setName("u1").setOrganization(organizationId), TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(new User().setId("u2").setName("u2").setOrganization(organizationId), TestParamConstants.PASSWORD, opencgaToken);

        // Add analysts
        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setAnalysts(
                        Arrays.asList(new ClinicalAnalystParam("u1"), new ClinicalAnalystParam("u2"))), INCLUDE_RESULT, ownerToken);
        assertEquals(3, result.first().getAnalysts().size());
        assertTrue(result.first().getAnalysts().stream().map(ClinicalAnalyst::getId).collect(Collectors.toSet()).containsAll(Arrays.asList("u1", "u2")));

        // Check analyst params
        for (ClinicalAnalyst analyst : result.first().getAnalysts()) {
            assertNotNull(analyst.getId());
            assertNotNull(analyst.getName());
            assertNotNull(analyst.getRole());
            assertNotNull(analyst.getAttributes());
        }

        // Remove analysts
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.ANALYSTS.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        result = catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setAnalysts(
                        Arrays.asList(new ClinicalAnalystParam("u1"), new ClinicalAnalystParam("u2"))), options, ownerToken);
        assertEquals(1, result.first().getAnalysts().size());
        assertTrue(result.first().getAnalysts().stream().map(ClinicalAnalyst::getId).noneMatch(x -> Arrays.asList("u1", "u2").contains(x)));

        // Set analysts
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.ANALYSTS.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        result = catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setAnalysts(
                        Arrays.asList(new ClinicalAnalystParam("u1"), new ClinicalAnalystParam("u2"))), options, ownerToken);
        assertEquals(2, result.first().getAnalysts().size());
        assertTrue(result.first().getAnalysts().stream().map(ClinicalAnalyst::getId).allMatch(x -> Arrays.asList("u1", "u2").contains(x)));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setAnalysts(
                        Arrays.asList(new ClinicalAnalystParam("unknown"), new ClinicalAnalystParam("u2"))), options, ownerToken);
    }

    @Test
    public void updateClinicalAnalysisRequest() throws CatalogException {
        ClinicalAnalysis case1 = createDummyEnvironment(true, true).first();
        assertTrue(StringUtils.isEmpty(case1.getRequest().getId()));

        catalogManager.getUserManager().create(new User().setId("u1").setName("u1").setOrganization(organizationId).setEmail("mail@mail.com"),
                TestParamConstants.PASSWORD, opencgaToken);

        ClinicalRequest request = new ClinicalRequest("requestId", "bla", null, new ClinicalResponsible().setId("u1"), new HashMap<>());

        // Change request
        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setRequest(request), INCLUDE_RESULT, ownerToken);
        assertNotNull(result.first().getRequest());
        assertTrue(StringUtils.isNotEmpty(result.first().getRequest().getDate()));
        assertEquals("requestId", result.first().getRequest().getId());
        assertEquals("u1", result.first().getRequest().getResponsible().getId());
        assertEquals("u1", result.first().getRequest().getResponsible().getName());
        assertEquals("mail@mail.com", result.first().getRequest().getResponsible().getEmail());

        // Remove request responsible
        request.setResponsible(null);
        result = catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setRequest(request), INCLUDE_RESULT, ownerToken);
        assertNotNull(result.first().getRequest());
        assertTrue(StringUtils.isNotEmpty(result.first().getRequest().getDate()));
        assertEquals("requestId", result.first().getRequest().getId());
        assertNull(result.first().getRequest().getResponsible());

        // Add non existing request responsible user id
        request.setResponsible(new ClinicalResponsible().setId("unknown"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setRequest(request), INCLUDE_RESULT, ownerToken);
    }

    @Test
    public void updateClinicalAnalysisResponsible() throws CatalogException {
        ClinicalAnalysis case1 = createDummyEnvironment(true, true).first();
        assertEquals(orgOwnerUserId, case1.getResponsible().getId());

        catalogManager.getUserManager().create(new User().setId("u1").setName("u1").setEmail("mail@mail.com")
                        .setOrganization(organizationId),
                TestParamConstants.PASSWORD, opencgaToken);

        ClinicalResponsible responsible = new ClinicalResponsible().setId("u1");

        // Change responsible
        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setResponsible(responsible), INCLUDE_RESULT, ownerToken);
        assertNotNull(result.first().getResponsible());
        assertEquals("u1", result.first().getResponsible().getId());
        assertEquals("u1", result.first().getResponsible().getName());
        assertEquals("mail@mail.com", result.first().getResponsible().getEmail());

        // Change to non existing request responsible user id
        responsible.setId("unknown");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setResponsible(responsible), INCLUDE_RESULT, ownerToken);
    }

    @Test
    public void updateClinicalAnalysisReport() throws CatalogException {
        ClinicalAnalysis case1 = createDummyEnvironment(true, true).first();
        assertNull(case1.getReport());

        ClinicalReport report = new ClinicalReport()
                .setTitle("my report")
                .setComments(Arrays.asList(new ClinicalComment("author", "msg", null, null), new ClinicalComment("author2", "msg", null, null)));

        // Change report
        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setReport(report), INCLUDE_RESULT, ownerToken);
        assertNotNull(result.first().getReport());
        assertEquals(report.getTitle(), result.first().getReport().getTitle());
        assertEquals(2, result.first().getReport().getComments().size());
        for (ClinicalComment comment : result.first().getReport().getComments()) {
            assertEquals(orgOwnerUserId, comment.getAuthor());
            assertTrue(StringUtils.isNotEmpty(comment.getDate()));
        }

        // Add files
        catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(1000))
                        .setPath("/data/file1.txt")
                        .setType(File.Type.FILE),
                true, ownerToken);
        catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(1000))
                        .setPath("/data/file2.txt")
                        .setType(File.Type.FILE),
                true, ownerToken);

        List<File> fileList = Arrays.asList(new File().setId("data:file1.txt"), new File().setId("data:file2.txt"));
        report.setSupportingEvidences(fileList);
        result = catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setReport(report), INCLUDE_RESULT, ownerToken);
        assertNotNull(result.first().getReport());
        assertEquals(report.getTitle(), result.first().getReport().getTitle());
        assertEquals(2, result.first().getReport().getComments().size());
        for (ClinicalComment comment : result.first().getReport().getComments()) {
            assertEquals(orgOwnerUserId, comment.getAuthor());
            assertTrue(StringUtils.isNotEmpty(comment.getDate()));
        }
        assertEquals(2, result.first().getReport().getSupportingEvidences().size());
        assertEquals("data/file1.txt", result.first().getReport().getSupportingEvidences().get(0).getPath());
        assertEquals("data/file2.txt", result.first().getReport().getSupportingEvidences().get(1).getPath());
        assertNull(result.first().getReport().getFiles());

        report.setFiles(fileList);
        result = catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setReport(report), INCLUDE_RESULT, ownerToken);
        assertNotNull(result.first().getReport());
        assertEquals(report.getTitle(), result.first().getReport().getTitle());
        assertEquals(2, result.first().getReport().getComments().size());
        for (ClinicalComment comment : result.first().getReport().getComments()) {
            assertEquals(orgOwnerUserId, comment.getAuthor());
            assertTrue(StringUtils.isNotEmpty(comment.getDate()));
        }
        assertEquals(2, result.first().getReport().getSupportingEvidences().size());
        assertEquals("data/file1.txt", result.first().getReport().getSupportingEvidences().get(0).getPath());
        assertEquals("data/file2.txt", result.first().getReport().getSupportingEvidences().get(1).getPath());
        assertEquals(2, result.first().getReport().getFiles().size());
        assertEquals("data/file1.txt", result.first().getReport().getFiles().get(0).getPath());
        assertEquals("data/file2.txt", result.first().getReport().getFiles().get(1).getPath());

        // Provide non existing file
        report.setFiles(Collections.singletonList(new File().setId("nonexisting.txt")));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setReport(report), INCLUDE_RESULT, ownerToken);
    }

    @Test
    public void updateClinicalAnalysisReportWithActions() throws CatalogException {
        ClinicalAnalysis case1 = createDummyEnvironment(true, true).first();
        assertNull(case1.getReport());

        // Add files
        catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(1000))
                        .setPath("/data/file1.txt")
                        .setType(File.Type.FILE),
                true, ownerToken);
        catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(1000))
                        .setPath("/data/file2.txt")
                        .setType(File.Type.FILE),
                true, ownerToken);
        catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(1000))
                        .setPath("/data/file3.txt")
                        .setType(File.Type.FILE),
                true, ownerToken);

        ClinicalReport report = new ClinicalReport("title", "overview", new ClinicalDiscussion("me", TimeUtils.getTime(), "text"), "logo",
                "me", "signature", TimeUtils.getTime(), Arrays.asList(
                new ClinicalComment().setMessage("comment1"),
                new ClinicalComment().setMessage("comment2")
        ),
                Collections.singletonList(new File().setId("data:file1.txt")),
                Collections.singletonList(new File().setId("data:file2.txt")));
        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().update(studyFqn, case1.getId(),
                new ClinicalAnalysisUpdateParams().setReport(report), INCLUDE_RESULT, ownerToken);
        assertNotNull(result.first().getReport());
        assertEquals(report.getTitle(), result.first().getReport().getTitle());
        assertEquals(report.getOverview(), result.first().getReport().getOverview());
        assertEquals(report.getDate(), result.first().getReport().getDate());
        assertEquals(report.getLogo(), result.first().getReport().getLogo());
        assertEquals(report.getSignature(), result.first().getReport().getSignature());
        assertEquals(report.getSignedBy(), result.first().getReport().getSignedBy());
        assertEquals(2, result.first().getReport().getComments().size());
        assertEquals(1, result.first().getReport().getFiles().size());
        assertEquals(1, result.first().getReport().getSupportingEvidences().size());

        // Add comment
        // Set files
        // Remove supporting evidence
        ObjectMap actionMap = new ObjectMap()
                .append(ClinicalAnalysisDBAdaptor.ReportQueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.ADD)
                .append(ClinicalAnalysisDBAdaptor.ReportQueryParams.FILES.key(), ParamUtils.BasicUpdateAction.SET)
                .append(ClinicalAnalysisDBAdaptor.ReportQueryParams.SUPPORTING_EVIDENCES.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        ClinicalReport reportToUpdate = new ClinicalReport()
                .setComments(Collections.singletonList(new ClinicalComment().setMessage("comment3")))
                .setFiles(Arrays.asList(
                        new File().setId("data:file2.txt"),
                        new File().setId("data:file3.txt")
                ))
                .setSupportingEvidences(Collections.singletonList(new File().setId("data:file1.txt")));
        ClinicalReport reportResult = catalogManager.getClinicalAnalysisManager().updateReport(studyFqn, case1.getId(), reportToUpdate,
                options, ownerToken).first();
        // Check comments
        assertEquals(3, reportResult.getComments().size());
        assertEquals("comment1", reportResult.getComments().get(0).getMessage());
        assertEquals("comment2", reportResult.getComments().get(1).getMessage());
        assertEquals("comment3", reportResult.getComments().get(2).getMessage());

        // Check files
        assertEquals(2, reportResult.getFiles().size());
        assertTrue(reportResult.getFiles().stream().map(File::getPath).collect(Collectors.toSet()).containsAll(Arrays.asList("data/file2.txt", "data/file3.txt")));

        // Check supporting evidences
        assertEquals(0, reportResult.getSupportingEvidences().size());


        // Remove comment
        // Remove file
        // Set supporting evidences
        actionMap = new ObjectMap()
                .append(ClinicalAnalysisDBAdaptor.ReportQueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.REMOVE)
                .append(ClinicalAnalysisDBAdaptor.ReportQueryParams.FILES.key(), ParamUtils.BasicUpdateAction.REMOVE)
                .append(ClinicalAnalysisDBAdaptor.ReportQueryParams.SUPPORTING_EVIDENCES.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        reportToUpdate = new ClinicalReport()
                .setComments(Arrays.asList(reportResult.getComments().get(0), reportResult.getComments().get(1)))
                .setFiles(Collections.singletonList(new File().setId("data:file3.txt")))
                .setSupportingEvidences(Arrays.asList(
                        new File().setId("data:file1.txt"),
                        new File().setId("data:file3.txt")
                ));
        ClinicalComment pendingComment = reportResult.getComments().get(2);
        reportResult = catalogManager.getClinicalAnalysisManager().updateReport(studyFqn, case1.getId(), reportToUpdate,
                options, ownerToken).first();
        // Check comments
        assertEquals(1, reportResult.getComments().size());
        assertEquals(pendingComment.getMessage(), reportResult.getComments().get(0).getMessage());

        // Check supporting evidences
        assertEquals(2, reportResult.getSupportingEvidences().size());
        assertTrue(reportResult.getSupportingEvidences().stream().map(File::getPath).collect(Collectors.toSet())
                .containsAll(Arrays.asList("data/file1.txt", "data/file3.txt")));

        // Check files
        assertEquals(1, reportResult.getFiles().size());
        assertEquals("data/file2.txt", reportResult.getFiles().get(0).getPath());


        // Add file
        // Add supporting evidences
        actionMap = new ObjectMap()
                .append(ClinicalAnalysisDBAdaptor.ReportQueryParams.FILES.key(), ParamUtils.BasicUpdateAction.ADD)
                .append(ClinicalAnalysisDBAdaptor.ReportQueryParams.SUPPORTING_EVIDENCES.key(), ParamUtils.BasicUpdateAction.ADD);
        options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        reportToUpdate = new ClinicalReport()
                .setFiles(Arrays.asList(
                        new File().setId("data:file1.txt"),
                        new File().setId("data:file3.txt")
                ))
                .setSupportingEvidences(Collections.singletonList(
                        new File().setId("data:file2.txt")
                ));
        reportResult = catalogManager.getClinicalAnalysisManager().updateReport(studyFqn, case1.getId(), reportToUpdate,
                options, ownerToken).first();
        // Check comments
        assertEquals(1, reportResult.getComments().size());
        assertEquals("comment3", reportResult.getComments().get(0).getMessage());

        // Check files
        assertEquals(3, reportResult.getFiles().size());
        assertTrue(reportResult.getFiles().stream().map(File::getPath).collect(Collectors.toSet())
                .containsAll(Arrays.asList("data/file1.txt", "data/file2.txt", "data/file3.txt")));

        // Check supporting evidences
        assertEquals(3, reportResult.getSupportingEvidences().size());
        assertTrue(reportResult.getSupportingEvidences().stream().map(File::getPath).collect(Collectors.toSet())
                .containsAll(Arrays.asList("data/file1.txt", "data/file2.txt", "data/file3.txt")));
    }

    @Test
    public void queryByVersionTest() throws CatalogException {
        Individual individual = new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2")));
        catalogManager.getIndividualManager().create(studyFqn, individual, null, ownerToken);

        ClinicalComment comment = new ClinicalComment(orgOwnerUserId, "my comment", new ArrayList<>(), "");
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis" + RandomStringUtils.randomAlphanumeric(3))
                .setDescription("My description").setType(ClinicalAnalysis.Type.SINGLE)
                .setQualityControl(new ClinicalAnalysisQualityControl(ClinicalAnalysisQualityControl.QualityControlSummary.LOW,
                        Collections.singletonList(comment), Collections.emptyList()))
                .setProband(individual);

        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, true, INCLUDE_RESULT, ownerToken).first();
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams().setDescription("blabla"), null, ownerToken);

        ClinicalAnalysis case1 = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), null, ownerToken).first();
        assertEquals(2, case1.getVersion());
        assertEquals("blabla", case1.getDescription());

        Query query = new Query(ParamConstants.CLINICAL_VERSION_PARAM, 1);
        case1 = catalogManager.getClinicalAnalysisManager().get(studyFqn, Collections.singletonList(clinicalAnalysis.getId()), query, null, false, ownerToken).first();
        assertEquals(1, case1.getVersion());
        assertEquals(clinicalAnalysis.getDescription(), case1.getDescription());
    }

        @Test
    public void createAndUpdateClinicalAnalysisWithQualityControl() throws CatalogException, InterruptedException {
        Individual individual = new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2")));
        catalogManager.getIndividualManager().create(studyFqn, individual, null, ownerToken);

        ClinicalComment comment = new ClinicalComment(orgOwnerUserId, "my comment", new ArrayList<>(), "");
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis" + RandomStringUtils.randomAlphanumeric(3))
                .setDescription("My description").setType(ClinicalAnalysis.Type.SINGLE)
                .setQualityControl(new ClinicalAnalysisQualityControl(ClinicalAnalysisQualityControl.QualityControlSummary.LOW,
                        Collections.singletonList(comment), Collections.emptyList()))
                .setProband(individual);

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, true, INCLUDE_RESULT,
                ownerToken).first();

        assertEquals(ClinicalAnalysisQualityControl.QualityControlSummary.LOW, ca.getQualityControl().getSummary());
        assertEquals("my comment", ca.getQualityControl().getComments().get(0).getMessage());
        assertEquals(orgOwnerUserId, ca.getQualityControl().getComments().get(0).getAuthor());
        assertNotNull(ca.getQualityControl().getComments().get(0).getDate());

        String date = ca.getQualityControl().getComments().get(0).getDate();

        // Sleep 1 second so the date field can be different
        Thread.sleep(1000);
        ClinicalAnalysisQualityControlUpdateParam qualityControlUpdateParam =
                new ClinicalAnalysisQualityControlUpdateParam(ClinicalAnalysisQualityControl.QualityControlSummary.HIGH,
                        Collections.singletonList("other"), Collections.emptyList());
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams().setQualityControl(qualityControlUpdateParam);

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), updateParams, null, ownerToken);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), null, ownerToken).first();

        assertEquals(ClinicalAnalysisQualityControl.QualityControlSummary.HIGH, ca.getQualityControl().getSummary());
        assertEquals("other", ca.getQualityControl().getComments().get(0).getMessage());
        assertEquals(orgOwnerUserId, ca.getQualityControl().getComments().get(0).getAuthor());
        assertNotNull(ca.getQualityControl().getComments().get(0).getDate());
        assertNotEquals(date, ca.getQualityControl().getComments().get(0).getDate());
    }

    @Test
    public void automaticallyLockCaseTest() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual);
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis,
                INCLUDE_RESULT, ownerToken);
        assertEquals(ClinicalStatusValue.ClinicalStatusType.NOT_STARTED, clinical.first().getStatus().getType());
        assertFalse(clinical.first().isLocked());

        clinical = catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("CLOSED")), INCLUDE_RESULT, ownerToken);
        assertEquals("CLOSED", clinical.first().getStatus().getId());
        assertTrue(clinical.first().isLocked());

        clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical2")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setStatus(new ClinicalStatus().setId("CLOSED"))
                .setProband(individual);
        clinical = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken);
        assertEquals("CLOSED", clinical.first().getStatus().getId());
        assertEquals(ClinicalStatusValue.ClinicalStatusType.CLOSED, clinical.first().getStatus().getType());
        assertTrue(clinical.first().isLocked());
    }

    @Test
    public void createSingleClinicalAnalysisTestWithoutDisorder() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual);
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis,
                INCLUDE_RESULT, ownerToken);
        assertEquals(1, clinical.getNumResults());
        assertTrue(StringUtils.isNotEmpty(clinical.first().getDueDate()));
    }

    @Test
    public void queryClinicalAnalysisByDate() throws CatalogException {
        DataResult<ClinicalAnalysis> result = createDummyEnvironment(true, true);
        assertTrue(StringUtils.isNotEmpty(result.first().getDueDate()));

        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.DUE_DATE.key(),
                        ">=" + TimeUtils.getTime(TimeUtils.add24HtoDate(TimeUtils.getDate()))
                );
        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals(result.first().getId(), search.first().getId());

        String dueDate = TimeUtils.getTime();
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setDueDate(dueDate);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, result.first().getId(), updateParams, QueryOptions.empty(), ownerToken);
        search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(0, search.getNumResults());

        query.put(ClinicalAnalysisDBAdaptor.QueryParams.DUE_DATE.key(),
                "<" + TimeUtils.getTime(TimeUtils.add24HtoDate(TimeUtils.getDate())));
        search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals(result.first().getId(), search.first().getId());
        assertEquals(dueDate, search.first().getDueDate());
    }

    @Test
    public void createClinicalWithComments() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setComments(Arrays.asList(
                        new ClinicalComment("", "My first comment", Arrays.asList("tag1", "tag2"), ""),
                        new ClinicalComment("", "My second comment", Arrays.asList("1tag", "2tag"), "")))
                .setProband(individual);
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis,
                INCLUDE_RESULT, ownerToken);
        assertEquals(1, clinical.getNumResults());
        assertEquals(2, clinical.first().getComments().size());
        assertEquals(orgOwnerUserId, clinical.first().getComments().get(0).getAuthor());
        assertEquals("My first comment", clinical.first().getComments().get(0).getMessage());
        assertEquals(2, clinical.first().getComments().get(0).getTags().size());
        assertEquals(orgOwnerUserId, clinical.first().getComments().get(1).getAuthor());
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
        familyManager.create(studyFqn, family, QueryOptions.empty(), ownerToken);

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
        DataResult<ClinicalAnalysis> clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().create(studyFqn,
                clinicalAnalysis, INCLUDE_RESULT, ownerToken);

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
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setComments(Collections.singletonList(new ClinicalComment("", "My first comment", Arrays.asList("tag1", "tag2"), "")))
                .setProband(individual);

        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);

        List<ClinicalCommentParam> commentParamList = new ArrayList<>();
        commentParamList.add(new ClinicalCommentParam("My second comment", Arrays.asList("myTag")));
        commentParamList.add(new ClinicalCommentParam("My third comment", Arrays.asList("myTag2")));

        ObjectMap actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setComments(commentParamList), options, ownerToken);

        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(),
                QueryOptions.empty(), ownerToken);
        assertEquals(1, clinical.getNumResults());
        assertEquals(3, clinical.first().getComments().size());
        assertEquals(orgOwnerUserId, clinical.first().getComments().get(1).getAuthor());
        assertEquals("My second comment", clinical.first().getComments().get(1).getMessage());
        assertEquals(1, clinical.first().getComments().get(1).getTags().size());
        assertEquals("myTag", clinical.first().getComments().get(1).getTags().get(0));
        assertTrue(StringUtils.isNotEmpty(clinical.first().getComments().get(1).getDate()));

        assertEquals(orgOwnerUserId, clinical.first().getComments().get(2).getAuthor());
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

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setComments(commentParamList), options, ownerToken);
        clinical = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, clinical.getNumResults());
        assertEquals(3, clinical.first().getComments().size());
        assertEquals(orgOwnerUserId, clinical.first().getComments().get(1).getAuthor());
        assertEquals("My updated second comment", clinical.first().getComments().get(1).getMessage());
        assertEquals(2, clinical.first().getComments().get(1).getTags().size());
        assertEquals("myTag", clinical.first().getComments().get(1).getTags().get(0));
        assertEquals("myOtherTag", clinical.first().getComments().get(1).getTags().get(1));
        assertTrue(StringUtils.isNotEmpty(clinical.first().getComments().get(1).getDate()));

        assertEquals(orgOwnerUserId, clinical.first().getComments().get(2).getAuthor());
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

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setComments(commentParamList), options, ownerToken);

        clinical = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, clinical.getNumResults());
        assertEquals(1, clinical.first().getComments().size());
        assertEquals(orgOwnerUserId, clinical.first().getComments().get(0).getAuthor());
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

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setComments(commentParamList), options, ownerToken);

        clinical = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, clinical.getNumResults());
        assertEquals(0, clinical.first().getComments().size());

        // Remove dummy comment with no date
        commentParamList = Collections.singletonList(new ClinicalCommentParam("", Collections.emptyList()));
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                    .setComments(commentParamList), options, ownerToken);
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
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setComments(commentParamList), options, ownerToken);
    }

    @Test
    public void updateClinicalAnalysis() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setComments(Collections.singletonList(new ClinicalComment("", "My first comment", Arrays.asList("tag1", "tag2"), "")))
                .setProband(individual);

        ClinicalAnalysis clinical = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken).first();
        assertTrue(clinical.getAttributes().isEmpty());

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("a", "a_value");
        attributes.put("b", "b_value");

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setAttributes(attributes);
        clinical = catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), updateParams, INCLUDE_RESULT, ownerToken).first();
        assertFalse(clinical.getAttributes().isEmpty());
        assertEquals(2, clinical.getAttributes().size());
        assertEquals(attributes.get("a"), clinical.getAttributes().get("a"));
        assertEquals(attributes.get("b"), clinical.getAttributes().get("b"));
    }

    @Test
    public void leftJoinQueryTest() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        List<ClinicalVariant> findingList = new ArrayList<>();
        VariantAvro variantAvro = new VariantAvro("id1", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        ClinicalVariant cv1 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv1);
        variantAvro = new VariantAvro("id2", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariant cv2 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv2);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual)
                .setInterpretation(new Interpretation()
                        .setPrimaryFindings(findingList)
                );
        ClinicalAnalysis clinical = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken).first();
        assertEquals(1, clinical.getVersion());
        assertEquals(1, clinical.getInterpretation().getVersion());

        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), clinical.getId())
                .append(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key() + "." + InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS_ID.key(), "id3");
        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(0, result.getNumResults());

        query.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key() + "." + InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS_ID.key(), "id2");
        result = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
    }


    @Test
    public void versioningTest() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        List<ClinicalVariant> findingList = new ArrayList<>();
        VariantAvro variantAvro = new VariantAvro("id1", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        ClinicalVariant cv1 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv1);
        variantAvro = new VariantAvro("id2", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariant cv2 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv2);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual)
                .setInterpretation(new Interpretation()
                        .setPrimaryFindings(findingList)
                );
        ClinicalAnalysis clinical = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken).first();
        assertEquals(1, clinical.getVersion());
        assertEquals(1, clinical.getInterpretation().getVersion());

        // Update clinical analysis
        clinical = catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setDescription("my new description"), INCLUDE_RESULT, ownerToken).first();
        assertEquals("my new description", clinical.getDescription());
        assertEquals(2, clinical.getVersion());
        assertEquals(1, clinical.getInterpretation().getVersion());

        // Update interpretation
        Interpretation interpretation = catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(),
                clinical.getInterpretation().getId(), new InterpretationUpdateParams().setDescription("my new interpretation description"),
                null, INCLUDE_RESULT, ownerToken).first();
        assertEquals("my new interpretation description", interpretation.getDescription());
        assertEquals(2, interpretation.getVersion());

        // Get clinical Analysis
        clinical = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals("my new description", clinical.getDescription());
        assertEquals(3, clinical.getVersion());
        assertEquals("my new interpretation description", clinical.getInterpretation().getDescription());
        assertEquals(2, clinical.getInterpretation().getVersion());
    }

    @Test
    public void createRepeatedInterpretationPrimaryFindings() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        List<ClinicalVariant> findingList = new ArrayList<>();
        VariantAvro variantAvro = new VariantAvro("id1", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        ClinicalVariant cv = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv);
        findingList.add(cv);
        variantAvro = new VariantAvro("id2", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        cv = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
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
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);
    }

    @Test
    public void createRepeatedInterpretationSecondaryFindings() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        List<ClinicalVariant> findingList = new ArrayList<>();
        VariantAvro variantAvro = new VariantAvro("id1", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        ClinicalVariant cv = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv);
        findingList.add(cv);
        variantAvro = new VariantAvro("id2", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        cv = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
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
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);
    }

    @Test
    public void updatePrimaryFindings() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        List<ClinicalVariant> findingList = new ArrayList<>();
        VariantAvro variantAvro = new VariantAvro("id1", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        ClinicalVariant cv1 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv1);
        variantAvro = new VariantAvro("id2", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariant cv2 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv2);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual)
                .setInterpretation(new Interpretation()
                        .setPrimaryFindings(findingList)
                );
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);

        Interpretation interpretation = catalogManager.getInterpretationManager().get(studyFqn, clinicalAnalysis.getId() + ".1", QueryOptions.empty(),
                ownerToken).first();
        assertEquals(2, interpretation.getPrimaryFindings().size());
        assertEquals(2, interpretation.getStats().getPrimaryFindings().getNumVariants());
        assertEquals(2, (int) interpretation.getStats().getPrimaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Add new finding
        findingList = new ArrayList<>();
        variantAvro = new VariantAvro("id3", null, "chr3", 2, 3, "", "", "+", null, 1, null, null, null);
        evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method2");
        ClinicalVariant cv3 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv3);

        InterpretationUpdateParams updateParams = new InterpretationUpdateParams()
                .setPrimaryFindings(findingList);
        ObjectMap actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null, options,
                ownerToken);
        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), ownerToken).first();
        assertEquals(3, interpretation.getPrimaryFindings().size());
        assertEquals("method2", interpretation.getPrimaryFindings().get(2).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id3", interpretation.getPrimaryFindings().get(2).getId());
        assertEquals(3, interpretation.getStats().getPrimaryFindings().getNumVariants());
        assertEquals(3, (int) interpretation.getStats().getPrimaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Add existing finding
        cv3.setDiscussion(new ClinicalDiscussion("author", "20220728", "My discussion"));
        try {
            catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null,
                    options, ownerToken);
            fail("It should not allow adding an already existing finding");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("repeated"));
        }

        // Remove findings
        updateParams = new InterpretationUpdateParams()
                .setPrimaryFindings(Arrays.asList(cv1, cv3));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null, options,
                ownerToken);
        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), ownerToken).first();
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

        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null, options,
                ownerToken);
        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), ownerToken).first();
        assertEquals(3, interpretation.getPrimaryFindings().size());
        assertEquals("method", interpretation.getPrimaryFindings().get(0).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id1", interpretation.getPrimaryFindings().get(0).getId());
        assertEquals("method", interpretation.getPrimaryFindings().get(1).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id2", interpretation.getPrimaryFindings().get(1).getId());
        assertEquals("method2", interpretation.getPrimaryFindings().get(2).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id3", interpretation.getPrimaryFindings().get(2).getId());
        assertEquals("author", interpretation.getPrimaryFindings().get(2).getDiscussion().getAuthor());
        assertEquals("20220728", interpretation.getPrimaryFindings().get(2).getDiscussion().getDate());
        assertEquals("My discussion", interpretation.getPrimaryFindings().get(2).getDiscussion().getText());
        assertTrue(StringUtils.isNotEmpty(interpretation.getPrimaryFindings().get(2).getDiscussion().getDate()));
        assertEquals(3, interpretation.getStats().getPrimaryFindings().getNumVariants());
        assertEquals(3, (int) interpretation.getStats().getPrimaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Replace findings
        cv2.setEvidences(Collections.singletonList(new ClinicalVariantEvidence().setInterpretationMethodName("AnotherMethodName")));
        cv3.setEvidences(Collections.singletonList(new ClinicalVariantEvidence().setInterpretationMethodName("YetAnotherMethodName")));

        updateParams = new InterpretationUpdateParams()
                .setPrimaryFindings(Arrays.asList(cv2, cv3));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null, options,
                ownerToken);
        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), ownerToken).first();
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
        cv1 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);

        updateParams = new InterpretationUpdateParams()
                .setPrimaryFindings(Collections.singletonList(cv1));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null,
                    options, ownerToken);
            fail("It should fail because finding id is missing");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("id"));
        }

        // Remove finding with missing id
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", updateParams, null,
                    options, ownerToken);
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
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        List<ClinicalVariant> findingList = new ArrayList<>();
        VariantAvro variantAvro = new VariantAvro("id1", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method");
        ClinicalVariant cv1 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv1);
        variantAvro = new VariantAvro("id2", null, "chr2", 1, 2, "", "", "+", null, 1, null, null, null);
        ClinicalVariant cv2 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv2);

        ClinicalAnalysis ca = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual)
                .setInterpretation(new Interpretation()
                        .setSecondaryFindings(findingList)
                );
        catalogManager.getClinicalAnalysisManager().create(studyFqn, ca, QueryOptions.empty(), ownerToken);

        Interpretation interpretation = catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(),
                ownerToken).first();
        assertEquals(2, interpretation.getSecondaryFindings().size());
        assertNotNull(interpretation.getStats());
        assertEquals(2, interpretation.getStats().getSecondaryFindings().getNumVariants());
        assertEquals(2,
                (int) interpretation.getStats().getSecondaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Add new finding
        findingList = new ArrayList<>();
        variantAvro = new VariantAvro("id3", null, "chr3", 2, 3, "", "", "+", null, 1, null, null, null);
        evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method2");
        ClinicalVariant cv3 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);
        findingList.add(cv3);

        InterpretationUpdateParams updateParams = new InterpretationUpdateParams()
                .setSecondaryFindings(findingList);
        ObjectMap actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", updateParams, null, options,
                ownerToken);
        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(), ownerToken).first();
        assertEquals(3, interpretation.getSecondaryFindings().size());
        assertEquals("method2", interpretation.getSecondaryFindings().get(2).getEvidences().get(0).getInterpretationMethodName());
        assertEquals("id3", interpretation.getSecondaryFindings().get(2).getId());
        assertNotNull(interpretation.getStats());
        assertEquals(3, interpretation.getStats().getSecondaryFindings().getNumVariants());
        assertEquals(3,
                (int) interpretation.getStats().getSecondaryFindings().getStatusCount().get(ClinicalVariant.Status.NOT_REVIEWED));

        // Add existing finding
        try {
            catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", updateParams, null,
                    options, ownerToken);
            fail("It should not allow adding an already existing finding");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("repeated"));
        }

        // Remove findings
        updateParams = new InterpretationUpdateParams()
                .setSecondaryFindings(Arrays.asList(cv1, cv3));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", updateParams, null, options,
                ownerToken);
        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(), ownerToken).first();
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

        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", updateParams, null, options,
                ownerToken);
        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(), ownerToken).first();
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

        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", updateParams, null, options,
                ownerToken);
        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(), ownerToken).first();
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
        cv1 = new ClinicalVariant(variantAvro, Collections.singletonList(evidence), null, null, new ClinicalDiscussion(),
                ClinicalVariant.Status.NOT_REVIEWED, Collections.emptyList(), null);

        updateParams = new InterpretationUpdateParams()
                .setSecondaryFindings(Collections.singletonList(cv1));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", updateParams, null,
                    options, ownerToken);
            fail("It should fail because finding id is missing");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("id"));
        }

        // Remove finding with missing id
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.REPLACE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", updateParams, null,
                    options, ownerToken);
            fail("It should fail because finding id is missing");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("id"));
        }
    }

    @Test
    public void updateStatusTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        Interpretation interpretation = catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(),
                ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, ownerToken).first();

        // Create 2 allowed statuses of type CLOSED
        ClinicalAnalysisStudyConfiguration studyConfiguration = ClinicalAnalysisStudyConfiguration.defaultConfiguration();
        List<ClinicalStatusValue> statusValueList = new ArrayList<>();
        for (ClinicalStatusValue status : studyConfiguration.getStatus()) {
            if (!status.getType().equals(ClinicalStatusValue.ClinicalStatusType.CLOSED)) {
                statusValueList.add(status);
            }
        }
        // Add two statuses of type CLOSED
        statusValueList.add(new ClinicalStatusValue("closed1", "my desc", ClinicalStatusValue.ClinicalStatusType.CLOSED));
        statusValueList.add(new ClinicalStatusValue("closed2", "my desc", ClinicalStatusValue.ClinicalStatusType.CLOSED));
        studyConfiguration.setStatus(statusValueList);
        catalogManager.getClinicalAnalysisManager().configureStudy(studyFqn, studyConfiguration, studyAdminToken1);

        // Update status to one of the new statuses
        catalogManager.getClinicalAnalysisManager().update(studyFqn, ca.getId(),
                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("closed1")), QueryOptions.empty(), studyAdminToken1);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), studyAdminToken1).first();
        assertEquals("closed1", ca.getStatus().getId());
        assertEquals(ClinicalStatusValue.ClinicalStatusType.CLOSED, ca.getStatus().getType());

        // Update status to the other new CLOSED status
        catalogManager.getClinicalAnalysisManager().update(studyFqn, ca.getId(),
                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("closed2")), QueryOptions.empty(), studyAdminToken1);
        assertEquals("closed1", ca.getStatus().getId());
        assertEquals(ClinicalStatusValue.ClinicalStatusType.CLOSED, ca.getStatus().getType());
    }

    @Test
    public void updateInterpretationComments() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual)
                .setInterpretation(new Interpretation()
                        .setComments(Collections.singletonList(new ClinicalComment("", "My first comment", Arrays.asList("tag1", "tag2"),
                                "")))
                );
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);

        List<ClinicalCommentParam> commentParamList = new ArrayList<>();
        commentParamList.add(new ClinicalCommentParam("My second comment", Arrays.asList("myTag")));
        commentParamList.add(new ClinicalCommentParam("My third comment", Arrays.asList("myTag2")));

        ObjectMap actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", new InterpretationUpdateParams()
                .setComments(commentParamList), null, options, ownerToken);

        OpenCGAResult<Interpretation> interpretation = catalogManager.getInterpretationManager().get(studyFqn, clinicalAnalysis.getId() + ".1",
                QueryOptions.empty(), ownerToken);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(3, interpretation.first().getComments().size());
        assertEquals(orgOwnerUserId, interpretation.first().getComments().get(1).getAuthor());
        assertEquals("My second comment", interpretation.first().getComments().get(1).getMessage());
        assertEquals(1, interpretation.first().getComments().get(1).getTags().size());
        assertEquals("myTag", interpretation.first().getComments().get(1).getTags().get(0));
        assertTrue(StringUtils.isNotEmpty(interpretation.first().getComments().get(1).getDate()));

        assertEquals(orgOwnerUserId, interpretation.first().getComments().get(2).getAuthor());
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

        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", new InterpretationUpdateParams()
                .setComments(commentParamList), null, options, ownerToken);
        interpretation = catalogManager.getInterpretationManager().get(studyFqn, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), ownerToken);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(3, interpretation.first().getComments().size());
        assertEquals(orgOwnerUserId, interpretation.first().getComments().get(1).getAuthor());
        assertEquals("My updated second comment", interpretation.first().getComments().get(1).getMessage());
        assertEquals(2, interpretation.first().getComments().get(1).getTags().size());
        assertEquals("myTag", interpretation.first().getComments().get(1).getTags().get(0));
        assertEquals("myOtherTag", interpretation.first().getComments().get(1).getTags().get(1));
        assertTrue(StringUtils.isNotEmpty(interpretation.first().getComments().get(1).getDate()));

        assertEquals(orgOwnerUserId, interpretation.first().getComments().get(2).getAuthor());
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

        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", new InterpretationUpdateParams()
                .setComments(commentParamList), null, options, ownerToken);

        interpretation = catalogManager.getInterpretationManager().get(studyFqn, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), ownerToken);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(1, interpretation.first().getComments().size());
        assertEquals(orgOwnerUserId, interpretation.first().getComments().get(0).getAuthor());
        assertEquals("My updated second comment", interpretation.first().getComments().get(0).getMessage());
        assertEquals(2, interpretation.first().getComments().get(0).getTags().size());
        assertEquals("myTag", interpretation.first().getComments().get(0).getTags().get(0));
        assertTrue(StringUtils.isNotEmpty(interpretation.first().getComments().get(0).getDate()));

        commentParamList = Arrays.asList(
                ClinicalCommentParam.of(interpretation.first().getComments().get(0))
        );
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", new InterpretationUpdateParams()
                .setComments(commentParamList), null, options, ownerToken);

        interpretation = catalogManager.getInterpretationManager().get(studyFqn, clinicalAnalysis.getId() + ".1", QueryOptions.empty(), ownerToken);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(0, interpretation.first().getComments().size());

        // Remove dummy comment with no date
        commentParamList = Collections.singletonList(new ClinicalCommentParam("", Collections.emptyList()));
        actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1",
                    new InterpretationUpdateParams()
                            .setComments(commentParamList), null, options, ownerToken);
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
        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getId() + ".1", new InterpretationUpdateParams()
                .setComments(commentParamList), null, options, ownerToken);
    }

    @Test
    public void assignPermissions() throws CatalogException {
        ClinicalAnalysis clinicalAnalysis = createDummyEnvironment(true, false).first();
        catalogManager.getUserManager().create("external", "User Name", "external@mail.com", TestParamConstants.PASSWORD, organizationId, null,
                opencgaToken);

        OpenCGAResult<AclEntryList<ClinicalAnalysisPermissions>> aclResult =
                catalogManager.getClinicalAnalysisManager().getAcls(studyFqn, Collections.singletonList(clinicalAnalysis.getId()), "external",
                        false, ownerToken);
        assertEquals(1, aclResult.getNumResults());
        assertEquals(1, aclResult.first().getAcl().size());
        assertEquals("external", aclResult.first().getAcl().get(0).getMember());
        assertNull(aclResult.first().getAcl().get(0).getPermissions());

        OpenCGAResult<AclEntryList<FamilyPermissions>> fAclResult = catalogManager.getFamilyManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getFamily().getId()), "external", false, ownerToken);
        assertEquals(1, fAclResult.getNumResults());
        assertEquals(1, fAclResult.first().getAcl().size());
        assertEquals("external", fAclResult.first().getAcl().get(0).getMember());
        assertNull(fAclResult.first().getAcl().get(0).getPermissions());

        OpenCGAResult<AclEntryList<IndividualPermissions>> iAclResult = catalogManager.getIndividualManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getProband().getId()), "external", false, ownerToken);
        assertEquals(1, iAclResult.getNumResults());
        assertEquals(1, iAclResult.first().getAcl().size());
        assertEquals("external", iAclResult.first().getAcl().get(0).getMember());
        assertNull(iAclResult.first().getAcl().get(0).getPermissions());

        OpenCGAResult<AclEntryList<SamplePermissions>> sAclResult = catalogManager.getSampleManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getProband().getSamples().get(0).getId()), "external", false, ownerToken);
        assertEquals(1, sAclResult.getNumResults());
        assertEquals(1, sAclResult.first().getAcl().size());
        assertEquals("external", sAclResult.first().getAcl().get(0).getMember());
        assertNull(sAclResult.first().getAcl().get(0).getPermissions());

        // Assign permissions to clinical analysis without propagating the permissions
        catalogManager.getClinicalAnalysisManager().updateAcl(studyFqn, Collections.singletonList(clinicalAnalysis.getId()), "external",
                new AclParams(ClinicalAnalysisPermissions.DELETE.name()), ParamUtils.AclAction.ADD, false, ownerToken);

        aclResult = catalogManager.getClinicalAnalysisManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getId()), "external", false, ownerToken);
        assertEquals(1, aclResult.getNumResults());
        assertEquals(3, aclResult.first().getAcl().get(0).getPermissions().size());

        fAclResult = catalogManager.getFamilyManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getFamily().getId()), "external", false, ownerToken);
        assertEquals(1, fAclResult.getNumResults());
        assertEquals(1, fAclResult.first().getAcl().size());
        assertEquals("external", fAclResult.first().getAcl().get(0).getMember());
        assertNull(fAclResult.first().getAcl().get(0).getPermissions());

        iAclResult = catalogManager.getIndividualManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getProband().getId()), "external", false, ownerToken);
        assertEquals(1, iAclResult.getNumResults());
        assertEquals(1, iAclResult.first().getAcl().size());
        assertEquals("external", iAclResult.first().getAcl().get(0).getMember());
        assertNull(iAclResult.first().getAcl().get(0).getPermissions());

        sAclResult = catalogManager.getSampleManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getProband().getSamples().get(0).getId()), "external", false, ownerToken);
        assertEquals(1, sAclResult.getNumResults());
        assertEquals(1, sAclResult.first().getAcl().size());
        assertEquals("external", sAclResult.first().getAcl().get(0).getMember());
        assertNull(sAclResult.first().getAcl().get(0).getPermissions());

        // Assign permissions to clinical analysis PROPAGATING the permissions
        catalogManager.getClinicalAnalysisManager().updateAcl(studyFqn, Collections.singletonList(clinicalAnalysis.getId()), "external",
                new AclParams(ClinicalAnalysisPermissions.DELETE.name()), ParamUtils.AclAction.ADD, true,
                ownerToken);

        aclResult = catalogManager.getClinicalAnalysisManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getId()), "external", false, ownerToken);
        assertEquals(1, aclResult.getNumResults());
        assertEquals(3, aclResult.first().getAcl().get(0).getPermissions().size());

        fAclResult = catalogManager.getFamilyManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getFamily().getId()), "external", false, ownerToken);
        assertEquals(1, fAclResult.getNumResults());
        assertEquals(2, fAclResult.first().getAcl().get(0).getPermissions().size());

        iAclResult = catalogManager.getIndividualManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getProband().getId()), "external", false, ownerToken);
        assertEquals(1, iAclResult.getNumResults());
        assertEquals(2, iAclResult.first().getAcl().get(0).getPermissions().size());

        sAclResult = catalogManager.getSampleManager().getAcls(studyFqn,
                Collections.singletonList(clinicalAnalysis.getProband().getSamples().get(0).getId()), "external", false, ownerToken);
        assertEquals(1, sAclResult.getNumResults());
        assertEquals(2, sAclResult.first().getAcl().get(0).getPermissions().size());
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

        assertEquals(catalogManager.getSampleManager().get(studyFqn, "sample2", SampleManager.INCLUDE_SAMPLE_IDS, ownerToken)
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

        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                updateParams, QueryOptions.empty(), ownerToken);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, dummyEnvironment.first().getId(), QueryOptions.empty(),
                ownerToken).first();
        assertEquals("My description", ca.getDescription());
        assertEquals("URGENT", ca.getPriority().getId());
    }

    @Test
    public void adminPermissionTest() throws CatalogException {
        // Add ADMIN permissions to the user2
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId2,
                new StudyAclParams(StudyPermissions.Permissions.ADMIN_CLINICAL_ANALYSIS.name(), null), ParamUtils.AclAction.SET, ownerToken);

        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        // Update ClinicalAnalysis with user1
        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                new ClinicalAnalysisUpdateParams().setDescription("My description"), INCLUDE_RESULT, normalToken1).first();
        assertEquals("My description", clinicalAnalysis.getDescription());

        // Update ClinicalAnalysis with user2
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                new ClinicalAnalysisUpdateParams().setDescription("My description 2"), INCLUDE_RESULT, normalToken2).first();
        assertEquals("My description 2", clinicalAnalysis.getDescription());

        // Set status to CLOSED with user1 - FAIL
        assertThrows(CatalogAuthorizationException.class, () ->
                catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                        new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("CLOSED")), INCLUDE_RESULT, normalToken1)
        );

        // Set status to CLOSED with user2 - WORKS
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("CLOSED")), INCLUDE_RESULT, normalToken2).first();
        assertEquals("CLOSED", clinicalAnalysis.getStatus().getId());
        assertTrue(clinicalAnalysis.isLocked());

        // Unset status from CLOSED to other with user1 - FAIL
        assertThrows(CatalogAuthorizationException.class, () ->
                catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                        new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("READY_FOR_INTERPRETATION")), INCLUDE_RESULT, normalToken1)
        );

        // Edit CLOSED ClinicalAnalysis from user with ADMIN permission
        assertThrows(CatalogException.class, () ->
                catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                        new ClinicalAnalysisUpdateParams().setDescription("new description"), INCLUDE_RESULT, normalToken2)
        );

        // Send CLOSED status and edit description from CLOSED ClinicalAnalysis from user with ADMIN permission
        assertThrows(CatalogException.class, () ->
                        catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("CLOSED")).setDescription("new description"),
                                INCLUDE_RESULT, normalToken2)
        );

        // Remove CLOSED status and edit description from CLOSED ClinicalAnalysis from user with ADMIN permission
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("READY_FOR_INTERPRETATION")).setDescription("new description"),
                INCLUDE_RESULT, normalToken2).first();
        assertEquals("READY_FOR_INTERPRETATION", clinicalAnalysis.getStatus().getId());
        assertEquals("new description", clinicalAnalysis.getDescription());

        // Set status to CLOSED with study admin - WORKS
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("CLOSED")), INCLUDE_RESULT, studyAdminToken1).first();
        assertEquals("CLOSED", clinicalAnalysis.getStatus().getId());

        // Unset status from CLOSED to other with study admin - WORKS
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("READY_FOR_INTERPRETATION")).setLocked(false), INCLUDE_RESULT,
                studyAdminToken1).first();
        assertEquals("READY_FOR_INTERPRETATION", clinicalAnalysis.getStatus().getId());

        // Update ClinicalAnalysis with user1 - WORKS
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                new ClinicalAnalysisUpdateParams().setDescription("My description 3"), INCLUDE_RESULT, normalToken1).first();
        assertEquals("My description 3", clinicalAnalysis.getDescription());
    }

    @Test
    public void updateCustomStatusTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, QueryOptions.empty(), ownerToken).first();
        ClinicalAnalysisStudyConfiguration configuration = study.getInternal().getConfiguration().getClinical();

        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        ClinicalStatusValue status = configuration.getStatus().get(0);

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setStatus(new StatusParam(status.getId()));
        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                updateParams, QueryOptions.empty(), ownerToken);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, dummyEnvironment.first().getId(), QueryOptions.empty(),
                ownerToken).first();
        assertEquals(status.getId(), ca.getStatus().getId());
        assertEquals(status.getDescription(), ca.getStatus().getDescription());
        assertNotNull(ca.getStatus().getDate());
    }

    @Test
    public void updateCustomPriorityTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, QueryOptions.empty(), ownerToken).first();
        ClinicalAnalysisStudyConfiguration configuration = study.getInternal().getConfiguration().getClinical();

        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        ClinicalPriorityValue priority = configuration.getPriorities().get(1);

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setPriority(new PriorityParam(priority.getId()));
        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                updateParams, QueryOptions.empty(), ownerToken);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, dummyEnvironment.first().getId(), QueryOptions.empty(),
                ownerToken).first();
        assertEquals(priority.getId(), ca.getPriority().getId());
        assertEquals(priority.getDescription(), ca.getPriority().getDescription());
        assertEquals(priority.getRank(), ca.getPriority().getRank());
        assertNotNull(ca.getPriority().getDate());
    }

    @Test
    public void updateCustomFlagTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, QueryOptions.empty(), ownerToken).first();
        ClinicalAnalysisStudyConfiguration configuration = study.getInternal().getConfiguration().getClinical();

        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        FlagValue flag1 = configuration.getFlags().get(1);
        FlagValue flag2 = configuration.getFlags().get(3);
        FlagValue flag3 = configuration.getFlags().get(4);

        ObjectMap actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key(), ParamUtils.BasicUpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setFlags(Arrays.asList(new FlagValueParam(flag1.getId()), new FlagValueParam(flag1.getId()),
                        new FlagValueParam(flag2.getId())));
        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                updateParams, options, ownerToken);
        assertEquals(1, update.getNumUpdated());

        updateParams = new ClinicalAnalysisUpdateParams()
                .setFlags(Arrays.asList(new FlagValueParam(flag2.getId()), new FlagValueParam(flag3.getId())));
        update = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(), updateParams, options,
                ownerToken);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, dummyEnvironment.first().getId(), QueryOptions.empty(),
                ownerToken).first();
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
        flag1 = configuration.getFlags().get(0);
        flag2 = configuration.getFlags().get(2);

        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);

        updateParams = new ClinicalAnalysisUpdateParams()
                .setFlags(Arrays.asList(new FlagValueParam(flag1.getId()), new FlagValueParam(flag2.getId())));
        update = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(), updateParams, options,
                ownerToken);
        assertEquals(1, update.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, dummyEnvironment.first().getId(), QueryOptions.empty(),
                ownerToken).first();
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
        update = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(), updateParams, options,
                ownerToken);
        assertEquals(1, update.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, dummyEnvironment.first().getId(), QueryOptions.empty(),
                ownerToken).first();
        assertEquals(1, ca.getFlags().size());
        assertEquals(flag2.getId(), ca.getFlags().get(0).getId());
        assertEquals(flag2.getDescription(), ca.getFlags().get(0).getDescription());
        assertNotNull(ca.getFlags().get(0).getDate());
    }

    @Test
    public void updateCustomConsentTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, QueryOptions.empty(), ownerToken).first();
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
        OpenCGAResult<ClinicalAnalysis> update = catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(),
                updateParams, QueryOptions.empty(), ownerToken);
        assertEquals(1, update.getNumUpdated());

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, dummyEnvironment.first().getId(), QueryOptions.empty(),
                ownerToken).first();
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
        Study study = catalogManager.getStudyManager().get(studyFqn, QueryOptions.empty(), ownerToken).first();
        InterpretationStudyConfiguration configuration = study.getInternal().getConfiguration().getClinical().getInterpretation();

        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, true);
        ClinicalStatusValue status = configuration.getStatus().get(1);

        InterpretationUpdateParams updateParams = new InterpretationUpdateParams()
                .setStatus(new StatusParam(status.getId()));
        OpenCGAResult<Interpretation> update = catalogManager.getInterpretationManager().update(studyFqn, dummyEnvironment.first().getId(),
                dummyEnvironment.first().getInterpretation().getId(), updateParams, null, QueryOptions.empty(), ownerToken);
        assertEquals(1, update.getNumUpdated());

        Interpretation interpretation = catalogManager.getInterpretationManager().get(studyFqn,
                dummyEnvironment.first().getInterpretation().getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(status.getId(), interpretation.getStatus().getId());
        assertEquals(status.getDescription(), interpretation.getStatus().getDescription());
        assertNotNull(interpretation.getStatus().getDate());
    }

    @Test
    public void createInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), ownerToken);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".1", ca.getInterpretation().getId());

        // Delete old interpretation and create a new primary one
        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(),
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        catalogManager.getInterpretationManager().delete(studyFqn, ca.getId(), Collections.singletonList(ca.getId() + ".1"), ownerToken);

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(0, ca.getSecondaryInterpretations().size());
        assertEquals(0, catalogManager.getInterpretationManager().search(studyFqn,
                        new Query(InterpretationDBAdaptor.QueryParams.ID.key(), ca.getId() + ".1"), QueryOptions.empty(), ownerToken)
                .getNumResults());
        // Old interpretation was deleted
        assertEquals(1, catalogManager.getInterpretationManager().search(studyFqn, new Query()
                        .append(InterpretationDBAdaptor.QueryParams.ID.key(), ca.getId() + ".1")
                        .append(InterpretationDBAdaptor.QueryParams.DELETED.key(), true), QueryOptions.empty(), ownerToken)
                .getNumResults());

        // Interpretation2 should be moved to secondary interpretations
        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(),
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".3", ca.getInterpretation().getId());
        assertEquals(1, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());

        // Interpretation4 should be added to secondary interpretations
        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(),
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".3", ca.getInterpretation().getId());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing");
        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), null, QueryOptions.empty(), ownerToken);
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
        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), ownerToken);

        Interpretation interpretationResult = catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1",
                QueryOptions.empty(), ownerToken).first();
        assertEquals(ca.getId() + ".1", interpretationResult.getId());
        assertEquals(1, interpretationResult.getVersion());
        assertEquals("description", interpretationResult.getDescription());
        assertNotNull(interpretationResult.getMethod());
        assertEquals("name", interpretationResult.getMethod().getName());
        assertEquals(1, interpretationResult.getPrimaryFindings().size());
        assertEquals(1, interpretationResult.getSecondaryFindings().size());
        assertEquals(1, interpretationResult.getComments().size());

        catalogManager.getInterpretationManager().clear(studyFqn, ca.getId(), Collections.singletonList(ca.getId() + ".1"), ownerToken);
        interpretationResult = catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(),
                ownerToken).first();
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
        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        Interpretation interpretationResult = catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1",
                QueryOptions.empty(), ownerToken).first();
        assertEquals(ca.getId() + ".1", interpretationResult.getId());
        assertEquals(1, interpretationResult.getVersion());
        assertEquals("description", interpretationResult.getDescription());
        assertNotNull(interpretationResult.getMethod());
        assertEquals("name", interpretationResult.getMethod().getName());
        assertEquals(1, interpretationResult.getPrimaryFindings().size());
        assertEquals(1, interpretationResult.getSecondaryFindings().size());
        assertEquals(1, interpretationResult.getComments().size());

        catalogManager.getInterpretationManager().clear(studyFqn, ca.getId(), Collections.singletonList(ca.getId() + ".1"), ownerToken);
        interpretationResult = catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(),
                ownerToken).first();
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

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), ownerToken);

        ClinicalVariant clinicalVariant = new ClinicalVariant();
        clinicalVariant.setId("variantId");
        clinicalVariant.setChromosome("chr1");
        clinicalVariant.setStart(2);
        clinicalVariant.setEnd(3);
        clinicalVariant.setLength(2);

        InterpretationUpdateParams params = new InterpretationUpdateParams()
                .setPrimaryFindings(Collections.singletonList(clinicalVariant));
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1",
                params, null, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("repeated");
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", params, null, QueryOptions.empty(),
                ownerToken);
    }

//    @Test
//    public void mergeInterpretationFindingsTest() throws CatalogException {
//        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();
//
//        Interpretation interpretation = new Interpretation().setId("interpretation1");
//        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), interpretation, ParamUtils.SaveInterpretationAs.PRIMARY,
//                QueryOptions.empty(), token);
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
//        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), "interpretation1",
//                params, null, QueryOptions.empty(), token);
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
//        OpenCGAResult<Interpretation> merge = catalogManager.getInterpretationManager().merge(studyFqn, ca.getId(), interpretation.getId(),
//                interpretationAux, Collections.emptyList(), token);
//        assertEquals(1, merge.getNumUpdated());
//
//        Interpretation first = catalogManager.getInterpretationManager().get(studyFqn, interpretation.getId(), QueryOptions.empty(),
//                token).first();
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
//        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), interpretationAux, ParamUtils.SaveInterpretationAs.SECONDARY,
//                QueryOptions.empty(), token);
//
//        merge = catalogManager.getInterpretationManager().merge(studyFqn, ca.getId(), interpretation.getId(), interpretationAux.getId(),
//                Collections.singletonList("variantId3"), token);
//        assertEquals(1, merge.getNumUpdated());
//
//        first = catalogManager.getInterpretationManager().get(studyFqn, interpretation.getId(), QueryOptions.empty(), token).first();
//        assertEquals(3, first.getMethods().size());
//        assertEquals(3, first.getPrimaryFindings().size());
//        assertEquals(Arrays.asList("method1", "method2"), first.getPrimaryFindings().get(0).getInterpretationMethodNames());
//        assertEquals(Collections.singletonList("method2"), first.getPrimaryFindings().get(1).getInterpretationMethodNames());
//        assertEquals(Collections.singletonList("method3"), first.getPrimaryFindings().get(2).getInterpretationMethodNames());
//    }

    @Test
    public void searchInterpretationVersion() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), ownerToken);

        InterpretationUpdateParams params = new InterpretationUpdateParams().setAnalyst(new ClinicalAnalystParam(normalUserId2));
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1",
                params, null, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, InterpretationDBAdaptor.QueryParams.VERSION.key());
        result = catalogManager.getInterpretationManager().get(studyFqn, Collections.singletonList(ca.getId() + ".1"),
                new Query(Constants.ALL_VERSIONS, true), options, false, ownerToken);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getInterpretationManager().get(studyFqn, Collections.singletonList(ca.getId() + ".1"),
                new Query(InterpretationDBAdaptor.QueryParams.VERSION.key(), "1,2"), options, false, ownerToken);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getInterpretationManager().get(studyFqn, Collections.singletonList(ca.getId() + ".1"),
                new Query(InterpretationDBAdaptor.QueryParams.VERSION.key(), "All"), options, false, ownerToken);
        assertEquals(2, result.getNumResults());

        try {
            catalogManager.getInterpretationManager().get(studyFqn, Arrays.asList(ca.getId() + ".1", ca.getId() + ".2"),
                    new Query(Constants.ALL_VERSIONS, true), options, false, ownerToken);
            fail("The previous call should fail because it should not be possible to fetch all versions of multiple interpretations");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("multiple"));
        }

        try {
            catalogManager.getInterpretationManager().get(studyFqn, Arrays.asList(ca.getId() + ".1", "interpretation2"),
                    new Query(InterpretationDBAdaptor.QueryParams.VERSION.key(), "1"), options, false, ownerToken);
            fail("The previous call should fail users cannot fetch a concrete version for multiple interpretations");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("multiple"));
        }
    }

    @Test
    public void revertInterpretationVersion() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), ownerToken);

        // version 2
        InterpretationUpdateParams params = new InterpretationUpdateParams().setAnalyst(new ClinicalAnalystParam(normalUserId2));
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", params, null, QueryOptions.empty(),
                ownerToken);

        // version 3
        params = new InterpretationUpdateParams().setComments(Collections.singletonList(new ClinicalCommentParam("my first comment",
                Collections.singletonList("tag1"))));
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", params, null, QueryOptions.empty(),
                ownerToken);

        // version 4
        params = new InterpretationUpdateParams().setComments(Collections.singletonList(new ClinicalCommentParam("my second comment",
                Collections.singletonList("tag2"))));
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", params, null, QueryOptions.empty(),
                ownerToken);

        // Current status
        Interpretation interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(), ownerToken).first();
        assertEquals(4, interpretation.getVersion());
        assertEquals(normalUserId2, interpretation.getAnalyst().getId());
        assertEquals(2, interpretation.getComments().size());
        assertEquals(1, interpretation.getComments().get(0).getTags().size());
        assertEquals("tag1", interpretation.getComments().get(0).getTags().get(0));
        assertEquals(1, interpretation.getComments().get(1).getTags().size());
        assertEquals("tag2", interpretation.getComments().get(1).getTags().get(0));

        // TEST REVERT
        try {
            catalogManager.getInterpretationManager().revert(studyFqn, ca.getId(), ca.getId() + ".1", 0, ownerToken);
            fail("A CatalogException should be raised pointing we cannot set to a version equal or inferior to 0");
        } catch (CatalogException e) {
        }

        try {
            catalogManager.getInterpretationManager().revert(studyFqn, ca.getId(), ca.getId() + ".1", 5, ownerToken);
            fail("A CatalogException should be raised pointing we cannot set to a version above the current one");
        } catch (CatalogException e) {
        }

        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().revert(studyFqn, ca.getId(), ca.getId() + ".1", 2,
                ownerToken);
        assertEquals(1, result.getNumUpdated());

        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(), ownerToken).first();
        assertEquals(5, interpretation.getVersion());
        assertEquals(normalUserId2, interpretation.getAnalyst().getId());
        assertEquals(0, interpretation.getComments().size());

        result = catalogManager.getInterpretationManager().revert(studyFqn, ca.getId(), ca.getId() + ".1", 3, ownerToken);
        assertEquals(1, result.getNumUpdated());

        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(), ownerToken).first();
        assertEquals(6, interpretation.getVersion());
        assertEquals(normalUserId2, interpretation.getAnalyst().getId());
        assertEquals(1, interpretation.getComments().size());
        assertEquals(1, interpretation.getComments().get(0).getTags().size());
        assertEquals("tag1", interpretation.getComments().get(0).getTags().get(0));

        result = catalogManager.getInterpretationManager().revert(studyFqn, ca.getId(), ca.getId() + ".1", 4, ownerToken);
        assertEquals(1, result.getNumUpdated());

        interpretation =
                catalogManager.getInterpretationManager().get(studyFqn, ca.getId() + ".1", QueryOptions.empty(), ownerToken).first();
        assertEquals(7, interpretation.getVersion());
        assertEquals(normalUserId2, interpretation.getAnalyst().getId());
        assertEquals(2, interpretation.getComments().size());
        assertEquals(1, interpretation.getComments().get(0).getTags().size());
        assertEquals("tag1", interpretation.getComments().get(0).getTags().get(0));
        assertEquals(1, interpretation.getComments().get(1).getTags().size());
        assertEquals("tag2", interpretation.getComments().get(1).getTags().get(0));

        Query query = new Query(Constants.ALL_VERSIONS, true);
        result = catalogManager.getInterpretationManager().get(studyFqn, Collections.singletonList(ca.getId() + ".1"), query,
                QueryOptions.empty(), false, ownerToken);
        assertEquals(7, result.getNumResults());

        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(),
                ownerToken).first();
        assertEquals(8, clinicalAnalysis.getAudit().size());
        assertEquals(7, clinicalAnalysis.getInterpretation().getVersion());
    }

    @Test
    public void updateInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(5, ca.getAudit().size());
        assertEquals(ClinicalAudit.Action.CREATE_INTERPRETATION, ca.getAudit().get(4).getAction());
        assertEquals(ca.getId() + ".1", ca.getInterpretation().getId());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(2).getId());

        InterpretationUpdateParams params = new InterpretationUpdateParams().setAnalyst(new ClinicalAnalystParam(normalUserId2));
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1",
                params, null, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(6, ca.getAudit().size());
        assertEquals(ClinicalAudit.Action.UPDATE_INTERPRETATION, ca.getAudit().get(5).getAction());
        assertNotNull(ca.getInterpretation().getAnalyst());
        assertEquals(normalUserId2, ca.getInterpretation().getAnalyst().getId());
        assertEquals(2, ca.getInterpretation().getVersion());

        // Update a secondary interpretation
        params = new InterpretationUpdateParams()
                .setDescription("my description");
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".3", params, null, QueryOptions.empty(),
                ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(7, ca.getAudit().size());
        assertEquals(ClinicalAudit.Action.UPDATE_INTERPRETATION, ca.getAudit().get(6).getAction());
        assertEquals("my description", ca.getSecondaryInterpretations().get(1).getDescription());
        assertEquals(2, ca.getSecondaryInterpretations().get(1).getVersion());

        // Scalate secondary interpretation to primary and delete
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".3", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        catalogManager.getInterpretationManager().delete(studyFqn, ca.getId(), Collections.singletonList(ca.getId() + ".1"), ownerToken);

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
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
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".4", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
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
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".3", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".3", ca.getInterpretation().getId());
        assertEquals(2, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(1, ca.getSecondaryInterpretations().get(1).getVersion());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".3", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(2).getId());
        assertEquals(2, ca.getSecondaryInterpretations().get(2).getVersion());

        // Scalate to primary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(2).getId());
        assertEquals(1, ca.getSecondaryInterpretations().get(2).getVersion());

        // Scalate to primary and keep
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(2).getId());
        assertEquals(1, ca.getSecondaryInterpretations().get(2).getVersion());

        // Scalate to primary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(1, ca.getInterpretation().getVersion());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
    }

    @Test
    public void updatePrimarySecondaryLockedInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(),
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation().setLocked(true),
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation().setLocked(true),
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation().setLocked(true),
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertFalse(ca.getInterpretation().isLocked());
        assertEquals(5, ca.getAudit().size());
        assertEquals(ClinicalAudit.Action.CREATE_INTERPRETATION, ca.getAudit().get(4).getAction());
        assertEquals(ca.getId() + ".1", ca.getInterpretation().getId());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(2).getId());
        assertTrue(ca.getSecondaryInterpretations().get(0).isLocked());
        assertTrue(ca.getSecondaryInterpretations().get(1).isLocked());
        assertTrue(ca.getSecondaryInterpretations().get(2).isLocked());

        // Scalate secondary interpretation to primary and delete
        InterpretationUpdateParams params = new InterpretationUpdateParams();
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".3",
                params, ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        catalogManager.getInterpretationManager().delete(studyFqn, ca.getId(), Collections.singletonList(ca.getId() + ".1"), ownerToken);

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(8, ca.getAudit().size());
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".3", ca.getInterpretation().getId());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());

        // Scalate secondary interpretation to primary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".4", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".4", ca.getInterpretation().getId());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());

        // Scalate secondary interpretation to primary
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".3", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".3", ca.getInterpretation().getId());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".3", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(2).getId());

        // Scalate to primary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(2).getId());

        // Scalate to primary and keep
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());

        // Move primary to secondary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNull(ca.getInterpretation());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(2).getId());

        // Scalate to primary
        params = new InterpretationUpdateParams();
        result = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".2", params,
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumUpdated());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".2", ca.getInterpretation().getId());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
    }

    @Test
    public void deleteInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        // We update interpretation 1 so a new version is generated
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1", new InterpretationUpdateParams()
                .setDescription("my description"), null, QueryOptions.empty(), ownerToken);

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNotNull(ca.getInterpretation());
        assertEquals(ca.getId() + ".1", ca.getInterpretation().getId());
        assertEquals(3, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".3", ca.getSecondaryInterpretations().get(1).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(2).getId());

        OpenCGAResult delete = catalogManager.getInterpretationManager().delete(studyFqn, ca.getId(),
                Arrays.asList(ca.getId() + ".1", ca.getId() + ".3"), ownerToken);
        assertEquals(2, delete.getNumDeleted());

        Query query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList(ca.getId() + ".1", ca.getId() + ".3"))
                .append(Constants.ALL_VERSIONS, true);
        OpenCGAResult<Interpretation> search = catalogManager.getInterpretationManager().search(studyFqn, query, QueryOptions.empty(),
                ownerToken);
        assertEquals(0, search.getNumResults());

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNull(ca.getInterpretation());
        assertEquals(2, ca.getSecondaryInterpretations().size());
        assertEquals(ca.getId() + ".2", ca.getSecondaryInterpretations().get(0).getId());
        assertEquals(ca.getId() + ".4", ca.getSecondaryInterpretations().get(1).getId());

        query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList(ca.getId() + ".1", ca.getId() + ".3"));
        OpenCGAResult<Interpretation> result = catalogManager.getInterpretationManager().search(studyFqn, query, QueryOptions.empty(),
                ownerToken);
        assertEquals(0, result.getNumResults());

        query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList(ca.getId() + ".1", ca.getId() + ".3"))
                .append(InterpretationDBAdaptor.QueryParams.DELETED.key(), true);
        result = catalogManager.getInterpretationManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(3, result.getNumResults());
    }

    @Test
    public void searchClinicalAnalysisByProband() throws CatalogException {
        createDummyEnvironment(true, false);
        createDummyEnvironment(false, false);

        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(studyFqn,
                new Query(ParamConstants.CLINICAL_PROBAND_PARAM, "^chil"),
                new QueryOptions(QueryOptions.INCLUDE, ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_ID.key()), ownerToken);
        assertEquals(2, search.getNumResults());
        assertTrue(StringUtils.isNotEmpty(search.first().getProband().getId()));
        assertTrue(StringUtils.isEmpty(search.first().getProband().getName()));
    }

    @Test
    public void searchClinicalAnalysisByStatus() throws CatalogException {
        createDummyEnvironment(true, false);
        createDummyEnvironment(false, false);

        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(studyFqn,
                new Query(ParamConstants.STATUS_PARAM, "DONE"),
                new QueryOptions(QueryOptions.INCLUDE, ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_ID.key()), ownerToken);
        assertEquals(0, search.getNumResults());

        search = catalogManager.getClinicalAnalysisManager().search(studyFqn,
                new Query(ParamConstants.STATUS_PARAM, "READY_FOR_INTERPRETATION"),
                new QueryOptions(), ownerToken);
        assertEquals(2, search.getNumResults());
        for (ClinicalAnalysis result : search.getResults()) {
            assertEquals("READY_FOR_INTERPRETATION", result.getStatus().getId());
        }

        catalogManager.getClinicalAnalysisManager().update(studyFqn, search.first().getId(),
                new ClinicalAnalysisUpdateParams().setStatus(new StatusParam("REJECTED")), QueryOptions.empty(),
                ownerToken);
        search = catalogManager.getClinicalAnalysisManager().search(studyFqn,
                new Query(ParamConstants.STATUS_PARAM, "READY_FOR_INTERPRETATION"),
                new QueryOptions(), ownerToken);
        assertEquals(1, search.getNumResults());
        for (ClinicalAnalysis result : search.getResults()) {
            assertEquals("READY_FOR_INTERPRETATION", result.getStatus().getId());
        }

        search = catalogManager.getClinicalAnalysisManager().search(studyFqn,
                new Query(ParamConstants.STATUS_PARAM, "REJECTED"),
                new QueryOptions(), ownerToken);
        assertEquals(1, search.getNumResults());
        for (ClinicalAnalysis result : search.getResults()) {
            assertEquals("REJECTED", result.getStatus().getId());
        }
    }

    @Test
    public void deleteClinicalAnalysisTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);
        OpenCGAResult delete = catalogManager.getClinicalAnalysisManager().delete(studyFqn,
                Collections.singletonList(dummyEnvironment.first().getId()), null, ownerToken);
        assertEquals(1, delete.getNumDeleted());

        OpenCGAResult<ClinicalAnalysis> clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn,
                Collections.singletonList(dummyEnvironment.first().getId()),
                new Query(ClinicalAnalysisDBAdaptor.QueryParams.DELETED.key(), true), new QueryOptions(), false, ownerToken);
        assertEquals(1, clinicalResult.getNumResults());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getClinicalAnalysisManager().get(studyFqn, dummyEnvironment.first().getId(), new QueryOptions(), ownerToken);
    }

    @Test
    public void deleteClinicalAnalysisWithEmptyInterpretations() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getClinicalAnalysisManager().delete(studyFqn, Collections.singletonList(ca.getId()), null, ownerToken);

        assertEquals(0,
                catalogManager.getClinicalAnalysisManager().search(studyFqn, new Query(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(),
                        ca.getId()), QueryOptions.empty(), ownerToken).getNumResults());
        assertEquals(0,
                catalogManager.getInterpretationManager().search(studyFqn, new Query(InterpretationDBAdaptor.QueryParams.ID.key(),
                        ca.getId() + ".1"), QueryOptions.empty(), ownerToken).getNumResults());
        assertEquals(0,
                catalogManager.getInterpretationManager().search(studyFqn, new Query(InterpretationDBAdaptor.QueryParams.ID.key(),
                        ca.getId() + ".2"), QueryOptions.empty(), ownerToken).getNumResults());
        assertEquals(0,
                catalogManager.getInterpretationManager().search(studyFqn, new Query(InterpretationDBAdaptor.QueryParams.ID.key(),
                        ca.getId() + ".3"), QueryOptions.empty(), ownerToken).getNumResults());
        assertEquals(0,
                catalogManager.getInterpretationManager().search(studyFqn, new Query(InterpretationDBAdaptor.QueryParams.ID.key(),
                        ca.getId() + ".4"), QueryOptions.empty(), ownerToken).getNumResults());
    }

    @Test
    public void deleteClinicalAnalysisWithInterpretations() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        // Add finding to interpretation
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getId() + ".1",
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
                )), null, QueryOptions.empty(), ownerToken);

        try {
            catalogManager.getClinicalAnalysisManager().delete(studyFqn, Collections.singletonList(ca.getId()), null, ownerToken);
            fail("It should not allow deleting Clinical Analyses with interpretations containing primary findings");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("findings"));
        }

        OpenCGAResult delete = catalogManager.getClinicalAnalysisManager().delete(studyFqn, Collections.singletonList(ca.getId()),
                new QueryOptions(Constants.FORCE, true), ownerToken);
        assertEquals(1, delete.getNumDeleted());

        OpenCGAResult<ClinicalAnalysis> clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn,
                Collections.singletonList(ca.getId()),
                new Query(ClinicalAnalysisDBAdaptor.QueryParams.DELETED.key(), true), new QueryOptions(), false, ownerToken);
        assertEquals(1, clinicalResult.getNumResults());

        assertEquals(0, catalogManager.getInterpretationManager().search(studyFqn,
                new Query(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList(ca.getId() + ".1", ca.getId() + ".2",
                        ca.getId() + ".3", ca.getId() + ".4")), QueryOptions.empty(), ownerToken).getNumResults());

        // Old interpretations were deleted
        assertEquals(5, catalogManager.getInterpretationManager().search(studyFqn, new Query()
                        .append(InterpretationDBAdaptor.QueryParams.ID.key(), Arrays.asList(ca.getId() + ".1", ca.getId() + ".2",
                                ca.getId() + ".3", ca.getId() + ".4"))
                        .append(InterpretationDBAdaptor.QueryParams.DELETED.key(), true), QueryOptions.empty(), ownerToken)
                .getNumResults());
    }

    @Test
    public void deleteLockedClinicalAnalysis() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getClinicalAnalysisManager().update(studyFqn, ca.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), ownerToken);

        try {
            catalogManager.getClinicalAnalysisManager().delete(studyFqn, Collections.singletonList(ca.getId()), null, ownerToken);
            fail("It should not allow deleting locked Clinical Analyses");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

        OpenCGAResult delete = catalogManager.getClinicalAnalysisManager().delete(studyFqn, Collections.singletonList(ca.getId()),
                new QueryOptions(Constants.FORCE, true), ownerToken);
        assertEquals(1, delete.getNumDeleted());

        OpenCGAResult<ClinicalAnalysis> clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn,
                Collections.singletonList(ca.getId()), new Query(ClinicalAnalysisDBAdaptor.QueryParams.DELETED.key(), true),
                new QueryOptions(), false, ownerToken);
        assertEquals(1, clinicalResult.getNumResults());
    }

    @Test
    public void searchByDisorderTest() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setDisorder(new DisorderReferenceParam("dis1"));
        catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(), updateParams, QueryOptions.empty(),
                ownerToken);

        createDummyEnvironment(false, false);

        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().search(studyFqn,
                new Query(ParamConstants.CLINICAL_DISORDER_PARAM, "dis1"), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(dummyEnvironment.first().getId(), result.first().getId());

        result = catalogManager.getClinicalAnalysisManager().search(studyFqn, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void updateDisorder() throws CatalogException {
        DataResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true, false);

        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setDisorder(new DisorderReferenceParam("dis1"));

        catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(), updateParams, QueryOptions.empty(),
                ownerToken);
        OpenCGAResult<ClinicalAnalysis> result1 = catalogManager.getClinicalAnalysisManager().get(studyFqn, dummyEnvironment.first().getId(),
                new QueryOptions(), ownerToken);

        assertEquals("dis1", result1.first().getDisorder().getId());
        assertEquals("OT", result1.first().getDisorder().getSource());

        updateParams = new ClinicalAnalysisUpdateParams()
                .setDisorder(new DisorderReferenceParam("non_existing"));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("proband disorders");
        catalogManager.getClinicalAnalysisManager().update(studyFqn, dummyEnvironment.first().getId(), updateParams, QueryOptions.empty(),
                ownerToken);
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
        DataResult<ClinicalAnalysis> clinicalAnalysisDataResult = catalogManager.getClinicalAnalysisManager().create(studyFqn,
                clinicalAnalysis, INCLUDE_RESULT, ownerToken);

        assertTrue(clinicalAnalysisDataResult.first().getFamily().getMembers().stream().map(Individual::getId).collect(Collectors.toList())
                .containsAll(dummyFamily.first().getMembers().stream().map(Individual::getId).collect(Collectors.toList())));
        assertEquals(5, dummyFamily.first().getMembers().size());
        assertEquals(5, clinicalAnalysisDataResult.first().getFamily().getMembers().size());
    }

    @Test
    public void createClinicalAnalysisWithPanels() throws CatalogException {
        PanelImportParams params1 = new PanelImportParams()
                .setSource(PanelImportParams.Source.PANEL_APP)
                .setPanelIds(Collections.singletonList("VACTERL-like phenotypes"));
        catalogManager.getPanelManager().importFromSource(studyFqn, params1, null, ownerToken).first();
        Panel panel = catalogManager.getPanelManager().search(studyFqn, new Query(), QueryOptions.empty(), ownerToken).first();
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

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT,
                ownerToken).first();
        assertEquals(1, ca.getPanels().size());
        assertEquals(panel.getId(), ca.getPanels().get(0).getId());
        assertEquals(panel.getName(), ca.getPanels().get(0).getName());
        assertEquals(panel.getVersion(), ca.getPanels().get(0).getVersion());
        assertEquals(panel.getGenes().size(), ca.getPanels().get(0).getGenes().size());
    }

    @Test
    public void createInterpretationWithPanels() throws CatalogException {
        PanelImportParams params1 = new PanelImportParams()
                .setSource(PanelImportParams.Source.PANEL_APP)
                .setPanelIds(Collections.singletonList("VACTERL-like phenotypes"));
        catalogManager.getPanelManager().importFromSource(studyFqn, params1, null, ownerToken).first();
        Panel panel = catalogManager.getPanelManager().search(studyFqn, new Query(), QueryOptions.empty(), ownerToken).first();

        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        Interpretation interpretation = new Interpretation()
                .setPanels(Collections.singletonList(new Panel().setId(panel.getId())));

        interpretation = catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), interpretation,
                ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, ownerToken).first();
        interpretation = catalogManager.getInterpretationManager().get(studyFqn, interpretation.getId(), QueryOptions.empty(), ownerToken)
                .first();

        assertEquals(1, interpretation.getPanels().size());
        assertEquals(panel.getId(), interpretation.getPanels().get(0).getId());
    }

    @Test
    public void updatePanelsInClinicalAnalysis() throws CatalogException {
        PanelImportParams params1 = new PanelImportParams()
                .setSource(PanelImportParams.Source.PANEL_APP)
                .setPanelIds(Collections.singletonList("VACTERL-like phenotypes"));
        catalogManager.getPanelManager().importFromSource(studyFqn, params1, null, ownerToken).first();
        Panel panel = catalogManager.getPanelManager().search(studyFqn, new Query(), QueryOptions.empty(), ownerToken).first();

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
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken).first();
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setPanels(Collections.singletonList(new PanelReferenceParam(panel.getId()))),
                QueryOptions.empty(), ownerToken);

        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(),
                ownerToken).first();

        assertEquals(1, ca.getPanels().size());
        assertEquals(panel.getId(), ca.getPanels().get(0).getId());
        assertEquals(panel.getName(), ca.getPanels().get(0).getName());
        assertEquals(panel.getVersion(), ca.getPanels().get(0).getVersion());
        assertEquals(panel.getGenes().size(), ca.getPanels().get(0).getGenes().size());
    }

    @Test
    public void testQueriesInFamilyCase() throws CatalogException {
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
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);

        catalogManager.getFamilyManager().update(studyFqn, dummyFamily.first().getId(), new FamilyUpdateParams()
                .setId("familyId"), QueryOptions.empty(), ownerToken);

        QueryOptions includeClinicalIds = ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS;
        // Query by members
        Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.INDIVIDUAL.key(), "child3");
        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, includeClinicalIds,
                ownerToken);
        assertEquals(1, search.getNumResults());

        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.INDIVIDUAL.key(), "child1");
        search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, includeClinicalIds, ownerToken);
        assertEquals(1, search.getNumResults());

        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.INDIVIDUAL.key(), "child4");
        search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, includeClinicalIds, ownerToken);
        assertEquals(0, search.getNumResults());

        // Query by samples
        query = new Query(ParamConstants.CLINICAL_SAMPLE_PARAM, "sample2");
        search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, includeClinicalIds, ownerToken);
        assertEquals(1, search.getNumResults());

        query = new Query(ParamConstants.CLINICAL_SAMPLE_PARAM, "sample5");
        search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, includeClinicalIds, ownerToken);
        assertEquals(1, search.getNumResults());

        query = new Query(ParamConstants.CLINICAL_SAMPLE_PARAM, "sample4");
        search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, includeClinicalIds, ownerToken);
        assertEquals(0, search.getNumResults());

        query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), "familyId");
        search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, includeClinicalIds, ownerToken);
        assertEquals(1, search.getNumResults());
    }

    @Test
    public void testQueriesInCancerCase() throws CatalogException {
        Sample sample = DummyModelUtils.getDummySample("sample");
        sample.setSomatic(true);
        Individual individual = DummyModelUtils.getDummyIndividual("individual", SexOntologyTermAnnotation.initMale(),
                Collections.singletonList(sample), null, null);
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.CANCER)
                .setDueDate("20180510100000")
                .setProband(new Individual().setId(individual.getId()));
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);

        // Update to force a version increment and therefore, an update over the case
        catalogManager.getSampleManager().update(studyFqn, sample.getId(), new SampleUpdateParams().setDescription("descr"),
                QueryOptions.empty(), ownerToken);

        QueryOptions includeClinicalIds = ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS;
        // Query by members
        Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.INDIVIDUAL.key(), "individual");
        OpenCGAResult<ClinicalAnalysis> search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, includeClinicalIds,
                ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals(clinicalAnalysis.getId(), search.first().getId());

        query = new Query(ParamConstants.CLINICAL_SAMPLE_PARAM, "sample");
        search = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, includeClinicalIds, ownerToken);
        assertEquals(1, search.getNumResults());
        assertEquals(clinicalAnalysis.getId(), search.first().getId());
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
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);
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
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);
    }

    @Test
    public void createClinicalAnalysisWithoutFamily() throws CatalogException {
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setProband(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("missing");
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);
    }

    @Test
    public void createClinicalAnalysisWithoutProband() throws CatalogException {
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000");

        thrown.expect(CatalogException.class);
        thrown.expectMessage("missing");
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);
    }

    @Test
    public void createClinicalAnalysisWithoutType() throws CatalogException {
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description")
                .setDueDate("20180510100000");

        thrown.expect(CatalogException.class);
        thrown.expectMessage("missing");
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), ownerToken);
    }

    private List<Panel> createPanels(int nPanels) throws CatalogException {
        List<Panel> panelList = new ArrayList<>(nPanels);
        for (int i = 0; i < nPanels; i++) {
            panelList.add(catalogManager.getPanelManager().create(studyFqn, new Panel().setId("panel" + i), INCLUDE_RESULT,
                    ownerToken).first());
        }
        return panelList;
    }

    @Test
    public void createClinicalAnalysisWithPanelsTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanels(panels);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken);
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
        assertFalse(result.first().isPanelLocked());
    }

    @Test
    public void fetchInterpretationWithFullPanelInformationTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanels(panels);
        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken);

        Interpretation interpretation = catalogManager.getInterpretationManager().search(studyFqn,
                new Query(InterpretationDBAdaptor.QueryParams.CLINICAL_ANALYSIS_ID.key(), clinicalAnalysis.getId()), QueryOptions.empty(),
                ownerToken).first();
        assertEquals(2, interpretation.getPanels().size());
        for (Panel panel : interpretation.getPanels()) {
            assertNotNull(panel.getId());
            assertNotNull(panel.getName());
        }
    }

    @Test
    public void updatePanelsActionTest() throws CatalogException {
        List<Panel> panels = createPanels(5);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setPanels(panels.subList(0, 2))
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                options, ownerToken);
        result = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(3, result.first().getPanels().size());
        assertTrue(panels.subList(0, 3).stream().map(Panel::getId).collect(Collectors.toList()).containsAll(
                result.first().getPanels().stream().map(Panel::getId).collect(Collectors.toList())));

        actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.REMOVE);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(Arrays.asList(
                                new PanelReferenceParam(panels.get(0).getId()),
                                new PanelReferenceParam(panels.get(2).getId()))
                        ),
                options, ownerToken);
        result = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(1, result.first().getPanels().size());
        assertEquals(panels.get(1).getId(), result.first().getPanels().get(0).getId());

        actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(Arrays.asList(
                                new PanelReferenceParam(panels.get(3).getId()),
                                new PanelReferenceParam(panels.get(4).getId()))
                        ),
                options, ownerToken);
        result = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertTrue(panels.subList(3, 5).stream().map(Panel::getId).collect(Collectors.toList()).containsAll(
                result.first().getPanels().stream().map(Panel::getId).collect(Collectors.toList())));

    }

    @Test
    public void updateInterpretationPanelsActionTest() throws CatalogException {
        List<Panel> panels = createPanels(5);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
//                .setPanels(panels.subList(0, 2))
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(0, result.first().getPanels().size());

        String intepretationId = result.first().getInterpretation().getId();
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        OpenCGAResult<Interpretation> interpretation = catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(),
                intepretationId, new InterpretationUpdateParams()
                        .setPanels(panels.subList(0, 2).stream().map((p) -> new PanelReferenceParam(p.getId())).collect(Collectors.toList())),
                null, options, ownerToken);
        assertEquals(2, interpretation.first().getPanels().size());

        actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.ADD);
        options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        interpretation = catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(),
                intepretationId, new InterpretationUpdateParams()
                        .setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                null, options, ownerToken);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(3, interpretation.first().getPanels().size());
        assertTrue(panels.subList(0, 3).stream().map(Panel::getId).collect(Collectors.toList()).containsAll(
                interpretation.first().getPanels().stream().map(Panel::getId).collect(Collectors.toList())));

        actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.REMOVE);
        options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        interpretation = catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(),
                intepretationId, new InterpretationUpdateParams()
                        .setPanels(Arrays.asList(
                                new PanelReferenceParam(panels.get(0).getId()),
                                new PanelReferenceParam(panels.get(2).getId()))
                        ),
                null, options, ownerToken);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(1, interpretation.first().getPanels().size());
        assertEquals(panels.get(1).getId(), interpretation.first().getPanels().get(0).getId());

        actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions()
                .append(Constants.ACTIONS, actionMap)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        interpretation = catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(),
                intepretationId, new InterpretationUpdateParams()
                        .setPanels(Arrays.asList(
                                new PanelReferenceParam(panels.get(3).getId()),
                                new PanelReferenceParam(panels.get(4).getId()))
                        ),
                null, options, ownerToken);
        assertEquals(1, interpretation.getNumResults());
        assertEquals(2, interpretation.first().getPanels().size());
        assertTrue(panels.subList(3, 5).stream().map(Panel::getId).collect(Collectors.toList()).containsAll(
                interpretation.first().getPanels().stream().map(Panel::getId).collect(Collectors.toList())));

    }

    @Test
    public void updatePanelsAndPanelLockFromClinicalAnalysisTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, true, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(0, result.first().getPanels().size());
        assertFalse(result.first().isPanelLocked());

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions updateOptions = new QueryOptions(Constants.ACTIONS, actionMap);

        try {
            catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                            .setPanels(panels.stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList()))
                            .setPanelLocked(true),
                    updateOptions, ownerToken);
            fail("Updating panels and setting panellock to true in one call should not be accepted");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("not allowed"));
        }

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(panels.stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList())),
                updateOptions, ownerToken);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanelLocked(true),
                updateOptions, ownerToken);
        result = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertTrue(result.first().isPanelLocked());

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(0).getId())))
                        .setPanelLocked(false),
                updateOptions, ownerToken);
        result = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(1, result.first().getPanels().size());
        assertFalse(result.first().isPanelLocked());

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanelLocked(true),
                updateOptions, ownerToken);
        thrown.expect(CatalogException.class);
        thrown.expectMessage("panelLock");
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(panels.stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList())),
                updateOptions, ownerToken);
    }

    @Test
    public void updatePanelsAndPanelLockFromClinicalAnalysisWithInterpretationTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, null, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(0, result.first().getPanels().size());
        assertFalse(result.first().isPanelLocked());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not allowed");
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(panels.stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList()))
                        .setPanelLocked(true),
                QueryOptions.empty(), ownerToken);
    }

    @Test
    public void setPanelLockWithInterpretationWithNoPanelsTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, null, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(0, result.first().getPanels().size());
        assertFalse(result.first().isPanelLocked());

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(panels.stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList())),
                QueryOptions.empty(), ownerToken);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(2, clinicalAnalysis.getPanels().size());
        assertEquals(0, clinicalAnalysis.getInterpretation().getPanels().size());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("any of the case panels");
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanelLocked(true),
                QueryOptions.empty(), ownerToken);
    }

    @Test
    public void setPanelLockWithInterpretationWithPanelSubsetTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, null, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(0, result.first().getPanels().size());
        assertFalse(result.first().isPanelLocked());

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(panels.stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList())),
                QueryOptions.empty(), ownerToken);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(2, clinicalAnalysis.getPanels().size());
        assertEquals(0, clinicalAnalysis.getInterpretation().getPanels().size());

        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getInterpretation().getId(),
                new InterpretationUpdateParams()
                        .setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(0).getId()))), null,
                QueryOptions.empty(), ownerToken);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertFalse(clinicalAnalysis.isPanelLocked());
        assertEquals(2, clinicalAnalysis.getPanels().size());
        assertEquals(1, clinicalAnalysis.getInterpretation().getPanels().size());

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanelLocked(true),
                QueryOptions.empty(), ownerToken);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertTrue(clinicalAnalysis.isPanelLocked());
        assertEquals(2, clinicalAnalysis.getPanels().size());
        assertEquals(1, clinicalAnalysis.getInterpretation().getPanels().size());
    }

    @Test
    public void setPanelLockWithInterpretationWithDifferentPanelsTest() throws CatalogException {
        List<Panel> panels = createPanels(2);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband);

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, null, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(0, result.first().getPanels().size());
        assertFalse(result.first().isPanelLocked());

        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(0).getId()))),
                QueryOptions.empty(), ownerToken);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(1, clinicalAnalysis.getPanels().size());
        assertEquals(0, clinicalAnalysis.getInterpretation().getPanels().size());

        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), clinicalAnalysis.getInterpretation().getId(),
                new InterpretationUpdateParams()
                        .setPanels(panels.stream().map(p -> new PanelReferenceParam(p.getId())).collect(Collectors.toList())), null,
                QueryOptions.empty(), ownerToken);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(1, clinicalAnalysis.getPanels().size());
        assertEquals(2, clinicalAnalysis.getInterpretation().getPanels().size());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not defined by the case");
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanelLocked(true),
                QueryOptions.empty(), ownerToken);
    }

    @Test
    public void updatePanelsFromClinicalAnalysisWithPanelLockTest() throws CatalogException {
        List<Panel> panels = createPanels(3);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanels(panels.subList(0, 2));

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertFalse(result.first().isPanelLocked());

        Interpretation interpretation = catalogManager.getInterpretationManager().create(studyFqn, clinicalAnalysis.getId(),
                new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, ownerToken).first();
        assertEquals(2, interpretation.getPanels().size());

        // Set panelLock to true
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setPanelLocked(true), QueryOptions.empty(), ownerToken);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertTrue(clinicalAnalysis.isPanelLocked());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("panelLock");
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                        .setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                QueryOptions.empty(), ownerToken);
    }

    @Test
    public void updatePanelLockWithDifferentPanels() throws CatalogException {
        List<Panel> panels = createPanels(3);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanels(panels.subList(0, 2));

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertFalse(result.first().isPanelLocked());

        Interpretation interpretation = catalogManager.getInterpretationManager().create(studyFqn, clinicalAnalysis.getId(),
                new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, ownerToken).first();
        assertEquals(2, interpretation.getPanels().size());

        // Set panelLock to true
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setPanelLocked(true), QueryOptions.empty(), ownerToken);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertTrue(clinicalAnalysis.isPanelLocked());

        // Set panelLock to false
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setPanelLocked(false), QueryOptions.empty(), ownerToken);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertFalse(clinicalAnalysis.isPanelLocked());

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                null, options, ownerToken);
        interpretation = catalogManager.getInterpretationManager().get(studyFqn, interpretation.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(1, interpretation.getPanels().size());
        assertEquals(panels.get(2).getId(), interpretation.getPanels().get(0).getId());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("panels");
        // Set panelLock to true
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(), new ClinicalAnalysisUpdateParams()
                .setPanelLocked(true), QueryOptions.empty(), ownerToken);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertTrue(clinicalAnalysis.isPanelLocked());
    }

    @Test
    public void updatePanelsFromInterpretationWithLockedCATest() throws CatalogException {
        List<Panel> panels = createPanels(3);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanelLocked(true)
                .setPanels(panels.subList(0, 2));

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertTrue(result.first().isPanelLocked());

        Interpretation interpretation = catalogManager.getInterpretationManager().create(studyFqn, clinicalAnalysis.getId(),
                new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, ownerToken).first();
        assertEquals(2, interpretation.getPanels().size());

        thrown.expect(CatalogException.class);
        thrown.expectMessage("panelLock");
        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                null, QueryOptions.empty(), ownerToken);
    }

    @Test
    public void updatePanelsFromInterpretationWithUnlockedCATest() throws CatalogException {
        List<Panel> panels = createPanels(3);
        Individual proband = catalogManager.getIndividualManager().create(studyFqn,
                new Individual()
                        .setId("proband")
                        .setSamples(Collections.singletonList(new Sample().setId("sample"))),
                INCLUDE_RESULT, ownerToken).first();

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(proband)
                .setPanels(panels.subList(0, 2));

        OpenCGAResult<ClinicalAnalysis> result =
                catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, INCLUDE_RESULT, ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getPanels().size());
        assertFalse(result.first().isPanelLocked());

        Interpretation interpretation = catalogManager.getInterpretationManager().create(studyFqn, clinicalAnalysis.getId(),
                new Interpretation(), ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, ownerToken).first();
        assertEquals(2, interpretation.getPanels().size());

        // Ensure this fails
        catalogManager.getInterpretationManager().update(studyFqn, clinicalAnalysis.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setPanels(Collections.singletonList(new PanelReferenceParam(panels.get(2).getId()))),
                null, QueryOptions.empty(), ownerToken);
        interpretation = catalogManager.getInterpretationManager().get(studyFqn, interpretation.getId(), QueryOptions.empty(),
                ownerToken).first();

        assertEquals(3, interpretation.getPanels().size());
    }

    @Test
    public void createClinicalAnalysisWithFiles() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        // Register and associate files to sample "sample"
        List<File> files = registerDummyFiles();
        for (File file : files) {
            catalogManager.getFileManager().update(studyFqn, file.getPath(),
                    new FileUpdateParams().setSampleIds(Collections.singletonList("sample")), QueryOptions.empty(), ownerToken);
        }

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual);
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis,
                INCLUDE_RESULT, ownerToken);
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
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        // Register and associate files to sample "sample"
        List<File> files = registerDummyFiles();
        for (File file : files) {
            catalogManager.getFileManager().update(studyFqn, file.getPath(),
                    new FileUpdateParams().setSampleIds(Collections.singletonList("sample")), QueryOptions.empty(), ownerToken);
        }
        List<FileReferenceParam> fileRefs = files.stream().map(f -> new FileReferenceParam(f.getPath())).collect(Collectors.toList());

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual);
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis,
                INCLUDE_RESULT, ownerToken);
        assertEquals(1, clinical.getNumResults());
        assertEquals(4, clinical.first().getFiles().size());

        // Remove first and last file
        ObjectMap actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setFiles(Arrays.asList(fileRefs.get(0), fileRefs.get(3))), options, ownerToken);
        ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(),
                ownerToken).first();
        assertEquals(2, ca.getFiles().size());
        assertTrue(files.subList(1, 3).stream().map(File::getPath).collect(Collectors.toSet())
                .containsAll(ca.getFiles().stream().map(File::getPath).collect(Collectors.toSet())));

        // Add first file again
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), ParamUtils.BasicUpdateAction.ADD);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setFiles(Collections.singletonList(fileRefs.get(0))), options, ownerToken);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(3, ca.getFiles().size());
        assertTrue(files.subList(0, 3).stream().map(File::getPath).collect(Collectors.toSet())
                .containsAll(ca.getFiles().stream().map(File::getPath).collect(Collectors.toSet())));

        // Set file 3 and 4
        actionMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), ParamUtils.BasicUpdateAction.SET);
        options = new QueryOptions(Constants.ACTIONS, actionMap);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setFiles(Arrays.asList(fileRefs.get(2), fileRefs.get(3))), options, ownerToken);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, clinicalAnalysis.getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals(2, ca.getFiles().size());
        assertTrue(files.subList(2, 4).stream().map(File::getPath).collect(Collectors.toSet())
                .containsAll(ca.getFiles().stream().map(File::getPath).collect(Collectors.toSet())));
    }

    @Test
    public void fetchCasesWithSameProbandAndDifferentSample() throws CatalogException, IOException {
        Sample sample1 = DummyModelUtils.getDummySample("sample1");
        Sample sample2 = DummyModelUtils.getDummySample("sample2");

        Individual proband = DummyModelUtils.getDummyIndividual("proband", SexOntologyTermAnnotation.initMale(),
                Arrays.asList(sample1, sample2), null, null);
        catalogManager.getIndividualManager().create(studyFqn, proband, QueryOptions.empty(), ownerToken);

        Individual probandCopy = JacksonUtils.copy(proband, Individual.class);
        probandCopy.setSamples(Collections.singletonList(proband.getSamples().get(0)));
        ClinicalAnalysis case1 = DummyModelUtils.getDummyClinicalAnalysis("case1", probandCopy, null, null);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case1, QueryOptions.empty(), ownerToken);

        probandCopy.setSamples(Collections.singletonList(proband.getSamples().get(1)));
        ClinicalAnalysis case2 = DummyModelUtils.getDummyClinicalAnalysis("case2", probandCopy, null, null);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case2, QueryOptions.empty(), ownerToken);

        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().search(studyFqn, new Query(),
                QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
        assertEquals(case1.getId(), result.getResults().get(0).getId());
        assertEquals(proband.getId(), result.getResults().get(0).getProband().getId());
        assertEquals(1, result.getResults().get(0).getProband().getSamples().size());
        assertEquals(proband.getSamples().get(0).getId(), result.getResults().get(0).getProband().getSamples().get(0).getId());
        assertEquals(case2.getId(), result.getResults().get(1).getId());
        assertEquals(proband.getId(), result.getResults().get(1).getProband().getId());
        assertEquals(1, result.getResults().get(1).getProband().getSamples().size());
        assertEquals(proband.getSamples().get(1).getId(), result.getResults().get(1).getProband().getSamples().get(0).getId());
    }

    @Test
    public void loadClinicalAnalysesTest() throws CatalogException, IOException {
        String fileStr = "clinical_analyses.json.gz";
        File file;
        try (InputStream stream = getClass().getResourceAsStream("/biofiles/" + fileStr)) {
            file = catalogManager.getFileManager().upload(studyFqn, stream, new File().setPath("biofiles/" + fileStr), false, true, false, ownerToken).first();
        }

        Path filePath = Paths.get(file.getUri());

        System.out.println("Loading clinical analyses file: " + filePath + " ....");
        ClinicalAnalysisLoadResult loadResult = catalogManager.getClinicalAnalysisManager().load(studyFqn, filePath, ownerToken);
        System.out.println(loadResult);

        assertEquals(1, loadResult.getFailures().size());

        String ca1Id = "SAP-45016-1";
        String ca2Id = "OPA-6607-1";

        Query query = new Query();
        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, QueryOptions.empty(),
                ownerToken);
        assertTrue(result.getResults().stream().map(ca -> ca.getId()).collect(Collectors.toList()).contains(ca1Id));
        assertTrue(result.getResults().stream().map(ca -> ca.getId()).collect(Collectors.toList()).contains(ca2Id));

        query.put("id", ca1Id);
        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, QueryOptions.empty(),
                ownerToken).first();
        assertEquals(ca1Id, clinicalAnalysis.getId());

        query.put("id", ca2Id);
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, QueryOptions.empty(),
                ownerToken).first();
        assertEquals(ca2Id, clinicalAnalysis.getId());
    }

    // Annotation sets
    @Test
    public void searchByInternalAnnotationSetTest() throws CatalogException {
        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable().setId("a").setType(Variable.VariableType.STRING));
        variables.add(new Variable().setId("b").setType(Variable.VariableType.MAP_INTEGER).setAllowedKeys(Arrays.asList("b1", "b2")));
        VariableSet variableSet = new VariableSet("myInternalVset", "", false, false, true, "", variables, null, 1, null);
        catalogManager.getStudyManager().createVariableSet(studyFqn, variableSet, ownerToken);

        Map<String, Object> annotations = new HashMap<>();
        annotations.put("a", "hello");
        annotations.put("b", new ObjectMap("b1", 2).append("b2", 3));
        AnnotationSet annotationSet = new AnnotationSet("annSet", variableSet.getId(), annotations);

        annotations = new HashMap<>();
        annotations.put("a", "bye");
        annotations.put("b", new ObjectMap("b1", Integer.MAX_VALUE + 1L).append("b2", 5));
        AnnotationSet annotationSet2 = new AnnotationSet("annSet2", variableSet.getId(), annotations);

        DataResult<ClinicalAnalysis> clinicalAnalysisDataResult = createDummyEnvironment(true, true);
        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysisDataResult.first().getId(),
                new ClinicalAnalysisUpdateParams().setAnnotationSets(Arrays.asList(annotationSet, annotationSet2)), INCLUDE_RESULT, ownerToken).first();
        assertEquals(0, clinicalAnalysis.getAnnotationSets().size());

        // Create a different case with different annotations
        annotations = new HashMap<>();
        annotations.put("a", "hi");
        annotations.put("b", new ObjectMap("b1", 12).append("b2", 13));
        annotationSet = new AnnotationSet("annSet", variableSet.getId(), annotations);

        annotations = new HashMap<>();
        annotations.put("a", "goodbye");
        annotations.put("b", new ObjectMap("b1", 14).append("b2", 15));
        annotationSet2 = new AnnotationSet("annSet2", variableSet.getId(), annotations);

        DataResult<ClinicalAnalysis> clinicalAnalysisDataResult2 = createDummyEnvironment(false, true);
        ClinicalAnalysis clinicalAnalysis2 = catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysisDataResult2.first().getId(),
                new ClinicalAnalysisUpdateParams().setAnnotationSets(Arrays.asList(annotationSet, annotationSet2)), INCLUDE_RESULT, ownerToken).first();
        assertEquals(0, clinicalAnalysis2.getAnnotationSets().size());

        // Query by one of the annotations
        Query query = new Query(Constants.ANNOTATION, "myInternalVset:a=hello");
        assertEquals(1, catalogManager.getClinicalAnalysisManager().count(studyFqn, query, ownerToken).getNumMatches());
        assertEquals(clinicalAnalysis.getId(), catalogManager.getClinicalAnalysisManager().search(studyFqn, query, ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, ownerToken).first()
                .getId());

        query = new Query(Constants.ANNOTATION, "myInternalVset:b.b1=" + (Integer.MAX_VALUE + 1L));
        assertEquals(1, catalogManager.getClinicalAnalysisManager().count(studyFqn, query, ownerToken).getNumMatches());
        assertEquals(clinicalAnalysis.getId(), catalogManager.getClinicalAnalysisManager().search(studyFqn, query, ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, ownerToken).first()
                .getId());

        query = new Query(Constants.ANNOTATION, "b.b1=14");
        assertEquals(1, catalogManager.getClinicalAnalysisManager().count(studyFqn, query, ownerToken).getNumMatches());
        assertEquals(clinicalAnalysis2.getId(), catalogManager.getClinicalAnalysisManager().search(studyFqn, query, ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, ownerToken).first()
                .getId());

        query = new Query(Constants.ANNOTATION, "a=goodbye");
        assertEquals(1, catalogManager.getClinicalAnalysisManager().count(studyFqn, query, ownerToken).getNumMatches());
        assertEquals(clinicalAnalysis2.getId(), catalogManager.getClinicalAnalysisManager().search(studyFqn, query, ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, ownerToken).first()
                .getId());

        // Update sample annotation to be exactly the same as sample2
        ObjectMap action = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.ANNOTATION_SETS.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, action);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setAnnotationSets(Arrays.asList(annotationSet, annotationSet2)), options, ownerToken);

        query = new Query(Constants.ANNOTATION, "myInternalVset:a=hello");
        assertEquals(0, catalogManager.getClinicalAnalysisManager().count(studyFqn, query, ownerToken).getNumMatches());

        query = new Query(Constants.ANNOTATION, "myInternalVset:b.b1=4");
        assertEquals(0, catalogManager.getClinicalAnalysisManager().count(studyFqn, query, ownerToken).getNumMatches());

        query = new Query(Constants.ANNOTATION, "b.b1=14");
        assertEquals(2, catalogManager.getClinicalAnalysisManager().count(studyFqn, query, ownerToken).getNumMatches());
        assertTrue(Arrays.asList(clinicalAnalysis.getId(), clinicalAnalysis2.getId())
                .containsAll(catalogManager.getClinicalAnalysisManager().search(studyFqn, query, ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, ownerToken)
                        .getResults().stream().map(ClinicalAnalysis::getId).collect(Collectors.toList())));

        query = new Query(Constants.ANNOTATION, "a=goodbye");
        assertEquals(2, catalogManager.getClinicalAnalysisManager().count(studyFqn, query, ownerToken).getNumMatches());
        assertTrue(Arrays.asList(clinicalAnalysis.getId(), clinicalAnalysis2.getId())
                .containsAll(catalogManager.getClinicalAnalysisManager().search(studyFqn, query, ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, ownerToken)
                        .getResults().stream().map(ClinicalAnalysis::getId).collect(Collectors.toList())));
    }

    @Test
    public void testSearchAnnotation() throws CatalogException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "",
                "", null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), null, 0, "",
                "", null, Collections.emptyMap()));
        variables.add(new Variable("OTHER", "", "", Variable.VariableType.OBJECT, null, false, false, null, null, 1, "", "", null,
                Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.CLINICAL_ANALYSIS), ownerToken).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);

        DataResult<ClinicalAnalysis> clinicalAnalysisDataResult = createDummyEnvironment(true, true);
        createDummyEnvironment(false, true);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysisDataResult.first().getId(),
                new ClinicalAnalysisUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet)), QueryOptions.empty(),
                ownerToken);

        Query query = new Query(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25");
        DataResult<ClinicalAnalysis> annotDataResult = catalogManager.getClinicalAnalysisManager().search(studyFqn, query,
                QueryOptions.empty(), ownerToken);
        assertEquals(1, annotDataResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=23");
        annotDataResult = catalogManager.getClinicalAnalysisManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(0, annotDataResult.getNumResults());
    }

    @Test
    public void updateClinicalAnalysisFilesWithActions() throws CatalogException {
        Individual individual = new Individual()
                .setId("proband")
                .setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), ownerToken);

        // Register and associate files to sample "sample"
        List<File> files = registerDummyFiles();
        for (File file : files) {
            catalogManager.getFileManager().update(studyFqn, file.getPath(),
                    new FileUpdateParams().setSampleIds(Collections.singletonList("sample")), QueryOptions.empty(), ownerToken);
        }

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("Clinical")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setProband(individual);
        OpenCGAResult<ClinicalAnalysis> clinical = catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis,
                INCLUDE_RESULT, ownerToken);
        assertEquals(1, clinical.getNumResults());
        assertEquals(4, clinical.first().getFiles().size());
        for (File file : clinical.first().getFiles()) {
            assertNotNull(file.getPath());
            assertNotNull(file.getName());
        }

        // Test SET action - replace all files
        List<FileReferenceParam> setFiles = Arrays.asList(
                new FileReferenceParam("HG00096.chrom20.small.bam"),
                new FileReferenceParam("NA19600.chrom20.small.bam")
        );

        QueryOptions setOptions = new QueryOptions()
                .append(Constants.ACTIONS, new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), ParamUtils.UpdateAction.SET))
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);

        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setFiles(setFiles), setOptions, ownerToken);

        assertEquals(2, result.first().getFiles().size());
        Set<String> fileIds = result.first().getFiles().stream().map(File::getId).collect(Collectors.toSet());
        assertTrue(fileIds.contains("HG00096.chrom20.small.bam"));
        assertTrue(fileIds.contains("NA19600.chrom20.small.bam"));

        // Test ADD action - add new files
        List<FileReferenceParam> addFiles = Arrays.asList(
                new FileReferenceParam("variant-test-file.vcf.gz")
        );

        QueryOptions addOptions = new QueryOptions()
                .append(Constants.ACTIONS, new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), ParamUtils.UpdateAction.ADD))
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);

        result = catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setFiles(addFiles), addOptions, ownerToken);

        assertEquals(3, result.first().getFiles().size());
        fileIds = result.first().getFiles().stream().map(File::getId).collect(Collectors.toSet());
        assertTrue(fileIds.contains("HG00096.chrom20.small.bam"));
        assertTrue(fileIds.contains("NA19600.chrom20.small.bam"));
        assertTrue(fileIds.contains("variant-test-file.vcf.gz"));

        // Test REMOVE action - remove specific files
        List<FileReferenceParam> removeFiles = Arrays.asList(
                new FileReferenceParam("NA19600.chrom20.small.bam")
        );

        QueryOptions removeOptions = new QueryOptions()
                .append(Constants.ACTIONS, new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), ParamUtils.UpdateAction.REMOVE))
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);

        result = catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalAnalysis.getId(),
                new ClinicalAnalysisUpdateParams().setFiles(removeFiles), removeOptions, ownerToken);

        assertEquals(2, result.first().getFiles().size());
        fileIds = result.first().getFiles().stream().map(File::getId).collect(Collectors.toSet());
        assertTrue(fileIds.contains("HG00096.chrom20.small.bam"));
        assertTrue(fileIds.contains("variant-test-file.vcf.gz"));
        assertFalse(fileIds.contains("NA19600.chrom20.small.bam"));
    }

}
