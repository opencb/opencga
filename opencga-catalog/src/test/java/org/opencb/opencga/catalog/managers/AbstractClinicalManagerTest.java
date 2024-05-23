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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;

@Category(MediumTests.class)
public class AbstractClinicalManagerTest extends GenericTest {

    public final static String PASSWORD = "Password1234;";

    public final static String CA_ID1 = "clinical-analysis-1";

    public final static String CA_ID2 = "clinical-analysis-2";
    public final static String PROBAND_ID2 = "manuel_individual";

    public final static String CA_ID3 = "clinical-analysis-3";
    public final static String PROBAND_ID3 = "HG005";

    public final static String CA_ID4 = "clinical-analysis-4";
    public final static String PROBAND_ID4 = "HG105";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    public CatalogManager catalogManager;
    protected final String organizationId = "test";
    public String token;
    public String studyFqn;
    public Family family;
    public ClinicalAnalysis clinicalAnalysis;
    public ClinicalAnalysis clinicalAnalysis2;
    public ClinicalAnalysis clinicalAnalysis3;

    @Before
    public void setUp() throws IOException, CatalogException, URISyntaxException {
        catalogManager = catalogManagerResource.getCatalogManager();
        setUpCatalogManager();
    }

    public void setUpCatalogManager() throws IOException, CatalogException, URISyntaxException {
        ClinicalAnalysis auxClinicalAnalysis;

        // Create new organization, owner and admins
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId).setName("Test"), QueryOptions.empty(), catalogManagerResource.getAdminToken());
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, organizationId, null, catalogManagerResource.getAdminToken());

        catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams().setOwner("user"),
                null, catalogManagerResource.getAdminToken());

        token = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).getToken();

        catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens", null, "GRCh38",
                new QueryOptions(), token);

        Study study = catalogManager.getStudyManager().create("1000G", "phase1", null, "Phase 1", "Done", null, null,
                null, null, null, token).first();
        studyFqn = study.getFqn();

        family = catalogManager.getFamilyManager().create(studyFqn, getFamily(), QueryOptions.empty(), token).first();
//
        // Clinical analysis
        auxClinicalAnalysis = new ClinicalAnalysis()
                .setId(CA_ID1).setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000")
                .setDisorder(getDisorder())
                .setProband(getChild())
                .setFamily(getFamily());
//
        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().create(studyFqn, auxClinicalAnalysis,
                        new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token)
                .first();
//
        catalogUploadFile("/biofiles/family.vcf");

        //---------------------------------------------------------------------
        // Clinical analysis for exomiser test (SINGLE, manuel)
        //---------------------------------------------------------------------

        catalogManager.getIndividualManager().create(studyFqn, getMamuel(), QueryOptions.empty(), token).first();

        auxClinicalAnalysis = new ClinicalAnalysis()
                .setId(CA_ID2).setDescription("My description - exomiser").setType(ClinicalAnalysis.Type.SINGLE)
                .setDueDate("20180510100000")
                .setDisorder(getDisorder())
                .setProband(getMamuel());

        clinicalAnalysis2 = catalogManager.getClinicalAnalysisManager()
                .create(studyFqn, auxClinicalAnalysis, new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token)
                .first();

        catalogUploadFile("/biofiles/exomiser.vcf.gz");

        //---------------------------------------------------------------------
        // Chinese trio: FAMILY (clinicalAnalysis3)
        //---------------------------------------------------------------------

        Individual hg006Individual =  new Individual().setId("HG006_individual")
                .setPhenotypes(Collections.emptyList())
                .setSex(SexOntologyTermAnnotation.initMale())
                .setSamples(Collections.singletonList(new Sample().setId("HG006")));

        Individual hg007Individual =  new Individual().setId("HG007_individual")
                .setPhenotypes(Collections.emptyList())
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setSamples(Collections.singletonList(new Sample().setId("HG007")));

        Individual hg005Individual =  new Individual().setId("HG005_individual")
                .setDisorders(Collections.singletonList(getDisorder()))
                .setPhenotypes(getPhenotypes())
                .setFather(hg006Individual)
                .setMother(hg007Individual)
                .setSex(SexOntologyTermAnnotation.initMale())
                .setSamples(Collections.singletonList(new Sample().setId(PROBAND_ID3)));

        Individual hg004Individual =  new Individual().setId("HG004_individual")
                .setFather(hg006Individual)
                .setMother(hg007Individual)
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setSamples(Collections.singletonList(new Sample().setId("HG004")));

        Family chineseFamily = new Family("chinese_trio_family", "chinese_trio_family", null, null,
                Arrays.asList(hg005Individual, hg006Individual, hg007Individual, hg004Individual), "", 4, Collections.emptyList(),
                Collections.emptyMap());
        catalogManager.getFamilyManager().create(studyFqn, chineseFamily, QueryOptions.empty(), token).first();

        auxClinicalAnalysis = new ClinicalAnalysis()
                .setId(CA_ID3)
                .setDescription("My description - exomiser - trio - family")
                .setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000")
                .setDisorder(getDisorder())
                .setProband(hg005Individual)
                .setFamily(chineseFamily);

        catalogManager.getClinicalAnalysisManager().create(studyFqn, auxClinicalAnalysis, QueryOptions.empty(), token)
                .first();

        catalogUploadFile("/biofiles/HG004.1k.vcf.gz");
        catalogUploadFile("/biofiles/HG005.1k.vcf.gz");
        catalogUploadFile("/biofiles/HG006.1k.vcf.gz");
        catalogUploadFile("/biofiles/HG007.1k.vcf.gz");

        //---------------------------------------------------------------------
        // Chinese trio: SINGLE (clinicalAnalysis4)
        //---------------------------------------------------------------------

        Individual hg106Individual =  new Individual().setId("HG106_individual")
                .setPhenotypes(Collections.emptyList())
                .setSex(SexOntologyTermAnnotation.initMale())
                .setSamples(Collections.singletonList(new Sample().setId("HG106")));

        Individual hg107Individual =  new Individual().setId("HG107_individual")
                .setPhenotypes(Collections.emptyList())
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setSamples(Collections.singletonList(new Sample().setId("HG107")));

        Individual hg105Individual =  new Individual().setId("HG105_individual")
                .setDisorders(Collections.singletonList(getDisorder()))
                .setPhenotypes(getPhenotypes())
                .setFather(hg106Individual)
                .setMother(hg107Individual)
                .setSex(SexOntologyTermAnnotation.initMale())
                .setSamples(Collections.singletonList(new Sample().setId(PROBAND_ID4)));

        Individual hg104Individual =  new Individual().setId("HG104_individual")
                .setFather(hg106Individual)
                .setMother(hg107Individual)
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setSamples(Collections.singletonList(new Sample().setId("HG104")));

        Family chineseSingle = new Family("chinese_trio_single", "chinese_trio_single", null, null,
                Arrays.asList(hg105Individual, hg106Individual, hg107Individual, hg104Individual), "", 4, Collections.emptyList(),
                Collections.emptyMap());
        catalogManager.getFamilyManager().create(studyFqn, chineseSingle, QueryOptions.empty(), token).first();

        auxClinicalAnalysis = new ClinicalAnalysis()
                .setId(CA_ID4)
                .setDescription("My description - exomiser - trio - single")
                .setType(ClinicalAnalysis.Type.SINGLE)
                .setDueDate("20180510100000")
                .setDisorder(getDisorder())
                .setProband(hg105Individual)
                .setFamily(chineseSingle);

        catalogManager.getClinicalAnalysisManager().create(studyFqn, auxClinicalAnalysis, QueryOptions.empty(), token)
                .first();

        catalogUploadFile("/biofiles/HG104.1k.vcf.gz");
        catalogUploadFile("/biofiles/HG105.1k.vcf.gz");
        catalogUploadFile("/biofiles/HG106.1k.vcf.gz");
        catalogUploadFile("/biofiles/HG107.1k.vcf.gz");


        OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().search(studyFqn, new Query(), new QueryOptions(INCLUDE, "id"), token);
        Assert.assertEquals(12, sampleResult.getNumResults());
    }

    private void catalogUploadFile(String path) throws IOException, CatalogException {
        try (InputStream inputStream = getClass().getResource(path).openStream()) {
            catalogManager.getFileManager().upload(studyFqn, inputStream,
                    new File().setPath(Paths.get(path).getFileName().toString()), false, true, false, token);
        }
    }

    private Individual getMother() {
        return new Individual().setId("mother")
                .setDisorders(Collections.emptyList())
                .setSamples(Collections.singletonList(new Sample().setId("s2")));
    }

    private Individual getFather() {
        return new Individual().setId("father")
                .setDisorders(Collections.singletonList(getDisorder()))
                .setSamples(Collections.singletonList(new Sample().setId("s1")));
    }

    private Individual getChild() {
        return new Individual().setId("child")
                .setDisorders(Collections.singletonList(getDisorder()))
                .setFather(getFather())
                .setMother(getMother())
                .setSex(SexOntologyTermAnnotation.initMale())
                .setSamples(Collections.singletonList(new Sample().setId("s3")));
    }

    private Family getFamily() {
        return new Family("family", "family", null, null, Arrays.asList(getChild(), getFather(), getMother()), "", 3,
                Collections.emptyList(), Collections.emptyMap());
    }

    private Disorder getDisorder() {
        return new Disorder("disorder", "Disorder", "source", "description", null, null);
    }

    private Individual getMamuel() {
        return new Individual().setId(PROBAND_ID2)
                .setPhenotypes(getPhenotypes())
                .setSex(SexOntologyTermAnnotation.initMale())
                .setSamples(Collections.singletonList(new Sample().setId(PROBAND_ID2.split("_")[0])));
    }

    private List<Phenotype> getPhenotypes() {
        List<Phenotype> phenotypes = new ArrayList<>();
        phenotypes.add(new Phenotype("HP:0001159", "Syndactyly", "HPO"));
        phenotypes.add(new Phenotype("HP:0000486", "Strabismus", "HPO"));
        phenotypes.add(new Phenotype("HP:0000327", "Hypoplasia of the maxilla", "HPO"));
        phenotypes.add(new Phenotype("HP:0000520", "Proptosis", "HPO"));
        phenotypes.add(new Phenotype("HP:0000316", "Hypertelorism", "HPO"));
        phenotypes.add(new Phenotype("HP:0000244", "Brachyturricephaly", "HPO"));
        return phenotypes;
    }
}
