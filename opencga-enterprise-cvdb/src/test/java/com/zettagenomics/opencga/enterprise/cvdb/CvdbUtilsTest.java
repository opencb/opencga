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

package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantSummary;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
@Category(LongTests.class)
public class CvdbUtilsTest {

    public static final String ORGANIZATION = "test";
    public static final String USER = "user";
    public static final String PASSWORD = "P;-tosl-ak234W123456;";
    public static final String PROJECT = "project";
    public static final String STUDY = "study";
    public static final String PHENOTYPE_NAME = "myPhenotype";
    public static final Phenotype PHENOTYPE = new Phenotype(PHENOTYPE_NAME, PHENOTYPE_NAME, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String DB_NAME = VariantStorageManager.buildDatabaseName("opencga_test", ORGANIZATION, PROJECT);
    private ToolRunner toolRunner;
    private static String father = "NA19661";
    private static String mother = "NA19660";
    private static String son = "NA19685";
    private static String daughter = "NA19600";

    private Project project;
    private Study study;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][]{
                {HadoopVariantStorageEngine.STORAGE_ENGINE_ID}
        };
    }

    public CvdbUtilsTest(String storageEngine) {
        if (!storageEngine.equals(CvdbUtilsTest.storageEngine)) {
            indexed = false;
        }
        CvdbUtilsTest.storageEngine = storageEngine;
    }

    protected static CvdbSolrEngine cvdbEngine;
    protected static CvdbSolrExtenalResource cvdbSolrExternalResource;

    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;

    public static OpenCGAEnterpriseTestExternalResource opencga = new OpenCGAEnterpriseTestExternalResource();

    private static String storageEngine;
    private static boolean indexed = false;
    private static String token;
    private static File file;

    @Before
    public void setUp() throws Throwable {
        if (!indexed) {
            indexed = true;

            opencga.after();
            opencga.before(storageEngine);

            catalogManager = opencga.getCatalogManager();
            variantStorageManager = opencga.getVariantStorageManager();
            variantStorageManager.getStorageConfiguration().setMode(StorageConfiguration.Mode.READ_WRITE);

            setUpCatalogManager();

            // CVDB
            cvdbSolrExternalResource = new CvdbSolrExtenalResource(true, PROJECT);
            cvdbSolrExternalResource.before();

            cvdbEngine = cvdbSolrExternalResource.configure();
            cvdbEngine.setCatalogManager(catalogManager);
            cvdbEngine.setVariantStorageMetadataManager(new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory()));

            if (!cvdbEngine.existCollections(PROJECT)) {
                cvdbEngine.createCollections(PROJECT);
            }

            // Load and index
            TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca1.json.gz", "ca3.json.gz"), study, token, opencga.getAdminToken(),
                    catalogManager);

            // CVDB index from catalog
            CvdbIndexResult indexResult = cvdbEngine.indexProject(PROJECT, catalogManager, true, token);
            System.out.println(indexResult.getFailures());
            assertEquals(2, indexResult.getNumIndexed());
            assertEquals(0, indexResult.getFailures().size());

            // Index VCF
            file = opencga.createFile(STUDY, "LP1.vcf", token);
            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true), token);

            opencga.getStorageConfiguration().getVariant().setDefaultEngine(storageEngine);
            VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(storageEngine, DB_NAME);
            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) engine.getDBAdaptor()), Paths.get(opencga.createTmpOutdir("_hbase_print_variants")).toUri());
            }
        }

        // Reset engines
        opencga.getStorageEngineFactory().close();
        catalogManager = opencga.getCatalogManager();
        variantStorageManager = opencga.getVariantStorageManager();
        variantStorageManager.getStorageConfiguration().setMode(StorageConfiguration.Mode.READ_ONLY);
        toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();
    }

    @AfterClass
    public static void afterClass() {
//        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
//            hadoopExternalResource.after();
//        }
        opencga.after();
    }

    public void setUpCatalogManager() throws IOException, CatalogException {
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(ORGANIZATION), QueryOptions.empty(), opencga.getAdminToken());
        catalogManager.getUserManager().create(USER, "User Name", "mail@ebi.ac.uk", PASSWORD, ORGANIZATION, null, opencga.getAdminToken());
        catalogManager.getOrganizationManager().update(ORGANIZATION, new OrganizationUpdateParams().setAdmins(Collections.singletonList(USER)),
                null,
                opencga.getAdminToken());
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();

        project = catalogManager.getProjectManager().create(new ProjectCreateParams()
                        .setId(PROJECT)
                        .setDescription("Project about some genomes").setOrganism(new ProjectOrganism("hsapiens", "grch38")),
                new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first();
        String projectId = project.getId();
        study = catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, token).first();

        // Create 10 samples not indexed
        for (int i = 0; i < 10; i++) {
            Sample sample = new Sample().setId("SAMPLE_" + i);
            if (i % 2 == 0) {
                sample.setPhenotypes(Collections.singletonList(PHENOTYPE));
            }
            catalogManager.getSampleManager().create(STUDY, sample, null, token);
        }
    }


    //------------------------------------------------------------------------------------
    // T E S T S
    //------------------------------------------------------------------------------------

    @Test
    public void testCvdbContent() throws IOException, CvdbException, CatalogException {
        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, PROJECT);
        query.put(STUDY_PARAM_NAME, ALL_STUDIES_VALUE);
        QueryOptions queryOptions = new QueryOptions();

        DataResult<ClinicalAnalysis> caResult = cvdbEngine.searchClinicalAnalyses(query, queryOptions, token);
        System.out.println("num. ca = " + caResult.getNumResults());
        assertEquals(2, caResult.getNumResults());
        DataResult<Interpretation> ciResult = cvdbEngine.searchClinicalInterpretations(query, queryOptions, token);
        System.out.println("num. ci = " + ciResult.getNumResults());
        assertEquals(8, ciResult.getNumResults());
        DataResult<ClinicalVariant> cvResult = cvdbEngine.searchClinicalVariants(query, queryOptions, token);
        System.out.println("num. cv = " + cvResult.getNumResults());
        assertEquals(31, cvResult.getNumResults());
        DataResult<ClinicalVariantEvidence> cveResult = cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, token);
        System.out.println("num. cve = " + cveResult.getNumResults());
        assertEquals(31, cveResult.getNumResults());
    }

    @Test
    public void testClinicalVariantSummary() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_clinical_variant_summary"));
        System.out.println("output = " + outDir.toAbsolutePath());

        ClinicalInterpretationManager clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager,
                opencga.getStorageEngineFactory(), opencga.getOpencgaHome());

        // First, get clinical variants
        Query query = new Query();
        query.put(VariantQueryParam.STUDY.key(), STUDY);
        query.put(VariantQueryParam.ID.key(), "X:72140354:A:G");
        QueryOptions queryOptions = new QueryOptions();

        OpenCGAResult<ClinicalVariant> cvResult = clinicalInterpretationManager.get(query, queryOptions, token);
        assertEquals(1, cvResult.getNumResults());
        assertTrue(TestUtilities.existsVariantId(query.getString(VariantQueryParam.ID.key()), cvResult.getResults()));

        DataResult<ClinicalVariantSummary> summaryResult = cvdbEngine.getClinicalVariantSummary(query.getString(VariantQueryParam.ID.key()), PROJECT, STUDY, token);
        assertEquals(1, summaryResult.getNumResults());
    }

    @Test
    public void testCvdbUtils() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_cvdb_utils"));
        System.out.println("output = " + outDir.toAbsolutePath());

        ClinicalInterpretationManager clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager,
                opencga.getStorageEngineFactory(), opencga.getOpencgaHome());

        Query query = new Query();
        query.put(PROJECT_PARAM_NAME, PROJECT);
        query.put(VariantQueryParam.STUDY.key(), STUDY);
        query.put(VariantQueryParam.ID.key(), "X:72140354:A:G");
        QueryOptions queryOptions = new QueryOptions();

        DataResult<ClinicalVariant> cvResult = CvdbUtils.getClinicalVariant(query, queryOptions, clinicalInterpretationManager, cvdbEngine, token);
        assertEquals(1, cvResult.getNumResults());
        assertTrue(TestUtilities.existsVariantId(query.getString(VariantQueryParam.ID.key()), cvResult.getResults()));
        assertNotNull(cvResult.first().getSummary());
        assertEquals(1, cvResult.first().getSummary().getEvidenceTierCounts().size());
        assertEquals(1, (int) cvResult.first().getSummary().getEvidenceTierCounts().get("TIER3"));
    }

    @Test
    public void testVariantStats() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        Path outDir = Paths.get(opencga.createTmpOutdir("_variant_stats"));
        System.out.println("output = " + outDir.toAbsolutePath());
        List<String> samples = file.getSampleIds();

        VariantStatsAnalysis variantStatsAnalysis = new VariantStatsAnalysis()
                .setStudy(STUDY)
                .setSamples(samples.subList(1, 3));
        variantStatsAnalysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, "", token);

        ExecutionResult ar = variantStatsAnalysis.start();
        TestUtilities.checkExecutionResult(ar, storageEngine);

        MutableInt count = new MutableInt();
        java.io.File file = TestUtilities.getOutputFile(outDir);
        FileUtils.lineIterator(file).forEachRemaining(line -> {
            if (!line.startsWith("#")) {
                count.increment();
            }
        });
        assertEquals(variantStorageManager.count(new Query(VariantQueryParam.STUDY.key(), STUDY), token).first().intValue(),
                count.intValue());
    }

    @Test
    public void testCellbaseConfigure() throws Exception {
        String project = "Project_test_cellbase_configure";
        catalogManager.getProjectManager().create(new ProjectCreateParams(project, project, "", "", "", new ProjectOrganism("hsapiens", "grch38"), null, null), QueryOptions.empty(), token);

        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("The storage engine is in mode=READ_ONLY");
        variantStorageManager.setCellbaseConfiguration(project, new CellBaseConfiguration("https://uk.ws.zettagenomics.com/cellbase/", "v5.2", "3", ""), false, null, token);
    }
}
