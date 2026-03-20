package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseFactory;
import org.opencb.opencga.core.config.Configuration;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalAnalysisConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalInterpretationConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalVariantConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalVariantEvidenceConverter;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.iterators.ClinicalIterator;
import com.zettagenomics.opencga.enterprise.cvdb.models.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.stats.ClinicalAnalysisStats;
import org.opencb.biodata.models.clinical.interpretation.stats.ClinicalVariantEvidenceStats;
import org.opencb.biodata.models.clinical.interpretation.stats.ClinicalVariantSummaryStats;
import org.opencb.biodata.models.clinical.interpretation.stats.InterpretationStats;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.PROJECT_PARAM;
import static org.opencb.opencga.core.api.ParamConstants.STUDY_PARAM;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.ADMIN_PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.OpenCGAEnterpriseCatalogManagerExternalResource.PASSWORD;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.CV_VARIANT_ID_NAME;
import static org.junit.Assert.assertEquals;

public class CvdbSolrEngineClinicalVariantSummaryTest {

    protected static CvdbSolrEngine cvdbEngine;
    protected static CollectionNameGenerator collectionNameGenerator;
    protected static String organizationId = "test";
    protected static String projectId1 = "project1";
    protected static Study study1;

    protected static String projectId2 = "project2";
    protected static Study study2;

    protected static String projectId3 = "project3";
    protected static Study study3;

    public static CvdbSolrExtenalResource cvdbSolrExternalResource;

    public static OpenCGAEnterpriseCatalogManagerExternalResource catalogManagerResource;

    protected static CatalogManager catalogManager;
    private static String opencgaToken;
    protected static String userToken;
    private static FamilyManager familyManager;

    public static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @BeforeClass
    public static void before() throws Throwable {
        // Catalog setup
        catalogManagerResource = new OpenCGAEnterpriseCatalogManagerExternalResource();
        catalogManagerResource.before();
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);

        // CVDB
        collectionNameGenerator = new CollectionNameGenerator(catalogManager);
        String collectionPrefix = collectionNameGenerator.getCollectionPrefix(organizationId, projectId1, userToken);
        cvdbSolrExternalResource = new CvdbSolrExtenalResource(false, organizationId, projectId1, collectionPrefix);
        cvdbSolrExternalResource.before();

        cvdbEngine = cvdbSolrExternalResource.configure();
        cvdbEngine.setCollectionNameGenerator(collectionNameGenerator);
        cvdbEngine.setCatalogManager(catalogManager);

        if (cvdbEngine.getSolrManager().isAlive()) {
            // Enterprise factory init
            EnterpriseFactory.init(catalogManager, catalogManager.getConfiguration());


            if (cvdbEngine.existCollections(collectionPrefix)) {
                cvdbEngine.removeCollections(collectionPrefix);
            }
            cvdbEngine.createCollections(projectId1, collectionPrefix, userToken);

            collectionPrefix = collectionNameGenerator.getCollectionPrefix(organizationId, projectId2, userToken);
            if (cvdbEngine.existCollections(collectionPrefix)) {
                cvdbEngine.removeCollections(collectionPrefix);
            }
            cvdbEngine.createCollections(projectId2, collectionPrefix, userToken);

            TestUtilities.loadClinicalAnalsysesInCatalog(Arrays.asList("ca3.json.gz", "ca4.json.gz", "ca5.json.gz"), study1, userToken, opencgaToken, catalogManager);
            TestUtilities.loadClinicalAnalsysesInCatalog(Collections.singletonList("ca3.json.gz"), study2, userToken, opencgaToken, catalogManager);

            // CVDB index from catalog
            CvdbIndexResult indexResult = cvdbEngine.indexProject(projectId1, true, userToken);
            System.out.println(indexResult.getFailures());
            assertEquals(3, indexResult.getNumIndexed());
            assertEquals(0, indexResult.getFailures().size());

            indexResult = cvdbEngine.indexProject(projectId2, true, userToken);
            System.out.println(indexResult.getFailures());
            assertEquals(1, indexResult.getNumIndexed());
            assertEquals(0, indexResult.getFailures().size());

            // Check CVDB datastores
            Assert.assertTrue(cvdbEngine.isAvailableCvdbDataStore(projectId1, userToken));
            Assert.assertTrue(cvdbEngine.isAvailableCvdbDataStore(projectId2, userToken));
            Assert.assertFalse(cvdbEngine.isAvailableCvdbDataStore(projectId3, userToken));
        }
    }

    public static void setUpCatalogManager(CatalogManager catalogManager) throws CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(ADMIN_PASSWORD).first().getToken();

        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId).setName("Test"), QueryOptions.empty(), opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user").setName("User Name").setOrganization(organizationId), PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(new User().setId("user2").setName("User Name2").setOrganization(organizationId), PASSWORD, opencgaToken);

        catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams()
                        .setOwner("user"),
                null, opencgaToken);

        userToken = catalogManager.getUserManager().login(organizationId, "user", PASSWORD).first().getToken();

        catalogManager.getProjectManager().create(projectId1, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userToken).first();
        study1 = catalogManager.getStudyManager().create(projectId1, "phase1", null, "Phase 1", "Done", null, null, null, null,
                INCLUDE_RESULT, userToken).first();

        catalogManager.getProjectManager().create(projectId2, "Project #2 about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userToken).first();
        study2 = catalogManager.getStudyManager().create(projectId2, "phase2", null, "Phase 3", "Done", null, null, null, null,
                INCLUDE_RESULT, userToken).first();

        catalogManager.getProjectManager().create(projectId3, "Project #3 about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userToken).first();
        study3 = catalogManager.getStudyManager().create(projectId3, "phase3", null, "Phase 3", "Done", null, null, null, null,
                INCLUDE_RESULT, userToken).first();
    }

    //-----------------------------------------------------------------------
    // T E S T S
    //-----------------------------------------------------------------------

    @Test
    public void testClinicalVariantSummaryStatsFromVariantId() throws IOException, CvdbException, CatalogException {
        Assume.assumeTrue(cvdbEngine.getSolrManager().isAlive());

        // Clinical analysis
        String variantId1 = "X:124481724:T:TA";

        List<String> variantIds = Arrays.asList(variantId1);
        List<String> projectIds = Arrays.asList(FqnUtils.buildFqn(organizationId, projectId1), FqnUtils.buildFqn(organizationId, projectId2), "ALL");

        DataResult<ClinicalVariantSummaryStats> result = cvdbEngine.getClinicalVariantSummaryStats(variantIds, null, userToken);
        System.out.println("============================================");
        for (ClinicalVariantSummaryStats resultResult : result.getResults()) {
            System.out.println(resultResult);
            System.out.println("============================================");
        }

        // Common checks
        checkCommonStats(result, variantIds, projectIds);

        Query query = new Query()
                .append(PROJECT_PARAM, projectId1)
                .append(STUDY_PARAM, study1.getFqn())
                .append(CV_VARIANT_ID_NAME, variantId1);
        DataResult<ClinicalAnalysis> caResult1 = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), userToken);
        query.append(PROJECT_PARAM, projectId2)
                .append(STUDY_PARAM, study2.getFqn());
        DataResult<ClinicalAnalysis> caResult2 = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), userToken);

        checkStats(result, caResult1.getResults(), variantId1, FqnUtils.buildFqn(organizationId, projectId1));
        checkStats(result, caResult2.getResults(), variantId1, FqnUtils.buildFqn(organizationId, projectId2));
    }

    @Test
    public void testClinicalVariantSummaryStatsFromVariantIds() throws IOException, CvdbException, CatalogException {
        Assume.assumeTrue(cvdbEngine.getSolrManager().isAlive());

        String variantId1 = "X:124481724:T:TA";
        String variantId2 = "X:53196017:G:A";

        List<String> variantIds = Arrays.asList(variantId1, variantId2);
        List<String> projectIds = Arrays.asList(FqnUtils.buildFqn(organizationId, projectId1), FqnUtils.buildFqn(organizationId, projectId2), "ALL");

        DataResult<ClinicalVariantSummaryStats> result = cvdbEngine.getClinicalVariantSummaryStats(variantIds, null, userToken);

        System.out.println("============================================");
        for (ClinicalVariantSummaryStats resultResult : result.getResults()) {
            System.out.println(resultResult);
            System.out.println("============================================");
        }

        // Common checks
        checkCommonStats(result, variantIds, projectIds);

        Query query = new Query()
                .append(PROJECT_PARAM, projectId1)
                .append(STUDY_PARAM, study1.getFqn())
                .append(CV_VARIANT_ID_NAME, variantId1);
        DataResult<ClinicalAnalysis> caResult1 = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), userToken);
        query.append(PROJECT_PARAM, projectId2)
                .append(STUDY_PARAM, study2.getFqn())
                .append(CV_VARIANT_ID_NAME, variantId2);
        DataResult<ClinicalAnalysis> caResult2 = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), userToken);

        checkStats(result, caResult1.getResults(), variantId1, FqnUtils.buildFqn(organizationId, projectId1));
        checkStats(result, caResult2.getResults(), variantId2, FqnUtils.buildFqn(organizationId, projectId2));
    }

    @Test
    public void testNoCvdbDataStoreClinicalAnalysis() throws IOException, CvdbException, CatalogException {
        String variantId1 = "X:124481724:T:TA";

        Assert.assertFalse(cvdbEngine.isAvailableCvdbDataStore(projectId3, userToken));

        Query query = new Query()
                .append(PROJECT_PARAM, projectId3)
                .append(STUDY_PARAM, study3.getFqn())
                .append(CV_VARIANT_ID_NAME, variantId1);

        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), userToken);
        Assert.assertEquals(0, result.getNumResults());

        ClinicalIterator<ClinicalAnalysis, ClinicalAnalysisSearch, ClinicalAnalysisConverter> caIterator = cvdbEngine.clinicalAnalysisIterator(query, QueryOptions.empty(), userToken);
        Assert.assertFalse(caIterator.hasNext());

        DataResult<FacetField> facetFieldDataResult = cvdbEngine.facetClinicalAnalyses(query, QueryOptions.empty(), userToken);
        Assert.assertEquals(0, facetFieldDataResult.getNumResults());
    }

    @Test
    public void testNoCvdbDataStoreClinicalInterpretation() throws IOException, CvdbException, CatalogException {
        String variantId1 = "X:124481724:T:TA";

        Assert.assertFalse(cvdbEngine.isAvailableCvdbDataStore(projectId3, userToken));

        Query query = new Query()
                .append(PROJECT_PARAM, projectId3)
                .append(STUDY_PARAM, study3.getFqn())
                .append(CV_VARIANT_ID_NAME, variantId1);

        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, QueryOptions.empty(), userToken);
        Assert.assertEquals(0, result.getNumResults());

        ClinicalIterator<Interpretation, ClinicalInterpretationSearch, ClinicalInterpretationConverter> ciIterator = cvdbEngine.clinicalInterpretationIterator(query, QueryOptions.empty(), userToken);
        Assert.assertFalse(ciIterator.hasNext());

        DataResult<FacetField> facetFieldDataResult = cvdbEngine.facetClinicalInterpretations(query, QueryOptions.empty(), userToken);
        Assert.assertEquals(0, facetFieldDataResult.getNumResults());
    }

    @Test
    public void testNoCvdbDataStoreClinicalVariant() throws IOException, CvdbException, CatalogException {
        String variantId1 = "X:124481724:T:TA";

        Assert.assertFalse(cvdbEngine.isAvailableCvdbDataStore(projectId3, userToken));

        Query query = new Query()
                .append(PROJECT_PARAM, projectId3)
                .append(STUDY_PARAM, study3.getFqn())
                .append(CV_VARIANT_ID_NAME, variantId1);

        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, QueryOptions.empty(), userToken);
        Assert.assertEquals(0, result.getNumResults());

        ClinicalIterator<ClinicalVariant, ClinicalVariantSearch, ClinicalVariantConverter> cvIterator = cvdbEngine.clinicalVariantIterator(query, QueryOptions.empty(), userToken);
        Assert.assertFalse(cvIterator.hasNext());

        DataResult<FacetField> facetFieldDataResult = cvdbEngine.facetClinicalVariants(query, QueryOptions.empty(), userToken);
        Assert.assertEquals(0, facetFieldDataResult.getNumResults());
    }

    @Test
    public void testNoCvdbDataStoreClinicalVariantEvidence() throws IOException, CvdbException, CatalogException {
        String variantId1 = "X:124481724:T:TA";

        Assert.assertFalse(cvdbEngine.isAvailableCvdbDataStore(projectId3, userToken));

        Query query = new Query()
                .append(PROJECT_PARAM, projectId3)
                .append(STUDY_PARAM, study3.getFqn())
                .append(CV_VARIANT_ID_NAME, variantId1);

        DataResult<ClinicalVariantEvidence> result = cvdbEngine.searchClinicalVariantEvidences(query, QueryOptions.empty(), userToken);
        Assert.assertEquals(0, result.getNumResults());

        ClinicalIterator<ClinicalVariantEvidence, ClinicalVariantEvidenceSearch, ClinicalVariantEvidenceConverter> cveIterator = cvdbEngine.clinicalVariantEvidenceIterator(query, QueryOptions.empty(), userToken);
        Assert.assertFalse(cveIterator.hasNext());

        DataResult<FacetField> facetFieldDataResult = cvdbEngine.facetClinicalVariantEvidences(query, QueryOptions.empty(), userToken);
        Assert.assertEquals(0, facetFieldDataResult.getNumResults());
    }

    //-----------------------------------------------------------------------
    // P R I V A T E    M E T H O D S
    //-----------------------------------------------------------------------

    private void checkCommonStats(DataResult<ClinicalVariantSummaryStats> result, List<String> variantIds, List<String> projectIds) {
        Assert.assertEquals(projectIds.size() * variantIds.size(), result.getNumResults());
        for (String variantId : variantIds) {
            for (String projectId : projectIds) {
                Assert.assertTrue(result.getResults().stream().anyMatch(r -> r.getId().equalsIgnoreCase(projectId) && r.getVariantId().equals(variantId)));
            }
        }
    }


    private void checkStats(DataResult<ClinicalVariantSummaryStats> result, List<ClinicalAnalysis> clinicalAnalyses,
                            String variantId, String projectId) {
        ClinicalVariantSummaryStats stats = result.getResults().stream().filter(r -> r.getId().equalsIgnoreCase(projectId) && r.getVariantId().equals(variantId)).findFirst().get();

        System.out.println("Assertions for variant " + variantId + " in project " + projectId);

        ClinicalAnalysisStats caStats = new ClinicalAnalysisStats();
        InterpretationStats ciStats = new InterpretationStats();
        ClinicalVariantEvidenceStats cveStats = new ClinicalVariantEvidenceStats();

        for (ClinicalAnalysis ca : clinicalAnalyses) {
            // Clinical analysis stats
            if (ca.getDisorder() != null && StringUtils.isNotEmpty(ca.getDisorder().getId())) {
                if (!caStats.getDisorders().containsKey(ca.getDisorder().getId())) {
                    caStats.getDisorders().put(ca.getDisorder().getId(), 0L);
                }
                caStats.getDisorders().put(ca.getDisorder().getId(), caStats.getDisorders().get(ca.getDisorder().getId()) + 1);
            }
            if (ca.getProband() != null) {
                if (CollectionUtils.isNotEmpty(ca.getProband().getDisorders())) {
                    for (Disorder disorder : ca.getProband().getDisorders()) {
                        if (StringUtils.isNotEmpty(disorder.getId())) {
                            if (!caStats.getProbandDisorders().containsKey(disorder.getId())) {
                                caStats.getProbandDisorders().put(disorder.getId(), 0L);
                            }
                            caStats.getProbandDisorders().put(disorder.getId(), caStats.getProbandDisorders().get(disorder.getId()) + 1);
                        }
                    }
                }
                if (CollectionUtils.isNotEmpty(ca.getProband().getPhenotypes())) {
                    for (Phenotype phenotype : ca.getProband().getPhenotypes()) {
                        if (StringUtils.isNotEmpty(phenotype.getId())) {
                            if (!caStats.getProbandPhenotypes().containsKey(phenotype.getId())) {
                                caStats.getProbandPhenotypes().put(phenotype.getId(), 0L);
                            }
                            caStats.getProbandPhenotypes().put(phenotype.getId(), caStats.getProbandPhenotypes().get(phenotype.getId()) + 1);
                        }
                    }
                }
            }

            // Interpretation stats
            if (ca.getInterpretation() != null && CollectionUtils.isNotEmpty(ca.getInterpretation().getPanels())) {
                updateInterpretationStats(ca.getInterpretation(), ciStats);
            }

            // Evidence stats
            if (ca.getInterpretation() != null && CollectionUtils.isNotEmpty(ca.getInterpretation().getPrimaryFindings())) {
                for (ClinicalVariant primaryFinding : ca.getInterpretation().getPrimaryFindings()) {
                    if (variantId.equalsIgnoreCase(primaryFinding.getId()) && CollectionUtils.isNotEmpty(primaryFinding.getEvidences())) {
                        for (ClinicalVariantEvidence cve : primaryFinding.getEvidences()) {
                            if (cve.getGenomicFeature() != null && StringUtils.isNotEmpty(cve.getGenomicFeature().getGeneName())) {
                                String gene = cve.getGenomicFeature().getGeneName();
                                if (!cveStats.getGenes().containsKey(gene)) {
                                    cveStats.getGenes().put(gene, 0L);
                                }
                                cveStats.getGenes().put(gene, cveStats.getGenes().get(gene) + 1);

                                if (CollectionUtils.isNotEmpty(cve.getGenomicFeature().getConsequenceTypes())) {
                                    for (SequenceOntologyTerm soTerm : cve.getGenomicFeature().getConsequenceTypes()) {
                                        if (!cveStats.getSoTerms().containsKey(soTerm.getName())) {
                                            cveStats.getSoTerms().put(soTerm.getName(), 0L);
                                        }
                                        cveStats.getSoTerms().put(soTerm.getName(), cveStats.getSoTerms().get(soTerm.getName()) + 1);
                                    }
                                }
                            }
                            if (CollectionUtils.isNotEmpty(cve.getModeOfInheritances())) {
                                for (ClinicalProperty.ModeOfInheritance moi : cve.getModeOfInheritances()) {
                                    if (!cveStats.getMois().containsKey(moi.name())) {
                                        cveStats.getMois().put(moi.name(), 0L);
                                    }
                                    cveStats.getMois().put(moi.name(), cveStats.getMois().get(moi.name()) + 1);
                                }
                            }

                        }
                    }
                }
            }
        }

        // Assertions
        Assert.assertTrue(areEquivalent(stats.getClinicalAnalysis().getDisorders(), caStats.getDisorders(), "ca disorders"));
        Assert.assertTrue(areEquivalent(stats.getClinicalAnalysis().getProbandDisorders(), caStats.getProbandDisorders(), "ca proband disorders"));
        Assert.assertTrue(areEquivalent(stats.getClinicalAnalysis().getProbandPhenotypes(), caStats.getProbandPhenotypes(), "ca proband phenotypes"));

        Assert.assertTrue(areEquivalent(stats.getInterpretation().getPanels(), ciStats.getPanels(), "ci panels"));

        Assert.assertTrue(areEquivalent(stats.getEvidence().getGenes(), cveStats.getGenes(), "cve genes"));
        Assert.assertTrue(areEquivalent(stats.getEvidence().getSoTerms(), cveStats.getSoTerms(), "cve soTerms"));
        Assert.assertTrue(areEquivalent(stats.getEvidence().getMois(), cveStats.getMois(), "cve mois"));
    }

    private void updateInterpretationStats(Interpretation interpretation, InterpretationStats ciStats) {
        for (Panel panel : interpretation.getPanels()) {
            if (!ciStats.getPanels().containsKey(panel.getId())) {
                ciStats.getPanels().put(panel.getId(), 0L);
            }
            ciStats.getPanels().put(panel.getId(), ciStats.getPanels().get(panel.getId()) + 1);
            if (!ciStats.getPanels().containsKey(panel.getName())) {
                ciStats.getPanels().put(panel.getName(), 0L);
            }
            ciStats.getPanels().put(panel.getName(), ciStats.getPanels().get(panel.getName()) + 1);
        }
    }

    private boolean areEquivalent(Map<String, Long> map1, Map<String, Long> map2, String msg) {
        if (map1.size() > map2.size()) {
            System.out.println(msg + "; Map is bigger: " + map1.size() + " vs " + map2.size());
            System.out.println("map1:");
            for (Map.Entry<String, Long> entry : map1.entrySet()) {
                System.out.println("Key: " + entry.getKey() + " -> " + entry.getValue());
            }
            System.out.println("map2:");
            for (Map.Entry<String, Long> entry : map2.entrySet()) {
                System.out.println("Key: " + entry.getKey() + " -> " + entry.getValue());
            }
            return false;
        }
        for (String key : map1.keySet()) {
            if (!map2.containsKey(key) || !map2.get(key).equals(map1.get(key))) {
                System.out.println(msg + "; Maps differ at key " + key + ": " + map1.get(key) + " != " + map2.get(key));
                return false;
            }
        }
        return true;
    }
}

