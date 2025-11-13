package org.opencb.opencga.analysis.clinical.interpretation;

import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisUtilsTest;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationConfiguration;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.operations.VariantSecondarySampleIndexOperationTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.interpretation.ClinicalInterpretationAnalysisParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.operations.variant.VariantSecondarySampleIndexParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.biodata.models.clinical.ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS;
import static org.opencb.biodata.models.clinical.ClinicalProperty.ModeOfInheritance.DE_NOVO;
import static org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest.TIERING_MODE;
import static org.opencb.opencga.catalog.utils.ResourceManager.ANALYSIS_DIRNAME;

@Category(MediumTests.class)
public class ClinicalInterpretationAnalysisTest {

    private static AbstractClinicalManagerTest clinicalTest;
    private static ResourceManager resourceManager;

    private ToolRunner toolRunner;

    private ClinicalAnalysis ca;
    private Path outDir;

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource(true);

    @Before
    public void setUp() throws Exception {
        opencga.clear();
        clinicalTest = ClinicalAnalysisUtilsTest.getClinicalTest(opencga, TIERING_MODE);
        resourceManager = new ResourceManager(opencga.getOpencgaHome());

        toolRunner = new ToolRunner(opencga.getOpencgaHome().toAbsolutePath().toString(), clinicalTest.catalogManager,
                opencga.getVariantStorageManager());
    }

    @After
    public void tearDown() throws Exception {
        opencga.clear();
    }

    @Test
    public void testClinicalInterpretationConfiguration() throws IOException, CatalogException, ToolException {
        InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream("interpretation/interpretation-configuration-1.yml");
        ClinicalInterpretationConfiguration config = ClinicalInterpretationConfiguration.load(resourceAsStream);
        Assert.assertEquals(9, config.getQueries().size());
        Assert.assertEquals(3, config.getTierConfiguration().getTiers().size());
        Assert.assertEquals("TIER1", config.getTierConfiguration().getTiers().get("tier_1").getLabel());
        Assert.assertEquals("TIER3", config.getTierConfiguration().getTiers().get("tier_3").getLabel());
    }

    @Test
    public void testClinicalInterpretationWithDefaultConfig() throws IOException, CatalogException, ToolException {
        Assume.assumeTrue(Paths.get("/opt/tiering-data").toFile().exists());

        String caId = clinicalTest.CA_OPA_15914_1;

        checkClinicalAnalysis(caId);

        outDir = Paths.get(opencga.createTmpOutdir("_interpretation_default_config"));
        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());
        System.out.println("outDir = " + outDir);

        ClinicalInterpretationAnalysisParams params = new ClinicalInterpretationAnalysisParams();
        params.setClinicalAnalysisId(caId);

        toolRunner.execute(ClinicalInterpretationAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, clinicalTest.studyFqn),
                outDir, null, false, clinicalTest.token);

        ca = clinicalTest.catalogManager.getClinicalAnalysisManager().get(clinicalTest.studyFqn, caId, QueryOptions.empty(),
                clinicalTest.token).first();

        assertEquals(1, ca.getSecondaryInterpretations().size());
        assertEquals(0, ca.getSecondaryInterpretations().get(0).getPrimaryFindings().size());

        assertTrue(ca.getSecondaryInterpretations().get(0).getAttributes().containsKey("configuration"));
        ClinicalInterpretationConfiguration config = JacksonUtils.getDefaultObjectMapper().convertValue(ca.getSecondaryInterpretations()
                .get(0).getAttributes().get("configuration"), ClinicalInterpretationConfiguration.class);
        Assert.assertEquals(9, config.getQueries().size());
        Assert.assertEquals(3, config.getTierConfiguration().getTiers().size());
        Assert.assertEquals("TIER1", config.getTierConfiguration().getTiers().get("tier_1").getLabel());
        Assert.assertEquals("TIER3", config.getTierConfiguration().getTiers().get("tier_3").getLabel());

        // Clean up interpretations
        deleteInterpretations();
    }

    @Test
    public void testClinicalInterpretationWithCustomConfig() throws IOException, CatalogException, ToolException, StorageEngineException {
        Assume.assumeTrue(Paths.get("/opt/tiering-data").toFile().exists());

        String caId = clinicalTest.CA_OPA_15914_1;
        checkClinicalAnalysis(caId);

        outDir = Paths.get(opencga.createTmpOutdir("_interpretation_custom_config"));
        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());
        System.out.println("outDir = " + outDir);

        // Read tiering configuration and register in catalog
        Path configPath = opencga.getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve("interpretation").resolve("interpretation-configuration.yml");
        ClinicalInterpretationConfiguration interpretationConfiguration = JacksonUtils.getDefaultObjectMapper().convertValue(new Yaml().load(Files.newInputStream(configPath)), ClinicalInterpretationConfiguration.class);
        for (String key: interpretationConfiguration.getQueries().keySet()) {
            Map<String, Object> filters = (Map<String, Object>) interpretationConfiguration.getQueries().get(key);
            filters.remove("cohortStatsMaf");
        }
        Path updatedConfigPath = Paths.get(opencga.createTmpOutdir("_interpretation_custom_config_data")).resolve(configPath.getFileName());
        JacksonUtils.getDefaultObjectMapper().writerFor(ClinicalInterpretationConfiguration.class).writeValue(updatedConfigPath.toFile(), interpretationConfiguration);
        InputStream inputStream = Files.newInputStream(updatedConfigPath);
        File opencgaFile = opencga.getCatalogManager().getFileManager().upload(clinicalTest.studyFqn, inputStream,
                new File().setPath("data/" + updatedConfigPath.getFileName()), false, true, false, clinicalTest.token).first();

        toolRunner.execute(VariantSecondarySampleIndexOperationTool.class, clinicalTest.studyFqn, new VariantSecondarySampleIndexParams()
                        .setFamilyIndex(true)
                        .setSample(Collections.singletonList(ca.getProband().getSamples().get(0).getId())),
                Paths.get(opencga.createTmpOutdir()), "family-index", false, clinicalTest.token);

        ClinicalInterpretationAnalysisParams params = new ClinicalInterpretationAnalysisParams();
        params.setClinicalAnalysisId(caId);
        params.setConfigFile(opencgaFile.getId());

        toolRunner.execute(ClinicalInterpretationAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, clinicalTest.studyFqn),
                outDir, null, false, clinicalTest.token);

        ca = clinicalTest.catalogManager.getClinicalAnalysisManager().get(clinicalTest.studyFqn, caId, QueryOptions.empty(),
                clinicalTest.token).first();

        for (ClinicalVariant primaryFinding : ca.getSecondaryInterpretations().get(0).getPrimaryFindings()) {
            System.out.println("primaryFinding = " + primaryFinding.getId());
            for (ClinicalVariantEvidence evidence : primaryFinding.getEvidences()) {
                System.out.println("\tEvidence " + evidence.getClassification().getTier() + " (gene, transcript, panel, moi, so): "
                        + evidence.getGenomicFeature().getGeneName() + ", "
                        + evidence.getGenomicFeature().getTranscriptId() + ", "
                        + evidence.getPanelId() + ", "
                        + evidence.getModeOfInheritances().get(0) + ", "
                        +  StringUtils.join(evidence.getGenomicFeature().getConsequenceTypes().stream().map(ct -> ct.getName()).toArray(), ",")
                );
            }
        }

        assertEquals(1, ca.getSecondaryInterpretations().size());
        assertEquals(3, ca.getSecondaryInterpretations().get(0).getPrimaryFindings().size());
        int numEvidences = 0;
        for (ClinicalVariant primaryFinding : ca.getSecondaryInterpretations().get(0).getPrimaryFindings()) {
            numEvidences += primaryFinding.getEvidences().size();
        }
        checkEvidence("13:24905747:C:T", "TIER3", "CENPJ", "ENST00000418179.1", "Intellectual_disability-PanelAppId-285",
                COMPOUND_HETEROZYGOUS, "2KB_upstream_variant", ca.getSecondaryInterpretations().get(0).getPrimaryFindings());
        checkEvidence("13:24907147:A:C", "TIER2", "CENPJ", "XM_011535150.2", "Intellectual_disability-PanelAppId-285",
                COMPOUND_HETEROZYGOUS, "missense_variant", ca.getSecondaryInterpretations().get(0).getPrimaryFindings());
        checkEvidence("15:93020173:C:T", "TIER1", "CHD2", "ENST00000626874.2", "Epileptic_encephalopathy-PanelAppId-67",
                DE_NOVO, "stop_gained", ca.getSecondaryInterpretations().get(0).getPrimaryFindings());
        assertEquals(41, numEvidences);

        assertTrue(ca.getSecondaryInterpretations().get(0).getAttributes().containsKey("configuration"));
        ClinicalInterpretationConfiguration config = JacksonUtils.getDefaultObjectMapper().convertValue(ca.getSecondaryInterpretations()
                .get(0).getAttributes().get("configuration"), ClinicalInterpretationConfiguration.class);
        Assert.assertEquals(9, config.getQueries().size());
        Assert.assertEquals(3, config.getTierConfiguration().getTiers().size());
        Assert.assertEquals("TIER1", config.getTierConfiguration().getTiers().get("tier_1").getLabel());
        Assert.assertEquals("TIER3", config.getTierConfiguration().getTiers().get("tier_3").getLabel());

        // Clean up interpretations
        deleteInterpretations();
    }

    private void checkEvidence(String variantId, String tier, String gene, String transcript, String panel,
                               ClinicalProperty.ModeOfInheritance moi, String soTerm, List<ClinicalVariant> primaryFindings) {
        for (ClinicalVariant primaryFinding : primaryFindings) {
            if (primaryFinding.toString().equals(variantId)) {
                for (ClinicalVariantEvidence evidence : primaryFinding.getEvidences()) {
                    if (evidence.getClassification().getTier().equals(tier)
                            && evidence.getGenomicFeature().getGeneName().equals(gene)
                            && evidence.getGenomicFeature().getTranscriptId().equals(transcript)
                            && evidence.getPanelId().equals(panel)
                            && evidence.getModeOfInheritances().contains(moi)) {
                        for (String ctName : evidence.getGenomicFeature().getConsequenceTypes().stream()
                                .map(SequenceOntologyTerm::getName).collect(Collectors.toList())) {
                            if (ctName.equals(soTerm)) {
                                return;
                            }
                        }
                    }
                }
            }
        }
        fail("Did not find evidence for variant " + variantId + " with tier " + tier + ", gene " + gene + ", transcript "
                + transcript + ", panel " + panel + ", MOI " + moi + " and SO term " + soTerm);
    }


    private void checkClinicalAnalysis(String caId) throws CatalogException {
        OpenCGAResult<ClinicalAnalysis> caResult = clinicalTest.catalogManager.getClinicalAnalysisManager()
                .get(clinicalTest.studyFqn, caId, QueryOptions.empty(), clinicalTest.token);
        assertEquals(1, caResult.getNumResults());
        assertEquals(0, caResult.first().getSecondaryInterpretations().size());

        ca = caResult.first();
    }

    private void deleteInterpretations() throws CatalogException {
        // Delete all interpretations
        clinicalTest.catalogManager.getInterpretationManager().delete(clinicalTest.studyFqn, ca.getId(),
                Arrays.asList(ca.getInterpretation().getId(), ca.getSecondaryInterpretations().get(0).getId()), false, clinicalTest.token);

        ca = clinicalTest.catalogManager.getClinicalAnalysisManager().get(clinicalTest.studyFqn, ca.getId(), QueryOptions.empty(), clinicalTest.token).first();

        assertEquals(null, ca.getInterpretation());
        assertEquals(0, ca.getSecondaryInterpretations().size());
    }
}
