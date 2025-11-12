package org.opencb.opencga.analysis.clinical.interpretation;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisUtilsTest;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationConfiguration;
import org.opencb.opencga.analysis.clinical.InterpretationAnalysisConfiguration;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.interpretation.ClinicalInterpretationAnalysisParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
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
//        checkClinicalAnalysis();

        InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream("interpretation/interpretation-configuration-1.yml");
        ClinicalInterpretationConfiguration config = ClinicalInterpretationConfiguration.load(resourceAsStream);
        Assert.assertEquals(9, config.getQueries().size());
        Assert.assertEquals(3, config.getTierConfiguration().getTiers().size());
        Assert.assertEquals("TIER1", config.getTierConfiguration().getTiers().get("tier_1").getLabel());
        Assert.assertEquals("TIER3", config.getTierConfiguration().getTiers().get("tier_3").getLabel());
    }

    @Test
    public void testClinicalInterpretationWithDefaultConfig() throws IOException, CatalogException, ToolException {
        String caId = clinicalTest.CA_OPA;

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

        // Clean up interpretations
        deleteInterpretations(ca);
    }

    @Test
    public void testClinicalInterpretationWithCustomConfig() throws IOException, CatalogException, ToolException, StorageEngineException {
        String caId = clinicalTest.CA_OPA_15914;
        checkClinicalAnalysis(caId);

//        Query query = new Query();
//        query.append("study", "test@CASES:OPA");
//        query.append("filter", "PASS");
//        query.append("unknownGenotype", "./.");
//        query.append("biotype", Arrays.asList("protein_coding","IG_C_gene","IG_D_gene","IG_J_gene","IG_V_gene","nonsense_mediated_decay",
//                "non_stop_decay","TR_C_gene","TR_D_gene","TR_J_gene","TR_V_gene"));
//        query.append("populationFrequencyAlt", Arrays.asList("1000G:AFR<0.002","1000G:AMR<0.002","1000G:EAS<0.002","1000G:EUR<0.002",
//                "1000G:SAS<0.002","GNOMAD_EXOMES:AFR<0.001","GNOMAD_EXOMES:AMR<0.001","GNOMAD_EXOMES:EAS<0.001","GNOMAD_EXOMES:FIN<0.001",
//                "GNOMAD_EXOMES:NFE<0.001","GNOMAD_EXOMES:ASJ<0.001","GNOMAD_EXOMES:OTH<0.002"));
//        query.append("ct", Arrays.asList("transcript_ablation","splice_acceptor_variant","splice_donor_variant","stop_gained",
//                "frameshift_variant","stop_lost","start_lost","transcript_amplification","inframe_insertion","inframe_deletion",
//                "initiator_codon_variant","missense_variant","splice_region_variant","incomplete_terminal_codon_variant"));
////        query.append("familySegregation", "mitochondrial");
////        query.append("family", "111002516");
////        query.append("familyProband", "111002516");
////        query.append("includeSample", Arrays.asList("LP3000474-DNA_A02","LP3000468-DNA_A01","LP3000469-DNA_A06"));
////        query.append("region", new Region("MT", 0, 2147483647));
//        query.append("genotype", "LP3000468-DNA_A01:1,0/1,1/1,0|1,1|0,1|1;LP3000469-DNA_A06:0,0/0,0|0");
//        VariantQueryResult<Variant> variantResult = opencga.getVariantStorageManager().get(query, QueryOptions.empty(), clinicalTest.token);
//        System.out.println("variantResult.getNumResults() = " + variantResult.getNumResults());
//        for (Variant variant : variantResult.getResults()) {
//            System.out.println(variant.toString());
//        }
//        System.exit(-1);


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

        ClinicalInterpretationAnalysisParams params = new ClinicalInterpretationAnalysisParams();
        params.setClinicalAnalysisId(caId);
        params.setConfigFile(opencgaFile.getId());

        toolRunner.execute(ClinicalInterpretationAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, clinicalTest.studyFqn),
                outDir, null, false, clinicalTest.token);

        ca = clinicalTest.catalogManager.getClinicalAnalysisManager().get(clinicalTest.studyFqn, caId, QueryOptions.empty(),
                clinicalTest.token).first();

        assertEquals(1, ca.getSecondaryInterpretations().size());
        assertEquals(13, ca.getSecondaryInterpretations().get(0).getPrimaryFindings().size());

        // Clean up interpretations
        deleteInterpretations(ca);
    }


    private void checkClinicalAnalysis(String caId) throws CatalogException {
        OpenCGAResult<ClinicalAnalysis> caResult = clinicalTest.catalogManager.getClinicalAnalysisManager()
                .get(clinicalTest.studyFqn, caId, QueryOptions.empty(), clinicalTest.token);
        assertEquals(1, caResult.getNumResults());
        assertEquals(0, caResult.first().getSecondaryInterpretations().size());

        ca = caResult.first();
    }

    private void deleteInterpretations(ClinicalAnalysis ca) throws CatalogException {
        // Delete all interpretations
        clinicalTest.catalogManager.getInterpretationManager().delete(clinicalTest.studyFqn, ca.getId(),
                Arrays.asList(ca.getInterpretation().getId(), ca.getSecondaryInterpretations().get(0).getId()), false, clinicalTest.token);

        ca = clinicalTest.catalogManager.getClinicalAnalysisManager().get(clinicalTest.studyFqn,
                clinicalTest.CA_OPA, QueryOptions.empty(), clinicalTest.token).first();

        assertEquals(null, ca.getInterpretation());
        assertEquals(0, ca.getSecondaryInterpretations().size());
    }
}
