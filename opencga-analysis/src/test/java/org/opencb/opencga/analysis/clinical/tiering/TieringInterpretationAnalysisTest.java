package org.opencb.opencga.analysis.clinical.tiering;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.tools.clinical.tiering.TieringConfiguration;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisUtilsTest;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.tiering.TieringInterpretationAnalysisParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.fasterxml.jackson.databind.type.LogicalType.Map;
import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest.TIERING_MODE;
import static org.opencb.opencga.core.tools.ResourceManager.ANALYSIS_DIRNAME;

@Deprecated
@Category(MediumTests.class)
public class TieringInterpretationAnalysisTest {

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
    public void tieringAnalysisDefaultConfig() throws IOException, CatalogException, ToolException {
        checkClinicalAnalysis();

        outDir = Paths.get(opencga.createTmpOutdir("_tiering_default_config"));
        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());
        System.out.println("outDir = " + outDir);

        TieringInterpretationAnalysisParams params = new TieringInterpretationAnalysisParams();
        params.setClinicalAnalysisId(ca.getId());

        toolRunner.execute(TieringInterpretationAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, clinicalTest.studyFqn),
                outDir, null, false, clinicalTest.token);

        ca = clinicalTest.catalogManager.getClinicalAnalysisManager().get(clinicalTest.studyFqn,
                clinicalTest.CA_OPA, QueryOptions.empty(), clinicalTest.token).first();

        assertEquals(1, ca.getSecondaryInterpretations().size());
        assertEquals(0, ca.getSecondaryInterpretations().get(0).getPrimaryFindings().size());

        // Clean up interpretations
        deleteInterpretations();
    }

    @Test
    public void tieringAnalysisCustomConfig() throws IOException, CatalogException, ToolException {
        checkClinicalAnalysis();

        outDir = Paths.get(opencga.createTmpOutdir("_tiering_custom_config"));
        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());
        System.out.println("outDir = " + outDir);

        // Read tiering configuration and register in catalog
        Path configPath = opencga.getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve("tiering").resolve("tiering-configuration.yml");
        TieringConfiguration tieringConfiguration = JacksonUtils.getDefaultObjectMapper().convertValue(new Yaml().load(Files.newInputStream(configPath)), TieringConfiguration.class);
        for (String key: tieringConfiguration.getQueries().keySet()) {
            Map<String, Object> filters = (Map<String, Object>) tieringConfiguration.getQueries().get(key);
            filters.remove("cohortStatsMaf");
        }
        Path updatedConfigPath = Paths.get(opencga.createTmpOutdir("_tiering_custom_config_data")).resolve(configPath.getFileName());
        JacksonUtils.getDefaultObjectMapper().writerFor(TieringConfiguration.class).writeValue(updatedConfigPath.toFile(), tieringConfiguration);
        InputStream inputStream = Files.newInputStream(updatedConfigPath);
        File opencgaFile = opencga.getCatalogManager().getFileManager().upload(clinicalTest.studyFqn, inputStream,
                new File().setPath("data/" + updatedConfigPath.getFileName()), false, true, false, clinicalTest.token).first();

        TieringInterpretationAnalysisParams params = new TieringInterpretationAnalysisParams();
        params.setClinicalAnalysisId(ca.getId());
        params.setConfigFile(opencgaFile.getId());

        toolRunner.execute(TieringInterpretationAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, clinicalTest.studyFqn),
                outDir, null, false, clinicalTest.token);

        ca = clinicalTest.catalogManager.getClinicalAnalysisManager().get(clinicalTest.studyFqn,
                clinicalTest.CA_OPA, QueryOptions.empty(), clinicalTest.token).first();

        assertEquals(1, ca.getSecondaryInterpretations().size());
        assertEquals(10, ca.getSecondaryInterpretations().get(0).getPrimaryFindings().size());

        // Clean up interpretations
        deleteInterpretations();
    }


    private void checkClinicalAnalysis() throws CatalogException {
        OpenCGAResult<ClinicalAnalysis> caResult = clinicalTest.catalogManager.getClinicalAnalysisManager()
                .get(clinicalTest.studyFqn, clinicalTest.CA_OPA, QueryOptions.empty(), clinicalTest.token);
        assertEquals(1, caResult.getNumResults());
        assertEquals(0, caResult.first().getSecondaryInterpretations().size());

        ca = caResult.first();
    }

    private void deleteInterpretations() throws CatalogException {
        // Delete all interpretations
        clinicalTest.catalogManager.getInterpretationManager().delete(clinicalTest.studyFqn, ca.getId(),
                Arrays.asList(ca.getInterpretation().getId(), ca.getSecondaryInterpretations().get(0).getId()), false, clinicalTest.token);

        ca = clinicalTest.catalogManager.getClinicalAnalysisManager().get(clinicalTest.studyFqn,
                clinicalTest.CA_OPA, QueryOptions.empty(), clinicalTest.token).first();

        assertEquals(null, ca.getInterpretation());
        assertEquals(0, ca.getSecondaryInterpretations().size());
    }
}
