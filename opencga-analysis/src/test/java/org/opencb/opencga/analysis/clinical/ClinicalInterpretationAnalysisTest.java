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

package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationConfiguration;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationConfiguration;
import org.opencb.opencga.analysis.clinical.zetta.ZettaInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.zetta.ZettaInterpretationConfiguration;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.InterpretationUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.fail;
import static org.opencb.opencga.core.api.ParamConstants.INCLUDE_INTERPRETATION;

public class ClinicalInterpretationAnalysisTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {


    private AbstractClinicalManagerTest clinicalTest;

    Path outDir;


    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    @Before
    public void setUp() throws Exception {
        clearDB("opencga_test_user_1000G");
        clinicalTest = ClinicalAnalysisUtilsTest.getClinicalTest(catalogManagerResource, getVariantStorageEngine());
    }

    @Test
    public void tieringAnalysis() throws Exception {
        outDir = Paths.get(opencga.createTmpOutdir("_interpretation_analysis"));

        TieringInterpretationConfiguration config = new TieringInterpretationConfiguration();
        TieringInterpretationAnalysis tieringAnalysis = new TieringInterpretationAnalysis();
        tieringAnalysis.setUp(catalogManagerResource.getOpencgaHome().toString(), new ObjectMap(), outDir, clinicalTest.token);
        tieringAnalysis.setStudyId(clinicalTest.studyFqn)
                .setClinicalAnalysisId(clinicalTest.clinicalAnalysis.getId())
                .setPenetrance(ClinicalProperty.Penetrance.COMPLETE)
                .setConfig(config);

        ExecutionResult result = tieringAnalysis.start();

        System.out.println(result);

        checkInterpretation(0, result);
    }

    @Test
    public void teamAnalysis() throws IOException {
        outDir = Paths.get(opencga.createTmpOutdir("_interpretation_analysis"));

        try {
            TeamInterpretationConfiguration config = new TeamInterpretationConfiguration();
            TeamInterpretationAnalysis teamAnalysis = new TeamInterpretationAnalysis();
            teamAnalysis.setUp(catalogManagerResource.getOpencgaHome().toString(), new ObjectMap(), outDir, clinicalTest.token);
            teamAnalysis.setStudyId(clinicalTest.studyFqn)
                    .setClinicalAnalysisId(clinicalTest.clinicalAnalysis.getId())
                    .setConfig(config);

            teamAnalysis.start();

            fail();
        } catch (ToolException e) {
            Assert.assertEquals(e.getMessage(), "Missing disease panels for TEAM interpretation analysis");
        }
    }

    @Test
    public void customAnalysisFromClinicalAnalysisTest() throws Exception {
        outDir = Paths.get(opencga.createTmpOutdir("_interpretation_analysis"));

        //  for (Variant variant : variantStorageManager.iterable(clinicalTest.token)) {
//            System.out.println("variant = " + variant.toStringSimple());// + ", ALL:maf = " + variant.getStudies().get(0).getStats("ALL").getMaf());
//        }
        Query query = new Query();
        query.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");

        ZettaInterpretationConfiguration config = new ZettaInterpretationConfiguration();
        ZettaInterpretationAnalysis customAnalysis = new ZettaInterpretationAnalysis();
        customAnalysis.setUp(catalogManagerResource.getOpencgaHome().toString(), new ObjectMap(), outDir, clinicalTest.token);
        customAnalysis.setStudyId(clinicalTest.studyFqn)
                .setClinicalAnalysisId(clinicalTest.clinicalAnalysis.getId())
                .setConfig(config);

        ExecutionResult result = customAnalysis.start();
        System.out.println(result);

        checkInterpretation(18, result);
    }

    @Test
    public void customAnalysisFromSamplesTest() throws Exception {
        outDir = Paths.get(opencga.createTmpOutdir("_interpretation_analysis"));

        //        for (Variant variant : variantStorageManager.iterable(clinicalTest.token)) {
//            System.out.println("variant = " + variant.toStringSimple());// + ", ALL:maf = " + variant.getStudies().get(0).getStats("ALL").getMaf());
//        }
//        ObjectMap options = new ObjectMap();
//        String param = FamilyInterpretationAnalysis.SKIP_UNTIERED_VARIANTS_PARAM;
//        options.put(param, false);

        Query query = new Query();
//        List<String> samples = new ArrayList();
//        for (Individual member : clinicalTest.clinicalAnalysis.getFamily().getMembers()) {
//            if (CollectionUtils.isNotEmpty(member.getSamples())) {
//                samples.add(member.getSamples().get(0).getId());
//            }
//        }
//        query.put(VariantQueryParam.SAMPLE.key(), samples);
        query.put(VariantQueryParam.SAMPLE.key(), "s3");

        ZettaInterpretationConfiguration config = new ZettaInterpretationConfiguration();
        ZettaInterpretationAnalysis customAnalysis = new ZettaInterpretationAnalysis();
        customAnalysis.setUp(catalogManagerResource.getOpencgaHome().toString(), new ObjectMap(), outDir, clinicalTest.token);
        customAnalysis.setStudyId(clinicalTest.studyFqn)
                .setClinicalAnalysisId(clinicalTest.clinicalAnalysis.getId())
                .setQuery(query)
                .setConfig(config);

        ExecutionResult result = customAnalysis.start();

        System.out.println(result);

        checkInterpretation(12, result);
    }

    @Test
    public void testClinicalVariantQuery() throws Exception {
        String study = "1000G:phase1";
        String clinicalAnalysisId = "clinical-analysis-1";
        String interpretationId = "clinical-analysis-1.1";
        StorageEngineFactory engineFactory = StorageEngineFactory.get(getVariantStorageEngine().getConfiguration());

        ClinicalInterpretationManager manager = new ClinicalInterpretationManager(clinicalTest.catalogManager, engineFactory,
                opencga.getOpencgaHome());

        // Add new finding
        OpenCGAResult<org.opencb.opencga.core.models.clinical.Interpretation> interpretationResult = clinicalTest.catalogManager
                .getInterpretationManager().get(study, interpretationId, QueryOptions.empty(), clinicalTest.token);
        org.opencb.opencga.core.models.clinical.Interpretation interpretation = interpretationResult.first();
        List<ClinicalVariant> findingList = new ArrayList<>();
//        VariantAvro variant = new VariantAvro("1:1456330:C:A", null, "1", 1456330, 1456330, "C", "A", "+", null, 1, null, null, null);
        VariantAvro variant = new VariantAvro("rs1212112", null, "1", 1456330, 1456330, "C", "A", "+", null, 1, null, null, null);
        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence().setInterpretationMethodName("method2");
        ClinicalVariant cv3 = new ClinicalVariant(variant, Collections.singletonList(evidence), null, null,
                new ClinicalDiscussion(null, null, "helllooooo"), null, ClinicalVariant.Status.REVIEWED, Collections.emptyList(), null);
        findingList.add(cv3);
        InterpretationUpdateParams updateParams = new InterpretationUpdateParams().setPrimaryFindings(findingList);
        ObjectMap actionMap = new ObjectMap(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
        clinicalTest.catalogManager.getInterpretationManager().update(study, clinicalAnalysisId, interpretationId, updateParams, null,
                options, clinicalTest.token);

        Query query = new Query();
        query.put("study", study);
        query.put(INCLUDE_INTERPRETATION, interpretationId);
        query.put("type", "SNV");
        OpenCGAResult<ClinicalVariant> result = manager.get(query, QueryOptions.empty(), clinicalTest.token);
        boolean success = false;
        for (ClinicalVariant cv : result.getResults()) {
            if (cv3.toStringSimple().equals(cv.toStringSimple())
                    && cv3.getStatus() == cv.getStatus()
                    && cv3.getDiscussion().equals(cv.getDiscussion())) {
                System.out.println(cv.getId() + ", " + cv.toStringSimple() + ", " + cv.getDiscussion() + ", " + cv.getStatus());
                success = true;
            }
        }
        if (!success) {
            fail();
        }
    }

    private void checkInterpretation(int expected, ExecutionResult result) {
        System.out.println("out dir (to absolute path) = " + outDir.toAbsolutePath());

        String msg = "Success";

        Interpretation interpretation = null;
        try {
            interpretation = readInterpretation(result, outDir);
            System.out.println("Interpreation ID: " + interpretation.getId());
            System.out.println("# primary findings: " + interpretation.getPrimaryFindings().size());
        } catch (ToolException e) {
            if (CollectionUtils.isNotEmpty(result.getEvents())) {
                System.out.println(StringUtils.join(result.getEvents(), "\n"));
            }
            Assert.fail();
        }

        Assert.assertEquals(expected, interpretation.getPrimaryFindings().size());
    }

    private Interpretation readInterpretation(ExecutionResult result, Path outDir)
            throws ToolException {
        File file = new java.io.File(outDir + "/" + InterpretationAnalysis.INTERPRETATION_FILENAME);
        if (file.exists()) {
            return ClinicalUtils.readInterpretation(file.toPath());
        }
        String msg = "Interpretation file not found for " + result.getId() + " analysis";
        if (CollectionUtils.isNotEmpty(result.getEvents())) {
            msg += (": " + StringUtils.join(result.getEvents(), ". "));
        }
        throw new ToolException(msg);
    }

}
