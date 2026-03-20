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

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.io.FileUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclUpdateParams;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.PROJECT_PARAM;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
import static org.junit.Assert.assertEquals;

public class TestUtilities {

    public static void checkExecutionResult(ExecutionResult er, String storageEngine) {checkExecutionResult(er, storageEngine, true);
    }

    public static void checkExecutionResult(ExecutionResult er, String storageEngine, boolean customExecutor) {
        if (customExecutor) {
            if (storageEngine.equals("hadoop")) {
                assertEquals("hbase-mapreduce", er.getExecutor().getId());
            } else {
                assertEquals("mongodb-local", er.getExecutor().getId());
            }
        } else {
            assertEquals("opencga-local", er.getExecutor().getId());
        }
    }

    public static java.io.File getOutputFile(Path outDir) {
        return FileUtils.listFiles(outDir.toFile(), null, false)
                .stream()
                .filter(f -> !ExecutionResultManager.isExecutionResultFile(f.getName()))
                .findFirst().orElse(null);
    }

    public static ClinicalAnalysis getClinicalAnalyis(String caId, String projectId, CvdbSolrEngine cvdbEngine, String userToken)
            throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM, projectId);
        query.put(CA_ID_NAME, caId);
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals(caId, result.first().getId());
        return result.first();
    }

    public static Interpretation getClinicalInterpretation(String ciId, String projectId, CvdbSolrEngine cvdbEngine, String userToken)
            throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM, projectId);
        query.put(CI_ID_NAME, ciId);
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals(ciId, result.first().getId());
        return result.first();
    }

    public static ClinicalVariant getClinicalVariant(String variantId, String projectId, CvdbSolrEngine cvdbEngine, String userToken)
            throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM, projectId);
        query.put(CV_VARIANT_ID_NAME, variantId);
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals(variantId, result.first().getId());
        return result.first();
    }

    public static boolean existsVariantId(String variantId, ClinicalAnalysis clinicalAnalysis) {
        if (existsVariantId(variantId, clinicalAnalysis.getInterpretation())) {
            return true;
        }
        for (Interpretation interpretation : clinicalAnalysis.getSecondaryInterpretations()) {
            if (existsVariantId(variantId, interpretation)) {
                return true;
            }
        }
        return false;
    }

    public static boolean existsVariantId(String variantId, Interpretation interpretation) {
        if (existsVariantId(variantId, interpretation.getPrimaryFindings())) {
            return true;
        }
        if (existsVariantId(variantId, interpretation.getSecondaryFindings())) {
            return true;
        }
        return false;
    }

    public static boolean existsVariantId(String variantId, List<ClinicalVariant> clinicalVariants) {
        for (ClinicalVariant clinicaVariant : clinicalVariants) {
            if (clinicaVariant.toString().equals(variantId)) {
                return true;
            }
        }
        return false;
    }

    public static void loadClinicalAnalsysesInCatalog(List<String> caFilenames, Study study, String userToken, String adminToken,
                                                      CatalogManager catalogManager) throws IOException, CatalogException {
        for (String caFilename : caFilenames) {
            URL resource = ClinicalInterpretationConverterTest.class.getClassLoader().getResource(caFilename);
            ClinicalAnalysisLoadResult loadResult = catalogManager.getClinicalAnalysisManager().load(study.getFqn(),
                    Paths.get(resource.getPath()), userToken);
            System.out.println(loadResult);
        }
        OpenCGAResult<ClinicalAnalysis> results = catalogManager.getClinicalAnalysisManager().search(study.getFqn(), new Query(),
                QueryOptions.empty(), adminToken);
        for (ClinicalAnalysis clinicalAnalysis : results.getResults()) {
            catalogManager.getClinicalAnalysisManager().updateAcl(study.getFqn(), Collections.singletonList(clinicalAnalysis.getId()),
                    "user", new ClinicalAnalysisAclUpdateParams(null, "VIEW"), ParamUtils.AclAction.SET, false, adminToken);
        }
    }

    public static void checkClinicalAnalysisIndexStatus(String indexStatus, Study study, CatalogManager catalogManager, String userToken) throws CatalogException {
        OpenCGAResult<ClinicalAnalysis> caResult = catalogManager.getClinicalAnalysisManager().search(study.getId(), new Query(), QueryOptions.empty(), userToken);
        for (ClinicalAnalysis ca : caResult.getResults()) {
            assertEquals(indexStatus, ca.getInternal().getCvdbIndex().getId());
            System.out.println("ca.getInternal().getCvdbIndex().getId() = " + ca.getInternal().getCvdbIndex().getId());
        }
    }
}
