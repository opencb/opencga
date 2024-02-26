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
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclUpdateParams;
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
import org.opencb.opencga.core.tools.result.ExecutionResultManager;
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
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(STUDY_PARAM_NAME, ALL_STUDIES_VALUE);
        query.put(CA_ID_NAME, caId);
        DataResult<ClinicalAnalysis> result = cvdbEngine.searchClinicalAnalyses(query, QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals(caId, result.first().getId());
        return result.first();
    }

    public static Interpretation getClinicalInterpretation(String ciId, String projectId, CvdbSolrEngine cvdbEngine, String userToken)
            throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(STUDY_PARAM_NAME, ALL_STUDIES_VALUE);
        query.put(CI_ID_NAME, ciId);
        DataResult<Interpretation> result = cvdbEngine.searchClinicalInterpretations(query, QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals(ciId, result.first().getId());
        return result.first();
    }

    public static ClinicalVariant getClinicalVariant(String cvId, String projectId, CvdbSolrEngine cvdbEngine, String userToken)
            throws IOException, CvdbException, CatalogException {
        Query query = new Query(PROJECT_PARAM_NAME, projectId);
        query.put(STUDY_PARAM_NAME, ALL_STUDIES_VALUE);
        query.put(CV_ID_NAME, cvId);
        DataResult<ClinicalVariant> result = cvdbEngine.searchClinicalVariants(query, QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals(cvId, result.first().getId());
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
}
