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

package org.opencb.opencga.analysis.variant.inferredSex;

import org.apache.commons.collections4.MapUtils;
import org.opencb.biodata.models.clinical.qc.InferredSexReport;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.individual.qc.IndividualQcAnalysis;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.individual.qc.InferredSexComputation;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.opencb.opencga.core.tools.variant.IndividualQcAnalysisExecutor.COVERAGE_RATIO_INFERRED_SEX_METHOD;

@ToolExecutor(id = "opencga-local", tool = InferredSexAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class InferredSexLocalAnalysisExecutor extends InferredSexAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        File bwFile = AnalysisUtils.getBwFileBySampleId(sampleId, getStudyId(), getVariantStorageManager().getCatalogManager().getFileManager(),
                    getToken());

        if (bwFile == null) {
            throw new ToolException("BIGWIG file not found for sample '" + sampleId + "' of individual '" + individualId + "'");
        }

        // Get managers
        AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();

        // Get assembly
        String assembly;
        try {
            assembly = IndividualQcUtils.getAssembly(getStudyId(), alignmentStorageManager.getCatalogManager(), getToken());
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Infer the sex for that sample
        // Compute ratios: X-chrom / autosomic-chroms and Y-chrom / autosomic-chroms
        double[] ratios = InferredSexComputation.computeRatios(studyId, bwFile, assembly, alignmentStorageManager, getToken());

        // Infer sex from ratios
        double xAuto = ratios[0];
        double yAuto = ratios[1];

        Map<String, Double> karyotypicSexThresholds = new HashMap<>();
        String opencgaHome = getExecutorParams().getString(ParamConstants.OPENCGA_HOME);
        Path thresholdsPath = Paths.get(opencgaHome).resolve("analysis").resolve(IndividualQcAnalysis.ID)
                .resolve("karyotypic_sex_thresholds.json");
        try {
            karyotypicSexThresholds = JacksonUtils.getDefaultNonNullObjectMapper().readerFor(Map.class).readValue(thresholdsPath.toFile());
        } catch (IOException e) {
            throw new ToolException("Skipping inferring karyotypic sex: something wrong happened when loading the karyotypic sex"
                    + " thresholds file: " + thresholdsPath);
        }
        if (MapUtils.isEmpty(karyotypicSexThresholds)) {
            throw new ToolException("Impossible to infer karyotypic sex beacause sex thresholds are empty: " + thresholdsPath);
        }
        String inferredKaryotypicSex = InferredSexComputation.inferKaryotypicSex(xAuto, yAuto, karyotypicSexThresholds);

        // Set coverage ratio
        Map<String, Object> values = new HashMap<>();
        values.put("ratioX", xAuto);
        values.put("ratioY", yAuto);

        // Set inferred sex report (individual fields will be set later)
        inferredSexReport = new InferredSexReport(sampleId, COVERAGE_RATIO_INFERRED_SEX_METHOD, inferredKaryotypicSex, values,
                Collections.singletonList(bwFile.getId()));
    }
}
