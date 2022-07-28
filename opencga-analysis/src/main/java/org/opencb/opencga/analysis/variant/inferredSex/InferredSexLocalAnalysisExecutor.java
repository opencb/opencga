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
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
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

@ToolExecutor(id = "opencga-local", tool = InferredSexAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class InferredSexLocalAnalysisExecutor extends InferredSexAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        // IMPORTANT: we assume sample and individual have the same ID
        AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();
        CatalogManager catalogManager = alignmentStorageManager.getCatalogManager();
        FileManager fileManager = catalogManager.getFileManager();
        String assembly;

        // Get alignment file by individual
        File inferredSexBamFile = AnalysisUtils.getBamFileBySampleId(getIndividualId(), getStudyId(), fileManager, getToken());
        if (inferredSexBamFile == null) {
            throw new ToolException("Alignment file not found for the individual/sample '" + getIndividualId() + "'");
        }

        // Ge assembly
        try {
            assembly = IndividualQcUtils.getAssembly(getStudyId(), alignmentStorageManager.getCatalogManager(), getToken());
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Compute ratios: X-chrom / autosomic-chroms and Y-chrom / autosomic-chroms
        double[] ratios = InferredSexComputation.computeRatios(getStudyId(), inferredSexBamFile, assembly, alignmentStorageManager,
                getToken());

        double xAuto = ratios[0];
        double yAuto = ratios[1];

        // Read the karyotypic sex tyhresholds
        String inferredKaryotypicSex = "UNKNOWN";
        Map<String, Double> karyotypicSexThresholds = new HashMap<>();
        try {
            String opencgaHome = getExecutorParams().getString("opencgaHome");
            Path thresholdsPath = Paths.get(opencgaHome).resolve("analysis").resolve(IndividualQcAnalysis.ID)
                    .resolve("karyotypic_sex_thresholds.json");
            karyotypicSexThresholds = JacksonUtils.getDefaultNonNullObjectMapper().readerFor(Map.class).readValue(thresholdsPath.toFile());
        } catch (IOException e) {
            addWarning("Skipping inferring karyotypic sex: something wrong happened when loading the karyotypic sex thresholds file"
                    + " (karyotypic_sex_thresholds.json)");
        }
        if (MapUtils.isNotEmpty(karyotypicSexThresholds)) {
            inferredKaryotypicSex = InferredSexComputation.inferKaryotypicSex(xAuto, yAuto, karyotypicSexThresholds);
        }

        // Set inferred sex report
        Map<String, Object> values = new HashMap<>();
        values.put("ratioX", xAuto);
        values.put("ratioY", yAuto);

        setInferredSexReport(new InferredSexReport(getIndividualId(), "CoverageRatio", inferredKaryotypicSex, values,
                Collections.singletonList(inferredSexBamFile.getName())));
    }
}
