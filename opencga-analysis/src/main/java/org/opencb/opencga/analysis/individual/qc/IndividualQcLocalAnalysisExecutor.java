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

package org.opencb.opencga.analysis.individual.qc;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.commons.collections4.MapUtils;
import org.opencb.biodata.models.clinical.qc.InferredSexReport;
import org.opencb.biodata.models.clinical.qc.MendelianErrorReport;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.mendelianError.MendelianErrorAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.IndividualQcAnalysisExecutor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@ToolExecutor(id = "opencga-local", tool = IndividualQcAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class IndividualQcLocalAnalysisExecutor extends IndividualQcAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        // Sanity check: metrics to update can not be null
        if (qualityControl == null) {
            throw new ToolException("Individual quality control metrics is null");
        }

        switch (qcType) {
            case INFERRED_SEX: {
                runInferredSex();
                break;
            }

            case MENDELIAN_ERRORS: {
                runMendelianErrors();
                break;
            }
        }
    }

    private void runInferredSex() throws ToolException {
        File inferredSexBamFile;
        try {
            inferredSexBamFile = AnalysisUtils.getBamFileBySampleId(sampleId, studyId,
                    getVariantStorageManager().getCatalogManager().getFileManager(), getToken());
        } catch (ToolException e) {
            addWarning("Skipping inferred sex: " + e.getMessage());
            return;
        }

        if (inferredSexBamFile == null) {
            addWarning("Skipping inferred sex: BAM file not found for sample '" + sampleId + "' of individual '" +
                    individual.getId() + "'");
            return;
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
        double[] ratios = InferredSexComputation.computeRatios(studyId, inferredSexBamFile, assembly, alignmentStorageManager, getToken());

        // Infer sex from ratios
        double xAuto = ratios[0];
        double yAuto = ratios[1];
        String inferredKaryotypicSex = "UNKNOWN";
        if (MapUtils.isEmpty(karyotypicSexThresholds)) {
            addWarning("Impossible to infer karyotypic sex beacause sex thresholds are empty");
        } else {
            inferredKaryotypicSex = InferredSexComputation.inferKaryotypicSex(xAuto, yAuto, karyotypicSexThresholds);
        }

        // Set coverage ratio
        Map<String, Object> values = new HashMap<>();
        values.put("ratioX", xAuto);
        values.put("ratioY", yAuto);

        // Set inferred sex report (individual fields will be set later)
        qualityControl.getInferredSexReports().add(new InferredSexReport(sampleId, "CoverageRatio", inferredKaryotypicSex, values,
                Collections.emptyList()));
    }

    private void runMendelianErrors() throws ToolException {
        // Compute mendelian inconsitencies
        try {
            // Get managers
            VariantStorageManager variantStorageManager = getVariantStorageManager();

            MendelianErrorReport mendelianErrorReport = MendelianInconsistenciesComputation.compute(studyId, sampleId, motherSampleId,
                    fatherSampleId, variantStorageManager, getToken());

            JacksonUtils.getDefaultObjectMapper().writer().writeValue(getOutDir().resolve(MendelianErrorAnalysis.ID + ".report.json")
                    .toFile(), mendelianErrorReport);

            // Set relatedness report
            qualityControl.setMendelianErrorReports(Collections.singletonList(mendelianErrorReport));
        } catch (ToolException | IOException e) {
            addWarning("Skipping mendelian errors: " + e.getMessage());
            return;
        }
    }
}
