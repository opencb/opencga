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

import org.opencb.biodata.models.clinical.qc.individual.InferredSexReport;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.individual.qc.InferredSexComputation;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.InferredSexAnalysisExecutor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@ToolExecutor(id="opencga-local", tool = InferredSexAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class InferredSexLocalAnalysisExecutor extends InferredSexAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();
        CatalogManager catalogManager = alignmentStorageManager.getCatalogManager();
        FileManager fileManager = catalogManager.getFileManager();
        String assembly;
        try {
            assembly = IndividualQcUtils.getAssembly(getStudyId(), alignmentStorageManager.getCatalogManager(), getToken());
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // Compute ratios: X-chrom / autosomic-chroms and Y-chrom / autosomic-chroms
        double[] ratios = InferredSexComputation.computeRatios(getStudyId(), getIndividualId(), assembly, fileManager, alignmentStorageManager,
                getToken());

        // TODO infer sex from ratios
        String inferredKaryotypicSex = "";

        Map<String, Object> values = new HashMap<>();
        values.put("ratioX", ratios[0]);
        values.put("ratioY", ratios[1]);

        // Set inferred sex report (individual fields will be set later)
        setInferredSexReport(new InferredSexReport("CoverageRatio", inferredKaryotypicSex, values, Collections.emptyList()));
    }
}
