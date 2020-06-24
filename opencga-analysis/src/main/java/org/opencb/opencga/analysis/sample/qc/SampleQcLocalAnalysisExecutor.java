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

package org.opencb.opencga.analysis.sample.qc;

import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.individual.qc.IBDComputation;
import org.opencb.opencga.analysis.individual.qc.InferredSexComputation;
import org.opencb.opencga.analysis.individual.qc.MendelianInconsistenciesComputation;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.models.variant.MendelianErrorReport;
import org.opencb.opencga.core.models.sample.RelatednessReport;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.SampleQcAnalysisExecutor;

import java.util.*;

@ToolExecutor(id="opencga-local", tool = SampleQcAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class SampleQcLocalAnalysisExecutor extends SampleQcAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        switch (getQc()) {

            case FASTQC: {

//                // Get managers
//                FastqcWrapperAnalysis fastqc = new FastqcWrapperAnalysis();
//
//                AlignmentCommandOptions.FastqcCommandOptions cliOptions = alignmentCommandOptions.fastqcCommandOptions;
//                ObjectMap params = new ObjectMap();
//                params.putAll(cliOptions.commonOptions.params);
//
//                FastqcWrapperAnalysis fastqc = new FastqcWrapperAnalysis();
//
//                fastqc.setUp(appHome, getVariantStorageManager().getCatalogManager(), storageEngineFactory, params, getOutDir()), getToken());
//
//                fastqc.setStudy(getStudyId());
//
//                fastqc.setFile();
//
//                fastqc.start();
//
//
//                ExecutionResult fastqcResult = fastqc.start();
//
//                VariantStorageManager variantStorageManager = getVariantStorageManager();
//                CatalogManager catalogManager = variantStorageManager.getCatalogManager();
//
//                // Compute mendelian inconsitencies
//                MendelianErrorReport mendelianErrorReport = MendelianInconsistenciesComputation.compute(getStudyId(), getFamilyId(),
//                        variantStorageManager, getToken());
//
//                // Set relatedness report
//                getReport().setMendelianErrorReport(mendelianErrorReport);
                break;
            }

            default: {
                throw new ToolException("Unknown genetic check: " + getQc());
            }
        }
    }
}
