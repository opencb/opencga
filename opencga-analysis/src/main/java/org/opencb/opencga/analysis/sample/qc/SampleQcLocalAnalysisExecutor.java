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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.FastqcWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.variant.InferredSexReport;
import org.opencb.opencga.core.models.variant.MendelianErrorReport;
import org.opencb.opencga.core.models.sample.RelatednessReport;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.variant.SampleQcAnalysisExecutor;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@ToolExecutor(id="opencga-local", tool = SampleQcAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class SampleQcLocalAnalysisExecutor extends SampleQcAnalysisExecutor implements StorageToolExecutor {

    @Override
    public void run() throws ToolException {
        switch (getQc()) {

            case INFERRED_SEX: {

                // Get managers
                AlignmentStorageManager alignmentStorageManager = getAlignmentStorageManager();
                CatalogManager catalogManager = alignmentStorageManager.getCatalogManager();
                FileManager fileManager = catalogManager.getFileManager();

                // Get assembly
                String assembly;
                try {
                    assembly = SampleQcUtils.getAssembly(getStudyId(), alignmentStorageManager.getCatalogManager(), getToken());
                } catch (CatalogException e) {
                    throw new ToolException(e);
                }

                // Infer the sex for each individual
                List<InferredSexReport> sexReportList = new ArrayList<>();
                for (String sampleId : getSampleIds()) {
                    // Compute ratios: X-chrom / autosomic-chroms and Y-chrom / autosomic-chroms
                    double[] ratios = InferredSexComputation.computeRatios(getStudyId(), sampleId, assembly, fileManager,
                            alignmentStorageManager, getToken());

                    // Add sex report to the list (individual fields will be set later)
                    // TODO infer sex from ratios
                    sexReportList.add(new InferredSexReport("", sampleId, "", "", ratios[0], ratios[1], ""));
                }

                // Set sex report
                getReport().setInferredSexReport(sexReportList);
                break;
            }

            case RELATEDNESS: {

                // Get managers
                VariantStorageManager variantStorageManager = getVariantStorageManager();
                CatalogManager catalogManager = variantStorageManager.getCatalogManager();

                // Run IBD/IBS computation using PLINK in docker
                RelatednessReport relatednessReport = IBDComputation.compute(getStudyId(), getSampleIds(), getMinorAlleleFreq(),
                        getOutDir(), variantStorageManager, getToken());

                // Set relatedness report
                getReport().setRelatednessReport(relatednessReport);
                break;
            }

            case MENDELIAN_ERRORS: {

                // Get managers
                VariantStorageManager variantStorageManager = getVariantStorageManager();
                CatalogManager catalogManager = variantStorageManager.getCatalogManager();

                // Compute mendelian inconsitencies
                MendelianErrorReport mendelianErrorReport = MendelianInconsistenciesComputation.compute(getStudyId(), getFamilyId(),
                        variantStorageManager, getToken());

                // Set relatedness report
                getReport().setMendelianErrorReport(mendelianErrorReport);
                break;
            }

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
