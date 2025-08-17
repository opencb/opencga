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

package org.opencb.opencga.analysis.wrappers.deseq2;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2Params;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2WrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.checkPath;

@Tool(id = DESeq2WrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = DESeq2WrapperAnalysis.DESCRIPTION)
public class DESeq2WrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "deseq2";
    public final static String DESCRIPTION = "DESeq2 is a widely used Bioconductor R package for differential gene expression analysis"
            + " of high-throughput sequencing data (primarily RNA-seq).";

    public final static String WALD_TEST_METHOD = "Wald";
    public final static String LRT_TEST_METHOD = "LRT";

    public final static String RESULTS_FILE_SUFIX = ".csv";
    public final static String PCA_PLOT_RESULTS_FILE_SUFIX = "_pca_plot.png";
    public final static String MA_PLOT_RESULTS_FILE_SUFIX = "_ma_plot.png";
    public final static String VST_COUNTS_RESULTS_FILE_SUFIX = "_transformed_counts.csv";

    @ToolParams
    protected final DESeq2WrapperParams analysisParams = new DESeq2WrapperParams();

    private DESeq2Params updatedParams = new DESeq2Params();

    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check parameters, and get physical paths from OpenCGA catalog files before passing them to the executor

        // Check input
        if (StringUtils.isEmpty(analysisParams.getDESeq2Params2Params().getInput().getCountsFile())) {
            throw new ToolException("Missing input counts file. Please, specify the OpenCGA file ID to the input counts file.");
        }
        String path = checkPath(analysisParams.getDESeq2Params2Params().getInput().getCountsFile(), study, catalogManager, token);
        updatedParams.getInput().setCountsFile(path);

        if (StringUtils.isEmpty(analysisParams.getDESeq2Params2Params().getInput().getMetadataFile())) {
            throw new ToolException("Missing input metadata file. Please, specify the OpenCGA file ID to the input metadata file.");
        }
        path = checkPath(analysisParams.getDESeq2Params2Params().getInput().getMetadataFile(), study, catalogManager, token);
        updatedParams.getInput().setMetadataFile(path);

        // Check DESeq2 parameters
        if (analysisParams.getDESeq2Params2Params().getAnalysis() == null) {
            throw new ToolException("Missing analysis parameters. Please, specify the DESeq2 analysis parameters.");
        }
        updatedParams.setAnalysis(analysisParams.getDESeq2Params2Params().getAnalysis());
        String testMethod = updatedParams.getAnalysis().getTestMethod();
        if (StringUtils.isEmpty(testMethod)) {
            updatedParams.getAnalysis().setTestMethod(WALD_TEST_METHOD);
        } else if (!testMethod.equalsIgnoreCase(WALD_TEST_METHOD) && !testMethod.equalsIgnoreCase(LRT_TEST_METHOD)) {
            throw new ToolException("Invalid test method '" + testMethod + "'. Expected '" + WALD_TEST_METHOD + "' or '"
                    + LRT_TEST_METHOD + "'.");
        }

        // Output
        if (StringUtils.isEmpty(analysisParams.getDESeq2Params2Params().getOutput().getBasename())) {
            throw new ToolException("Missing output basename. Please, specify the output basename for the DESeq2 results.");
        }
        updatedParams.setOutput(analysisParams.getDESeq2Params2Params().getOutput());
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(ID);
    }

    protected void run() throws ToolException, IOException {
        // Run DESeq2
        step(ID, this::runDESeq2);
    }

    protected void runDESeq2() throws ToolException {
        // Get executor
        DESeq2WrapperAnalysisExecutor executor = getToolExecutor(DESeq2WrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setDESeq2Params(updatedParams)
                .execute();
    }
}
