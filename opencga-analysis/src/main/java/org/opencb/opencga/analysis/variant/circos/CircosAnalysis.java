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

package org.opencb.opencga.analysis.variant.circos;

import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.CircosAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.CircosAnalysisExecutor;

@Tool(id = CircosAnalysis.ID, resource = Enums.Resource.VARIANT)
public class CircosAnalysis extends OpenCgaTool {

    public static final String ID = "circos";
    public static final String DESCRIPTION = "Generate a Circos plot for a given sample.";

    public final static String SUFFIX_FILENAME = ".genomePlot.png";

    private String study;
    private CircosAnalysisParams circosParams;

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (study == null || study.isEmpty()) {
            throw new ToolException("Missing study");
        }

//        String sampleName = query.getString(SAMPLE.key());
//        try {
//            study = catalogManager.getStudyManager().get(study, null, token).first().getFqn();
//
//            if (StringUtils.isNotEmpty(sampleName)) {
//                OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, sampleName, new QueryOptions(), token);
//                if (sampleResult.getNumResults() != 1) {
//                    throw new ToolException("Unable to compute mutational signature analysis. Sample '" + sampleName + "' not found");
//                }
//            }
//        } catch (CatalogException e) {
//            throw new ToolException(e);
//        }
//
//        addAttribute("sampleName", sampleName);
    }

    @Override
    protected void run() throws ToolException {
        step(getId(), () -> {
            getToolExecutor(CircosAnalysisExecutor.class)
                    .setStudy(study)
                    .setCircosParams(circosParams)
                    .execute();
        });
    }

    public String getStudy() {
        return study;
    }

    public CircosAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    public CircosAnalysisParams getCircosParams() {
        return circosParams;
    }

    public CircosAnalysis setCircosParams(CircosAnalysisParams circosParams) {
        this.circosParams = circosParams;
        return this;
    }
}
