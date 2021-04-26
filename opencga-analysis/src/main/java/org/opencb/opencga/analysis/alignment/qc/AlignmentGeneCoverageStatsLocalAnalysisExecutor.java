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

package org.opencb.opencga.analysis.alignment.qc;

import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.alignment.AlignmentGeneCoverageStatsAnalysisExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import static org.opencb.opencga.core.api.ParamConstants.LOW_COVERAGE_REGION_THRESHOLD_DEFAULT;

@ToolExecutor(id="opencga-local", tool = AlignmentGeneCoverageStatsAnalysis.ID, framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class AlignmentGeneCoverageStatsLocalAnalysisExecutor extends AlignmentGeneCoverageStatsAnalysisExecutor
        implements StorageToolExecutor {

    public final static String ID = AlignmentGeneCoverageStatsAnalysis.ID + "-local";

    @Override
    public void run() throws ToolException {
        try {
            OpenCGAResult<GeneCoverageStats> geneCoverageStatsResult = getAlignmentStorageManager().coverageStats(getStudyId(),
                    getBamFileId(), getGenes(), Integer.parseInt(LOW_COVERAGE_REGION_THRESHOLD_DEFAULT), getToken());

            geneCoverageStats = geneCoverageStatsResult.getResults();
        } catch (Exception e) {
            throw new ToolException(e);
        }
    }
}
