/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.analysis.clinical;

public class InterpretationAnalysisConfiguration {
    private int maxLowCoverage;
    private boolean includeLowCoverage;
    private boolean skipUntieredVariants;

    public int getMaxLowCoverage() {
        return maxLowCoverage;
    }

    public InterpretationAnalysisConfiguration setMaxLowCoverage(int maxLowCoverage) {
        this.maxLowCoverage = maxLowCoverage;
        return this;
    }

    public boolean isIncludeLowCoverage() {
        return includeLowCoverage;
    }

    public InterpretationAnalysisConfiguration setIncludeLowCoverage(boolean includeLowCoverage) {
        this.includeLowCoverage = includeLowCoverage;
        return this;
    }

    public boolean isSkipUntieredVariants() {
        return skipUntieredVariants;
    }

    public InterpretationAnalysisConfiguration setSkipUntieredVariants(boolean skipUntieredVariants) {
        this.skipUntieredVariants = skipUntieredVariants;
        return this;
    }
}
