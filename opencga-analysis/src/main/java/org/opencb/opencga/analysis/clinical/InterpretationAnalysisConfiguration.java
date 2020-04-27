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

package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InterpretationAnalysisConfiguration {

    private Tier tier1;
    private Tier tier2;
    private Tier tier3;

    private int maxLowCoverage;
    private boolean includeLowCoverage;
    private boolean skipUntieredVariants;

    public class Tier {
        private List<String> consequenceTypes;
        private Set<String> consequenceTypeSet;
        private boolean panels;
        private boolean actionable;

        public Tier() {
        }

        public List<String> getConsequenceTypes() {
            return consequenceTypes;
        }

        public Tier setConsequenceTypes(List<String> consequenceTypes) {
            this.consequenceTypes = consequenceTypes;
            return this;
        }

        public Set<String> getConsequenceTypeSet() {
            // Lazy
            if (CollectionUtils.isEmpty(consequenceTypeSet) && CollectionUtils.isNotEmpty(consequenceTypes)) {
                consequenceTypeSet = new HashSet<>(consequenceTypes);
            }
            return consequenceTypeSet;
        }

        public Tier setConsequenceTypeSet(Set<String> consequenceTypeSet) {
            this.consequenceTypeSet = consequenceTypeSet;
            return this;
        }

        public boolean isPanels() {
            return panels;
        }

        public Tier setPanels(boolean panels) {
            this.panels = panels;
            return this;
        }

        public boolean isActionable() {
            return actionable;
        }

        public Tier setActionable(boolean actionable) {
            this.actionable = actionable;
            return this;
        }
    }

    public InterpretationAnalysisConfiguration() {
    }

    public InterpretationAnalysisConfiguration(Tier tier1, Tier tier2, Tier tier3, int maxLowCoverage, boolean includeLowCoverage,
                                               boolean skipUntieredVariants) {
        this.tier1 = tier1;
        this.tier2 = tier2;
        this.tier3 = tier3;
        this.maxLowCoverage = maxLowCoverage;
        this.includeLowCoverage = includeLowCoverage;
        this.skipUntieredVariants = skipUntieredVariants;
    }

    public Tier getTier1() {
        return tier1;
    }

    public InterpretationAnalysisConfiguration setTier1(Tier tier1) {
        this.tier1 = tier1;
        return this;
    }

    public Tier getTier2() {
        return tier2;
    }

    public InterpretationAnalysisConfiguration setTier2(Tier tier2) {
        this.tier2 = tier2;
        return this;
    }

    public Tier getTier3() {
        return tier3;
    }

    public InterpretationAnalysisConfiguration setTier3(Tier tier3) {
        this.tier3 = tier3;
        return this;
    }

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
