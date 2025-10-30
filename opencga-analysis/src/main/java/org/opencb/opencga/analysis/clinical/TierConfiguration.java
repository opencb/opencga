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

import java.util.List;
import java.util.Map;

public class TierConfiguration {

    private Boolean discardUntieredEvidences;
    private Map<String, Tier> tiers;

    public TierConfiguration() {
    }

    public TierConfiguration(Boolean discardUntieredEvidences, Map<String, Tier> tiers) {
        this.discardUntieredEvidences = discardUntieredEvidences;
        this.tiers = tiers;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TierConfiguration{");
        sb.append("discardUntieredEvidences=").append(discardUntieredEvidences);
        sb.append(", tiers=").append(tiers);
        sb.append('}');
        return sb.toString();
    }

    public Boolean getDiscardUntieredEvidences() {
        return discardUntieredEvidences;
    }

    public TierConfiguration setDiscardUntieredEvidences(Boolean discardUntieredEvidences) {
        this.discardUntieredEvidences = discardUntieredEvidences;
        return this;
    }

    public Map<String, Tier> getTiers() {
        return tiers;
    }

    public TierConfiguration setTiers(Map<String, Tier> tiers) {
        this.tiers = tiers;
        return this;
    }

    public static class Tier {
        private String label;
        private String description;
        private List<Scenario> scenarios;

        public Tier() {
        }

        public Tier(String label, String description, List<Scenario> scenarios) {
            this.label = label;
            this.description = description;
            this.scenarios = scenarios;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Tier{");
            sb.append("label='").append(label).append('\'');
            sb.append(", description='").append(description).append('\'');
            sb.append(", scenarios=").append(scenarios);
            sb.append('}');
            return sb.toString();
        }

        public String getLabel() {
            return label;
        }

        public Tier setLabel(String label) {
            this.label = label;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public Tier setDescription(String description) {
            this.description = description;
            return this;
        }

        public List<Scenario> getScenarios() {
            return scenarios;
        }

        public Tier setScenarios(List<Scenario> scenarios) {
            this.scenarios = scenarios;
            return this;
        }
    }

    public static class Scenario {
        private Map<String, Object> conditions;

        public Scenario() {
        }

        public Scenario(Map<String, Object> conditions) {
            this.conditions = conditions;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Scenario{");
            sb.append(", conditions=").append(conditions);
            sb.append('}');
            return sb.toString();
        }

        public Map<String, Object> getConditions() {
            return conditions;
        }

        public Scenario setConditions(Map<String, Object> conditions) {
            this.conditions = conditions;
            return this;
        }
    }
}
