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

package org.opencb.opencga.storage.core.variant.stats;

import org.opencb.biodata.models.variant.stats.VariantStats;

import java.util.Map;

/**
 * Class to link a VariantStats with its variant, using just the chromosome and the position.
 */
public class VariantStatsWrapper {
    private String chromosome;
    private int position;
    private Map<String, VariantStats> cohortStats;

    public VariantStatsWrapper() {
        this.chromosome = null;
        this.position = -1;
        this.cohortStats = null;
    }

    public VariantStatsWrapper(String chromosome, int position, Map<String, VariantStats> cohortStats) {
        this.chromosome = chromosome;
        this.position = position;
        this.cohortStats = cohortStats;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public Map<String, VariantStats> getCohortStats() {
        return cohortStats;
    }

    public void setCohortStats(Map<String, VariantStats> cohortStats) {
        this.cohortStats = cohortStats;
    }
}
