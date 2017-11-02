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

import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.biodata.models.variant.stats.VariantStats;

import java.util.Map;

/**
 * Class to link a VariantStats with its variant, using just the chromosome and the position.
 */
public class VariantStatsWrapper {
    private String chromosome;
    private int start;
    private int end;
    private StructuralVariation sv;
    private Map<String, VariantStats> cohortStats;

    public VariantStatsWrapper() {
        this.chromosome = null;
        this.start = -1;
        this.end = -1;
        this.cohortStats = null;
        this.sv = null;
    }

    public VariantStatsWrapper(String chromosome, int start, int end, Map<String, VariantStats> cohortStats, StructuralVariation sv) {
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.cohortStats = cohortStats;
        this.sv = sv;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public VariantStatsWrapper setEnd(int end) {
        this.end = end;
        return this;
    }

    public Map<String, VariantStats> getCohortStats() {
        return cohortStats;
    }

    public void setCohortStats(Map<String, VariantStats> cohortStats) {
        this.cohortStats = cohortStats;
    }

    public StructuralVariation getSv() {
        return sv;
    }

    public VariantStatsWrapper setSv(StructuralVariation sv) {
        this.sv = sv;
        return this;
    }
}
