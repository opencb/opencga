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

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.biodata.models.variant.stats.VariantStats;

import java.util.List;

/**
 * Class to link a VariantStats with its variant, using just the chromosome and the position.
 */
public class VariantStatsWrapper {
    private String chromosome;
    private int start;
    private int end;
    private String reference;
    private String alternate;

    private StructuralVariation sv;
    private List<VariantStats> cohortStats;

    public VariantStatsWrapper() {
        this.chromosome = null;
        this.start = -1;
        this.end = -1;
        this.cohortStats = null;
        this.sv = null;
    }

    public VariantStatsWrapper(Variant variant, List<VariantStats> cohortStats) {
        this.chromosome = variant.getChromosome();
        this.start = variant.getStart();
        this.end = variant.getEnd();
        this.reference = variant.getReference();
        this.alternate = variant.getAlternate();
        this.sv = variant.getSv();
        this.cohortStats = cohortStats;
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

    public String getReference() {
        return reference;
    }

    public VariantStatsWrapper setReference(String reference) {
        this.reference = reference;
        return this;
    }

    public String getAlternate() {
        return alternate;
    }

    public VariantStatsWrapper setAlternate(String alternate) {
        this.alternate = alternate;
        return this;
    }

    public List<VariantStats> getCohortStats() {
        return cohortStats;
    }

    public void setCohortStats(List<VariantStats> cohortStats) {
        this.cohortStats = cohortStats;
    }

    public StructuralVariation getSv() {
        return sv;
    }

    public VariantStatsWrapper setSv(StructuralVariation sv) {
        this.sv = sv;
        return this;
    }

    public Variant toVariant() {
        return new Variant(chromosome, start, end, reference, alternate).setSv(sv);
    }
}
