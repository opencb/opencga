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

package org.opencb.opencga.core.models.analysis.knockout;

import java.util.LinkedList;
import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class KnockoutTranscript {

    @DataField(description = ParamConstants.KNOCKOUT_TRANSCRIPT_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.KNOCKOUT_TRANSCRIPT_CHROMOSOME_DESCRIPTION)
    private String chromosome;
    @DataField(description = ParamConstants.KNOCKOUT_TRANSCRIPT_START_DESCRIPTION)
    private int start;
    @DataField(description = ParamConstants.KNOCKOUT_TRANSCRIPT_END_DESCRIPTION)
    private int end;
    @DataField(description = ParamConstants.KNOCKOUT_TRANSCRIPT_BIOTYPE_DESCRIPTION)
    private String biotype;
    @DataField(description = ParamConstants.KNOCKOUT_TRANSCRIPT_STRAND_DESCRIPTION)
    private String strand;

    private List<KnockoutVariant> variants = new LinkedList<>();

    public KnockoutTranscript() {
    }

    public KnockoutTranscript(String id) {
        this.id = id;
    }

    public KnockoutTranscript(String id, String chromosome, int start, int end, String biotype, String strand,
                              List<KnockoutVariant> variants) {
        this.id = id;
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.biotype = biotype;
        this.strand = strand;
        this.variants = variants;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutTranscript{");
        sb.append("id='").append(id).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", biotype='").append(biotype).append('\'');
        sb.append(", strand='").append(strand).append('\'');
        sb.append(", variants=").append(variants);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public KnockoutTranscript setId(String id) {
        this.id = id;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public KnockoutTranscript setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public KnockoutTranscript setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public KnockoutTranscript setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public KnockoutTranscript setEnd(int end) {
        this.end = end;
        return this;
    }

    public String getStrand() {
        return strand;
    }

    public KnockoutTranscript setStrand(String strand) {
        this.strand = strand;
        return this;
    }

    public List<KnockoutVariant> getVariants() {
        return variants;
    }

    public KnockoutTranscript setVariants(List<KnockoutVariant> variants) {
        this.variants = variants;
        return this;
    }

    public KnockoutTranscript addVariant(KnockoutVariant variant) {
        this.variants.add(variant);
        return this;
    }
}
