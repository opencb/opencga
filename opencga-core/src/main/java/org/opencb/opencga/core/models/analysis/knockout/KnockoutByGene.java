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

import org.opencb.biodata.models.core.GeneAnnotation;

import java.util.*;

public class KnockoutByGene {

    private String id;
    private String name;
    private String chromosome;
    private int start;
    private int end;
    private String strand;
    private String biotype;
    private GeneAnnotation annotation;

    private List<KnockoutIndividual> individuals = new LinkedList<>();

    public String getId() {
        return id;
    }

    public KnockoutByGene setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public KnockoutByGene setName(String name) {
        this.name = name;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public KnockoutByGene setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public KnockoutByGene setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public KnockoutByGene setEnd(int end) {
        this.end = end;
        return this;
    }

    public String getStrand() {
        return strand;
    }

    public KnockoutByGene setStrand(String strand) {
        this.strand = strand;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public KnockoutByGene setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public GeneAnnotation getAnnotation() {
        return annotation;
    }

    public KnockoutByGene setAnnotation(GeneAnnotation annotation) {
        this.annotation = annotation;
        return this;
    }

    public KnockoutIndividual getIndividualBySampleId(String sample) {
        Objects.requireNonNull(sample);
        for (KnockoutIndividual s : individuals) {
            if (s.getSampleId().equals(sample)) {
                return s;
            }
        }
        KnockoutIndividual knockoutIndividual = new KnockoutIndividual().setSampleId(sample);
        individuals.add(knockoutIndividual);
        return knockoutIndividual;
    }

    public List<KnockoutIndividual> getIndividuals() {
        return individuals;
    }

    public KnockoutByGene setIndividuals(List<KnockoutIndividual> individuals) {
        this.individuals = individuals;
        return this;
    }

    public KnockoutByGene addIndividual(KnockoutIndividual individual) {
        this.individuals.add(individual);
        return this;
    }


    public static class KnockoutIndividual {
        private String id;
        private String sampleId;
        private Map<String, KnockoutTranscript> transcriptsMap = new HashMap<>(); // Internal only

        public KnockoutIndividual() {
        }

        public String getId() {
            return id;
        }

        public KnockoutIndividual setId(String id) {
            this.id = id;
            return this;
        }

        public String getSampleId() {
            return sampleId;
        }

        public KnockoutIndividual setSampleId(String sampleId) {
            this.sampleId = sampleId;
            return this;
        }

        public KnockoutTranscript getTranscript(String transcript) {
            return transcriptsMap.computeIfAbsent(transcript, KnockoutTranscript::new);
        }

        public Collection<KnockoutTranscript> getTranscripts() {
            return transcriptsMap.values();
        }

        public KnockoutIndividual setTranscripts(Collection<KnockoutTranscript> transcripts) {
            if (transcripts == null) {
                transcriptsMap = null;
            } else {
                transcriptsMap = new HashMap<>();
                for (KnockoutTranscript transcript : transcripts) {
                    transcriptsMap.put(transcript.getId(), transcript);
                }
            }
            return this;
        }
    }

}
