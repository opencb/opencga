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

import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.GeneAnnotation;

import java.util.*;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class KnockoutByGene {

    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_CHROMOSOME_DESCRIPTION)
    private String chromosome;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_START_DESCRIPTION)
    private int start;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_END_DESCRIPTION)
    private int end;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_STRAND_DESCRIPTION)
    private String strand;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_BIOTYPE_DESCRIPTION)
    private String biotype;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_ANNOTATION_DESCRIPTION)
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
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_ID_DESCRIPTION)
        private String id;
        private String sampleId;
        private String motherId;
        private String motherSampleId;
        private String fatherId;
        private String fatherSampleId;
        private String sex;
        private List<Phenotype> phenotypes;
        private List<Disorder> disorders;
        private Map<String, KnockoutTranscript> transcriptsMap = new HashMap<>(); // Internal only

        public KnockoutIndividual() {
        }

        public KnockoutIndividual(String id, String sampleId, String motherId, String motherSampleId, String fatherId,
                                  String fatherSampleId, String sex, List<Phenotype> phenotypes, List<Disorder> disorders) {
            this.id = id;
            this.sampleId = sampleId;
            this.motherId = motherId;
            this.motherSampleId = motherSampleId;
            this.fatherId = fatherId;
            this.fatherSampleId = fatherSampleId;
            this.sex = sex;
            this.phenotypes = phenotypes;
            this.disorders = disorders;
        }

        public KnockoutIndividual(KnockoutByIndividual knockoutByIndividual) {
            this(knockoutByIndividual.getId(), knockoutByIndividual.getSampleId(),
                    knockoutByIndividual.getMotherId(), knockoutByIndividual.getMotherSampleId(), knockoutByIndividual.getFatherId(),
                    knockoutByIndividual.getFatherSampleId(), knockoutByIndividual.getSex(), knockoutByIndividual.getPhenotypes(),
                    knockoutByIndividual.getDisorders());
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

        public String getMotherId() {
            return motherId;
        }

        public KnockoutIndividual setMotherId(String motherId) {
            this.motherId = motherId;
            return this;
        }

        public String getMotherSampleId() {
            return motherSampleId;
        }

        public KnockoutIndividual setMotherSampleId(String motherSampleId) {
            this.motherSampleId = motherSampleId;
            return this;
        }

        public String getFatherId() {
            return fatherId;
        }

        public KnockoutIndividual setFatherId(String fatherId) {
            this.fatherId = fatherId;
            return this;
        }

        public String getFatherSampleId() {
            return fatherSampleId;
        }

        public KnockoutIndividual setFatherSampleId(String fatherSampleId) {
            this.fatherSampleId = fatherSampleId;
            return this;
        }

        public String getSex() {
            return sex;
        }

        public KnockoutIndividual setSex(String sex) {
            this.sex = sex;
            return this;
        }

        public List<Phenotype> getPhenotypes() {
            return phenotypes;
        }

        public KnockoutIndividual setPhenotypes(List<Phenotype> phenotypes) {
            this.phenotypes = phenotypes;
            return this;
        }

        public List<Disorder> getDisorders() {
            return disorders;
        }

        public KnockoutIndividual setDisorders(List<Disorder> disorders) {
            this.disorders = disorders;
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

        public KnockoutIndividual addTranscripts(Collection<KnockoutTranscript> transcripts) {
            for (KnockoutTranscript transcript : transcripts) {
                transcriptsMap.put(transcript.getId(), transcript);
            }
            return this;
        }
    }

}
