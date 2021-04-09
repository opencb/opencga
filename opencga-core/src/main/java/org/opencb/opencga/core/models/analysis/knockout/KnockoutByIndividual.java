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
import org.opencb.biodata.models.pedigree.IndividualProperty.Sex;
import org.opencb.opencga.core.common.JacksonUtils;

import java.util.*;

public class KnockoutByIndividual {

    private String id;
    private String sampleId;
    private String motherId;
    private String motherSampleId;
    private String fatherId;
    private String fatherSampleId;
    private Sex sex;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;

    private GeneKnockoutByIndividualStats stats;
    private Map<String, KnockoutGene> genesMap = new HashMap<>();

    public KnockoutByIndividual() {
    }

    public KnockoutByIndividual(String id, String sampleId, Sex sex, List<Phenotype> phenotypes, List<Disorder> disorders,
                                GeneKnockoutByIndividualStats stats) {
        this.id = id;
        this.sampleId = sampleId;
        this.sex = sex;
        this.phenotypes = phenotypes;
        this.disorders = disorders;
        this.stats = stats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutByIndividual{");
        sb.append("id='").append(id).append('\'');
        sb.append(", sampleId='").append(sampleId).append('\'');
        sb.append(", motherId='").append(motherId).append('\'');
        sb.append(", fatherId='").append(fatherId).append('\'');
        sb.append(", sex=").append(sex);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", stats=").append(stats);
        sb.append(", genesMap=").append(genesMap);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public KnockoutByIndividual setId(String id) {
        this.id = id;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public KnockoutByIndividual setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getMotherId() {
        return motherId;
    }

    public KnockoutByIndividual setMotherId(String motherId) {
        this.motherId = motherId;
        return this;
    }

    public String getMotherSampleId() {
        return motherSampleId;
    }

    public KnockoutByIndividual setMotherSampleId(String motherSampleId) {
        this.motherSampleId = motherSampleId;
        return this;
    }

    public String getFatherId() {
        return fatherId;
    }

    public KnockoutByIndividual setFatherId(String fatherId) {
        this.fatherId = fatherId;
        return this;
    }

    public String getFatherSampleId() {
        return fatherSampleId;
    }

    public KnockoutByIndividual setFatherSampleId(String fatherSampleId) {
        this.fatherSampleId = fatherSampleId;
        return this;
    }

    public Sex getSex() {
        return sex;
    }

    public KnockoutByIndividual setSex(Sex sex) {
        this.sex = sex;
        return this;
    }

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public KnockoutByIndividual setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<Disorder> getDisorders() {
        return disorders;
    }

    public KnockoutByIndividual setDisorders(List<Disorder> disorders) {
        this.disorders = disorders;
        return this;
    }

    public GeneKnockoutByIndividualStats getStats() {
        return stats;
    }

    public KnockoutByIndividual setStats(GeneKnockoutByIndividualStats stats) {
        this.stats = stats;
        return this;
    }

    public Collection<KnockoutGene> getGenes() {
        return genesMap.values();
    }

    public KnockoutByIndividual addGene(KnockoutGene gene) {
        genesMap.put(gene.getName(), gene);
        return this;
    }

    public KnockoutByIndividual addGenes(Collection<KnockoutGene> genes) {
        for (KnockoutGene gene : genes) {
            genesMap.put(gene.getName(), gene);
        }
        return this;
    }

    public KnockoutGene getGene(String gene) {
        return genesMap.computeIfAbsent(gene, KnockoutGene::new);
    }

    public KnockoutByIndividual setGenes(Collection<KnockoutGene> genes) {
        if (genes == null) {
            genesMap = null;
        } else {
            genesMap = new HashMap<>(genes.size());
            for (KnockoutGene gene : genes) {
                genesMap.put(gene.getId(), gene);
            }
        }
        return this;
    }

    public static class GeneKnockoutByIndividualStats {
        private int numGenes;
        private int numTranscripts;
        private Map<KnockoutVariant.KnockoutType, Long> byType;

        public GeneKnockoutByIndividualStats() {
            byType = new EnumMap<>(KnockoutVariant.KnockoutType.class);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("GeneKnockoutByIndividualStats{");
            sb.append("numGenes=").append(numGenes);
            sb.append(", numTranscripts=").append(numTranscripts);
            sb.append(", byType=").append(byType);
            sb.append('}');
            return sb.toString();
        }

        public int getNumGenes() {
            return numGenes;
        }

        public GeneKnockoutByIndividualStats setNumGenes(int numGenes) {
            this.numGenes = numGenes;
            return this;
        }

        public int getNumTranscripts() {
            return numTranscripts;
        }

        public GeneKnockoutByIndividualStats setNumTranscripts(int numTranscripts) {
            this.numTranscripts = numTranscripts;
            return this;
        }

        public Map<KnockoutVariant.KnockoutType, Long> getByType() {
            return byType;
        }

        public GeneKnockoutByIndividualStats setByType(Map<KnockoutVariant.KnockoutType, Long> byType) {
            this.byType = byType;
            return this;
        }
    }

    public static class KnockoutGene {
        private String id;
        private String name;
        private String chromosome;
        private int start;
        private int end;
        private String biotype;
        private String strand;
        private Map<String, KnockoutTranscript> transcriptsMap = new HashMap<>(); // Internal only

        public KnockoutGene() {
        }

        public KnockoutGene(String id, String name, String chromosome, int start, int end, String biotype, String strand) {
            this.id = id;
            this.name = name;
            this.chromosome = chromosome;
            this.start = start;
            this.end = end;
            this.biotype = biotype;
            this.strand = strand;
        }

        public KnockoutGene(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("KnockoutGene{");
            sb.append("id='").append(id).append('\'');
            sb.append(", name='").append(name).append('\'');
            sb.append(", chromosome='").append(chromosome).append('\'');
            sb.append(", start=").append(start);
            sb.append(", end=").append(end);
            sb.append(", biotype='").append(biotype).append('\'');
            sb.append(", strand='").append(strand).append('\'');
            sb.append(", transcriptsMap=").append(transcriptsMap);
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public KnockoutGene setId(String id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public KnockoutGene setName(String name) {
            this.name = name;
            return this;
        }

        public String getChromosome() {
            return chromosome;
        }

        public KnockoutGene setChromosome(String chromosome) {
            this.chromosome = chromosome;
            return this;
        }

        public int getStart() {
            return start;
        }

        public KnockoutGene setStart(int start) {
            this.start = start;
            return this;
        }

        public int getEnd() {
            return end;
        }

        public KnockoutGene setEnd(int end) {
            this.end = end;
            return this;
        }

        public String getBiotype() {
            return biotype;
        }

        public KnockoutGene setBiotype(String biotype) {
            this.biotype = biotype;
            return this;
        }

        public String getStrand() {
            return strand;
        }

        public KnockoutGene setStrand(String strand) {
            this.strand = strand;
            return this;
        }

        public KnockoutTranscript getTranscript(String transcript) {
            return transcriptsMap.computeIfAbsent(transcript, KnockoutTranscript::new);
        }

        public Collection<KnockoutTranscript> getTranscripts() {
            return transcriptsMap.values();
        }

        public KnockoutGene addTranscripts(Collection<KnockoutTranscript> transcripts) {
            for (KnockoutTranscript transcript : transcripts) {
                transcriptsMap.put(transcript.getId(), transcript);
            }
            return this;
        }

        public KnockoutGene setTranscripts(List<KnockoutTranscript> transcripts) {
            transcriptsMap.clear();
            if (transcripts != null) {
                transcripts.forEach(t -> transcriptsMap.put(t.getId(), t));
            }
            return this;
        }
    }

}
