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

package org.opencb.opencga.analysis.variant.knockout.result;

import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty.Sex;

import java.util.*;

public class KnockoutByIndividual {


    private String id;
    private String sampleId;
    private Sex sex;
    private List<Phenotype> phenotypes;
    private List<Disorder> disorders;

    private GeneKnockoutBySampleStats stats;

    private Map<String, KnockoutGene> genesMap = new HashMap<>();

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

    public GeneKnockoutBySampleStats getStats() {
        return stats;
    }

    public KnockoutByIndividual setStats(GeneKnockoutBySampleStats stats) {
        this.stats = stats;
        return this;
    }

    public Collection<KnockoutGene> getGenes() {
        return genesMap.values();
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
                genesMap.put(gene.getName(), gene);
            }
        }
        return this;
    }

    public static class GeneKnockoutBySampleStats {
        private int numGenes;
        private int numTranscripts;
        private Map<KnockoutVariant.KnockoutType, Long> byType;

        public GeneKnockoutBySampleStats() {
            byType = new EnumMap<>(KnockoutVariant.KnockoutType.class);
        }

        public int getNumGenes() {
            return numGenes;
        }

        public GeneKnockoutBySampleStats setNumGenes(int numGenes) {
            this.numGenes = numGenes;
            return this;
        }

        public int getNumTranscripts() {
            return numTranscripts;
        }

        public GeneKnockoutBySampleStats setNumTranscripts(int numTranscripts) {
            this.numTranscripts = numTranscripts;
            return this;
        }

        public Map<KnockoutVariant.KnockoutType, Long> getByType() {
            return byType;
        }

        public GeneKnockoutBySampleStats setByType(Map<KnockoutVariant.KnockoutType, Long> byType) {
            this.byType = byType;
            return this;
        }
    }

    public static class KnockoutGene {
        private String id;
        private String name;
        private Map<String, KnockoutTranscript> transcriptsMap = new HashMap<>(); // Internal only

        public KnockoutGene() {
        }

        public KnockoutGene(String name) {
            this.name = name;
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
