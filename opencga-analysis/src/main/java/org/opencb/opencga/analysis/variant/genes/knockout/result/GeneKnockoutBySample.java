package org.opencb.opencga.analysis.variant.genes.knockout.result;

import org.opencb.opencga.core.models.sample.Sample;

import java.util.*;

public class GeneKnockoutBySample {

    private Sample sample;

    private GeneKnockoutBySampleStats stats;

    private Map<String, GeneKnockout> genesMap = new HashMap<>();

    public Sample getSample() {
        return sample;
    }

    public GeneKnockoutBySample setSample(Sample sample) {
        this.sample = sample;
        return this;
    }

    public GeneKnockoutBySampleStats getStats() {
        return stats;
    }

    public GeneKnockoutBySample setStats(GeneKnockoutBySampleStats stats) {
        this.stats = stats;
        return this;
    }

    public Collection<GeneKnockout> getGenes() {
        return genesMap.values();
    }

    public GeneKnockout getGene(String gene) {
        return genesMap.computeIfAbsent(gene, GeneKnockout::new);
    }

    public GeneKnockoutBySample setGenes(Collection<GeneKnockout> genes) {
        if (genes == null) {
            genesMap = null;
        } else {
            genesMap = new HashMap<>(genes.size());
            for (GeneKnockout gene : genes) {
                genesMap.put(gene.getName(), gene);
            }
        }
        return this;
    }

    public static class GeneKnockoutBySampleStats {
        private int numGenes;
        private int numTranscripts;
        private Map<VariantKnockout.KnockoutType, Long> byType;

        public GeneKnockoutBySampleStats() {
            byType = new EnumMap<>(VariantKnockout.KnockoutType.class);
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

        public Map<VariantKnockout.KnockoutType, Long> getByType() {
            return byType;
        }

        public GeneKnockoutBySampleStats setByType(Map<VariantKnockout.KnockoutType, Long> byType) {
            this.byType = byType;
            return this;
        }
    }

    public static class GeneKnockout {
        private String id;
        private String name;
        private Map<String, TranscriptKnockout> transcriptsMap = new HashMap<>(); // Internal only

        public GeneKnockout() {
        }

        public GeneKnockout(String name) {
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public GeneKnockout setId(String id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public GeneKnockout setName(String name) {
            this.name = name;
            return this;
        }

        public TranscriptKnockout getTranscript(String transcript) {
            return transcriptsMap.computeIfAbsent(transcript, TranscriptKnockout::new);
        }

        public Collection<TranscriptKnockout> getTranscripts() {
            return transcriptsMap.values();
        }

        public GeneKnockout addTranscripts(Collection<TranscriptKnockout> transcripts) {
            for (TranscriptKnockout transcript : transcripts) {
                transcriptsMap.put(transcript.getId(), transcript);
            }
            return this;
        }

        public GeneKnockout setTranscripts(List<TranscriptKnockout> transcripts) {
            transcriptsMap.clear();
            if (transcripts != null) {
                transcripts.forEach(t -> transcriptsMap.put(t.getId(), t));
            }
            return this;
        }
    }

}
