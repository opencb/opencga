package org.opencb.opencga.analysis.variant.genes.knockout.result;

import org.opencb.opencga.core.models.Sample;

import java.util.*;

public class GeneKnockoutBySample {

    private Sample sample;

    private GeneKnockoutBySampleStats stats;

    private Collection<GeneKnockout> genes;

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
        return genes;
    }

    public GeneKnockoutBySample setGenes(Collection<GeneKnockout> genes) {
        this.genes = genes;
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

        public TranscriptKnockout addTranscript(String transcript) {
            return transcriptsMap.computeIfAbsent(transcript, TranscriptKnockout::new);
        }

        public Collection<TranscriptKnockout> getTranscripts() {
            return transcriptsMap.values();
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
