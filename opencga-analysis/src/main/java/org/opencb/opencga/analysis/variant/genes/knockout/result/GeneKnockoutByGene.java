package org.opencb.opencga.analysis.variant.genes.knockout.result;

import java.util.*;

public class GeneKnockoutByGene {

    private String id;
    private String name;
    private List<KnockoutSample> samples = new LinkedList<>();

    public String getId() {
        return id;
    }

    public GeneKnockoutByGene setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public GeneKnockoutByGene setName(String name) {
        this.name = name;
        return this;
    }

    public KnockoutSample getSample(String sample) {
        return samples.stream().filter(s -> s.getId().equals(sample)).findFirst().orElseGet(() -> new KnockoutSample().setId(sample));
    }

    public List<KnockoutSample> getSamples() {
        return samples;
    }

    public GeneKnockoutByGene setSamples(List<KnockoutSample> samples) {
        this.samples = samples;
        return this;
    }

    public GeneKnockoutByGene addSample(KnockoutSample sample) {
        this.samples.add(sample);
        return this;
    }


    public static class KnockoutSample {
        private String id;
        private Map<String, TranscriptKnockout> transcriptsMap = new HashMap<>(); // Internal only

        public String getId() {
            return id;
        }

        public KnockoutSample setId(String id) {
            this.id = id;
            return this;
        }

        public TranscriptKnockout getTranscript(String transcript) {
            return transcriptsMap.computeIfAbsent(transcript, TranscriptKnockout::new);
        }

        public Collection<TranscriptKnockout> getTranscripts() {
            return transcriptsMap.values();
        }

        public KnockoutSample setTranscripts(Collection<TranscriptKnockout> transcripts) {
            if (transcripts == null) {
                transcriptsMap = null;
            } else {
                transcriptsMap = new HashMap<>();
                for (TranscriptKnockout transcript : transcripts) {
                    transcriptsMap.put(transcript.getId(), transcript);
                }
            }
            return this;
        }
    }

}
