package org.opencb.opencga.analysis.variant.knockout.result;

import java.util.*;

public class KnockoutByGene {

    private String id;
    private String name;
    private List<KnockoutSample> samples = new LinkedList<>();

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

    public KnockoutSample getSample(String sample) {
        for (KnockoutSample s : samples) {
            if (s.getId().equals(sample)) {
                return s;
            }
        }
        KnockoutSample knockoutSample = new KnockoutSample().setId(sample);
        samples.add(knockoutSample);
        return knockoutSample;
    }

    public List<KnockoutSample> getSamples() {
        return samples;
    }

    public KnockoutByGene setSamples(List<KnockoutSample> samples) {
        this.samples = samples;
        return this;
    }

    public KnockoutByGene addSample(KnockoutSample sample) {
        this.samples.add(sample);
        return this;
    }


    public static class KnockoutSample {
        private String id;
        private Map<String, KnockoutTranscript> transcriptsMap = new HashMap<>(); // Internal only

        public String getId() {
            return id;
        }

        public KnockoutSample setId(String id) {
            this.id = id;
            return this;
        }

        public KnockoutTranscript getTranscript(String transcript) {
            return transcriptsMap.computeIfAbsent(transcript, KnockoutTranscript::new);
        }

        public Collection<KnockoutTranscript> getTranscripts() {
            return transcriptsMap.values();
        }

        public KnockoutSample setTranscripts(Collection<KnockoutTranscript> transcripts) {
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
