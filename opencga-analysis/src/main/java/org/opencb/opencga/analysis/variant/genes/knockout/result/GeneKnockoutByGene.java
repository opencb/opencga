package org.opencb.opencga.analysis.variant.genes.knockout.result;

import java.util.Collection;
import java.util.List;

public class GeneKnockoutByGene {

    private String id;
    private String name;
    private List<KnockoutSample> samples;

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

    public List<KnockoutSample> getSamples() {
        return samples;
    }

    public GeneKnockoutByGene setSamples(List<KnockoutSample> samples) {
        this.samples = samples;
        return this;
    }


    public static class KnockoutSample {
        private String id;
        private Collection<TranscriptKnockout> transcripts;

        public String getId() {
            return id;
        }

        public KnockoutSample setId(String id) {
            this.id = id;
            return this;
        }

        public Collection<TranscriptKnockout> getTranscripts() {
            return transcripts;
        }

        public KnockoutSample setTranscripts(Collection<TranscriptKnockout> transcripts) {
            this.transcripts = transcripts;
            return this;
        }
    }

}
