package org.opencb.opencga.analysis.variant.genes.knockout.result;

import org.opencb.opencga.storage.core.metadata.models.Trio;

import java.util.*;

public class GeneKnockoutBySample {

    private String sample;
    private int genesCount;
    private int transcriptsCount;
    private Trio trio;

    private CountByType countByType;

    private Collection<GeneKnockout> genes;

    public String getSample() {
        return sample;
    }

    public GeneKnockoutBySample setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public int getGenesCount() {
        return genesCount;
    }

    public GeneKnockoutBySample setGenesCount(int genesCount) {
        this.genesCount = genesCount;
        return this;
    }

    public int getTranscriptsCount() {
        return transcriptsCount;
    }

    public GeneKnockoutBySample setTranscriptsCount(int transcriptsCount) {
        this.transcriptsCount = transcriptsCount;
        return this;
    }

    public Trio getTrio() {
        return trio;
    }

    public GeneKnockoutBySample setTrio(Trio trio) {
        this.trio = trio;
        return this;
    }

    public CountByType getCountByType() {
        return countByType;
    }

    public GeneKnockoutBySample setCountByType(CountByType countByType) {
        this.countByType = countByType;
        return this;
    }

    public Collection<GeneKnockout> getGenes() {
        return genes;
    }

    public GeneKnockoutBySample setGenes(Collection<GeneKnockout> genes) {
        this.genes = genes;
        return this;
    }

    public static class CountByType {
        private int homAltCount;
        private int multiAllelicCount;
        private int compHetCount;
        private int deletionOverlapCount;

        public CountByType() {
        }

        public CountByType(int homAltCount, int multiAllelicCount, int compHetCount, int deletionOverlapCount) {
            this.homAltCount = homAltCount;
            this.multiAllelicCount = multiAllelicCount;
            this.compHetCount = compHetCount;
            this.deletionOverlapCount = deletionOverlapCount;
        }

        public int getHomAltCount() {
            return homAltCount;
        }

        public CountByType setHomAltCount(int homAltCount) {
            this.homAltCount = homAltCount;
            return this;
        }

        public int getMultiAllelicCount() {
            return multiAllelicCount;
        }

        public CountByType setMultiAllelicCount(int multiAllelicCount) {
            this.multiAllelicCount = multiAllelicCount;
            return this;
        }

        public int getCompHetCount() {
            return compHetCount;
        }

        public CountByType setCompHetCount(int compHetCount) {
            this.compHetCount = compHetCount;
            return this;
        }

        public int getDeletionOverlapCount() {
            return deletionOverlapCount;
        }

        public CountByType setDeletionOverlapCount(int deletionOverlapCount) {
            this.deletionOverlapCount = deletionOverlapCount;
            return this;
        }
    }

    public static class GeneKnockout {
        private String id;
        private String name;
        private String biotype;
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

        public String getBiotype() {
            return biotype;
        }

        public GeneKnockout setBiotype(String biotype) {
            this.biotype = biotype;
            return this;
        }

        public TranscriptKnockout addTranscript(String transcript) {
            return transcriptsMap.computeIfAbsent(transcript, TranscriptKnockout::new);
        }

        public Collection<TranscriptKnockout> getTranscripts() {
            return transcriptsMap.values();
        }

        public GeneKnockout setTranscripts(Collection<TranscriptKnockout> transcripts) {
            transcriptsMap.clear();
            if (transcripts != null) {
                transcripts.forEach(t -> transcriptsMap.put(t.id, t));
            }
            return this;
        }
    }

    public static class TranscriptKnockout {

        private String id;
        private List<String> homAltVariants = new LinkedList<>();
        private List<String> multiAllelicVariants = new LinkedList<>();
        private List<String> compHetVariants = new LinkedList<>();
        private List<String> deletionOverlapVariants = new LinkedList<>();

        public TranscriptKnockout(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public TranscriptKnockout setId(String id) {
            this.id = id;
            return this;
        }

        public int getHomAltCount() {
            return homAltVariants.size();
        }

        public int getMultiAllelicCount() {
            return multiAllelicVariants.size();
        }

        public int getCompHetCount() {
            return compHetVariants.size();
        }

        public int getDeletionOverlapCount() {
            return deletionOverlapVariants.size();
        }

        public List<String> getHomAltVariants() {
            return homAltVariants;
        }

        public TranscriptKnockout setHomAltVariants(List<String> homAltVariants) {
            this.homAltVariants = homAltVariants;
            return this;
        }

        public List<String> getMultiAllelicVariants() {
            return multiAllelicVariants;
        }

        public TranscriptKnockout setMultiAllelicVariants(List<String> multiAllelicVariants) {
            this.multiAllelicVariants = multiAllelicVariants;
            return this;
        }

        public List<String> getCompHetVariants() {
            return compHetVariants;
        }

        public TranscriptKnockout setCompHetVariants(List<String> compHetVariants) {
            this.compHetVariants = compHetVariants;
            return this;
        }

        public List<String> getDeletionOverlapVariants() {
            return deletionOverlapVariants;
        }

        public TranscriptKnockout setDeletionOverlapVariants(List<String> deletionOverlapVariants) {
            this.deletionOverlapVariants = deletionOverlapVariants;
            return this;
        }

        public void addHomAlt(String variant) {
            homAltVariants.add(variant);
        }

        public void addMultiAllelic(String variant) {
            multiAllelicVariants.add(variant);
        }

        public void addCompHet(String variant) {
            compHetVariants.add(variant);
        }

        public void addDelOverlap(String variant) {
            deletionOverlapVariants.add(variant);
        }

    }
}
