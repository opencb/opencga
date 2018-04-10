package org.opencb.opencga.core.models.clinical;

import java.util.Map;

public class GenomicFeature {

    private String ensemblGeneId;
    private String ensemblTranscriptId;
    private String geneName;
    private Map<String, String> xrefs;

    private String ensemblRegulatoryId;

    public GenomicFeature() {
    }

    public GenomicFeature(String ensemblGeneId, String ensemblTranscriptId, String geneName, Map<String, String> xrefs,
                          String ensemblRegulatoryId) {
        this.ensemblGeneId = ensemblGeneId;
        this.ensemblTranscriptId = ensemblTranscriptId;
        this.geneName = geneName;
        this.xrefs = xrefs;
        this.ensemblRegulatoryId = ensemblRegulatoryId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenomicFeature{");
        sb.append("ensemblGeneId='").append(ensemblGeneId).append('\'');
        sb.append(", ensemblTranscriptId='").append(ensemblTranscriptId).append('\'');
        sb.append(", geneName='").append(geneName).append('\'');
        sb.append(", xrefs=").append(xrefs);
        sb.append(", ensemblRegulatoryId='").append(ensemblRegulatoryId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getEnsemblGeneId() {
        return ensemblGeneId;
    }

    public GenomicFeature setEnsemblGeneId(String ensemblGeneId) {
        this.ensemblGeneId = ensemblGeneId;
        return this;
    }

    public String getEnsemblTranscriptId() {
        return ensemblTranscriptId;
    }

    public GenomicFeature setEnsemblTranscriptId(String ensemblTranscriptId) {
        this.ensemblTranscriptId = ensemblTranscriptId;
        return this;
    }

    public String getGeneName() {
        return geneName;
    }

    public GenomicFeature setGeneName(String geneName) {
        this.geneName = geneName;
        return this;
    }

    public Map<String, String> getXrefs() {
        return xrefs;
    }

    public GenomicFeature setXrefs(Map<String, String> xrefs) {
        this.xrefs = xrefs;
        return this;
    }

    public String getEnsemblRegulatoryId() {
        return ensemblRegulatoryId;
    }

    public GenomicFeature setEnsemblRegulatoryId(String ensemblRegulatoryId) {
        this.ensemblRegulatoryId = ensemblRegulatoryId;
        return this;
    }
}
