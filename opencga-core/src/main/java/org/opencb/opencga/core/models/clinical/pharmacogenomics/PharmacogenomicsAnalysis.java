package org.opencb.opencga.core.models.clinical.pharmacogenomics;

/**
 * Pharmacogenomics analysis result linked to an Individual.
 * Contains a reference to the full results file and an inline summary.
 */
public class PharmacogenomicsAnalysis {

    private String sampleId;
    private String source;  // "openarray" or "ngs"
    private String path;    // Catalog path to the full JSON results file
    private PharmacogenomicsAnalysisSummary summary;

    public PharmacogenomicsAnalysis() {
    }

    public PharmacogenomicsAnalysis(String sampleId, String source, String path,
                                    PharmacogenomicsAnalysisSummary summary) {
        this.sampleId = sampleId;
        this.source = source;
        this.path = path;
        this.summary = summary;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PharmacogenomicsAnalysis{");
        sb.append("sampleId='").append(sampleId).append('\'');
        sb.append(", source='").append(source).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append(", summary=").append(summary);
        sb.append('}');
        return sb.toString();
    }

    public String getSampleId() { return sampleId; }
    public PharmacogenomicsAnalysis setSampleId(String sampleId) { this.sampleId = sampleId; return this; }

    public String getSource() { return source; }
    public PharmacogenomicsAnalysis setSource(String source) { this.source = source; return this; }

    public String getPath() { return path; }
    public PharmacogenomicsAnalysis setPath(String path) { this.path = path; return this; }

    public PharmacogenomicsAnalysisSummary getSummary() { return summary; }
    public PharmacogenomicsAnalysis setSummary(PharmacogenomicsAnalysisSummary summary) { this.summary = summary; return this; }
}
