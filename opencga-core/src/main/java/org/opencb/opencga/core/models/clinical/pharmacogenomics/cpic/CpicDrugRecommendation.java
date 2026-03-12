package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

/**
 * Drug recommendation retrieved from the CPIC /recommendation endpoint.
 * Example: GET https://api.cpicpgx.org/v1/recommendation?lookupkey=cs.{"CYP2C9":"1.0"}
 * Only the most clinically relevant fields are retained.
 */
public class CpicDrugRecommendation {

    private String source;            // always "CPIC"
    private String drugid;            // e.g. "RxNorm:202433"
    private String drugnamesource;    // human-readable drug name, e.g. "warfarin"
    private String drugrecommendation;
    private String classification;    // "Strong", "Moderate", "Optional", etc.
    private String population;        // "general", "pediatrics", etc.
    private String comments;

    public CpicDrugRecommendation() {
    }

    public CpicDrugRecommendation(String source, String drugid, String drugnamesource, String drugrecommendation,
                                  String classification, String population, String comments) {
        this.source = source;
        this.drugid = drugid;
        this.drugnamesource = drugnamesource;
        this.drugrecommendation = drugrecommendation;
        this.classification = classification;
        this.population = population;
        this.comments = comments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicDrugRecommendation{");
        sb.append("source='").append(source).append('\'');
        sb.append(", drugid='").append(drugid).append('\'');
        sb.append(", drugnamesource='").append(drugnamesource).append('\'');
        sb.append(", drugrecommendation='").append(drugrecommendation).append('\'');
        sb.append(", classification='").append(classification).append('\'');
        sb.append(", population='").append(population).append('\'');
        sb.append(", comments='").append(comments).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSource() {
        return source;
    }

    public CpicDrugRecommendation setSource(String source) {
        this.source = source;
        return this;
    }

    public String getDrugid() {
        return drugid;
    }

    public CpicDrugRecommendation setDrugid(String drugid) {
        this.drugid = drugid;
        return this;
    }

    public String getDrugnamesource() {
        return drugnamesource;
    }

    public CpicDrugRecommendation setDrugnamesource(String drugnamesource) {
        this.drugnamesource = drugnamesource;
        return this;
    }

    public String getDrugrecommendation() {
        return drugrecommendation;
    }

    public CpicDrugRecommendation setDrugrecommendation(String drugrecommendation) {
        this.drugrecommendation = drugrecommendation;
        return this;
    }

    public String getClassification() {
        return classification;
    }

    public CpicDrugRecommendation setClassification(String classification) {
        this.classification = classification;
        return this;
    }

    public String getPopulation() {
        return population;
    }

    public CpicDrugRecommendation setPopulation(String population) {
        this.population = population;
        return this;
    }

    public String getComments() {
        return comments;
    }

    public CpicDrugRecommendation setComments(String comments) {
        this.comments = comments;
        return this;
    }
}
