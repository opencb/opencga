package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

import java.util.Map;

public class CpicDrugRecommendation {
    private Integer id;
    private String  guidelineid;
    private String  drugid;          // e.g. "RxNorm:202433"
    private String  drugnamesource;  // human-readable drug name

    // Gene-keyed maps (support multi-gene guidelines)
    private Map<String, String> implications;  // per-gene implication text
    private Map<String, String> phenotypes;    // e.g. {"CYP2C9": "Intermediate Metabolizer"}
    private Map<String, String> activityscore; // e.g. {"CYP2C9": "1.0"}
    private Map<String, String> allelestatus;  // used for HLA-type genes
    private Map<String, String> lookupkey;     // the key used to match this rec

    private String recommendation;  // dosing recommendation text
    private String comments;
    private String population;      // "general", "pediatrics", etc.
    private String classification;  // "Strong", "Moderate", "Optional", etc.

    public CpicDrugRecommendation() {
    }

    public CpicDrugRecommendation(Integer id, String guidelineid, String drugid, String drugnamesource, Map<String, String> implications,
                                  Map<String, String> phenotypes, Map<String, String> activityscore, Map<String, String> allelestatus,
                                  Map<String, String> lookupkey, String recommendation, String comments, String population,
                                  String classification) {
        this.id = id;
        this.guidelineid = guidelineid;
        this.drugid = drugid;
        this.drugnamesource = drugnamesource;
        this.implications = implications;
        this.phenotypes = phenotypes;
        this.activityscore = activityscore;
        this.allelestatus = allelestatus;
        this.lookupkey = lookupkey;
        this.recommendation = recommendation;
        this.comments = comments;
        this.population = population;
        this.classification = classification;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicDrugRecommendation{");
        sb.append("id=").append(id);
        sb.append(", guidelineid='").append(guidelineid).append('\'');
        sb.append(", drugid='").append(drugid).append('\'');
        sb.append(", drugnamesource='").append(drugnamesource).append('\'');
        sb.append(", implications=").append(implications);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", activityscore=").append(activityscore);
        sb.append(", allelestatus=").append(allelestatus);
        sb.append(", lookupkey=").append(lookupkey);
        sb.append(", recommendation='").append(recommendation).append('\'');
        sb.append(", comments='").append(comments).append('\'');
        sb.append(", population='").append(population).append('\'');
        sb.append(", classification='").append(classification).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public Integer getId() {
        return id;
    }

    public CpicDrugRecommendation setId(Integer id) {
        this.id = id;
        return this;
    }

    public String getGuidelineid() {
        return guidelineid;
    }

    public CpicDrugRecommendation setGuidelineid(String guidelineid) {
        this.guidelineid = guidelineid;
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

    public Map<String, String> getImplications() {
        return implications;
    }

    public CpicDrugRecommendation setImplications(Map<String, String> implications) {
        this.implications = implications;
        return this;
    }

    public Map<String, String> getPhenotypes() {
        return phenotypes;
    }

    public CpicDrugRecommendation setPhenotypes(Map<String, String> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public Map<String, String> getActivityscore() {
        return activityscore;
    }

    public CpicDrugRecommendation setActivityscore(Map<String, String> activityscore) {
        this.activityscore = activityscore;
        return this;
    }

    public Map<String, String> getAllelestatus() {
        return allelestatus;
    }

    public CpicDrugRecommendation setAllelestatus(Map<String, String> allelestatus) {
        this.allelestatus = allelestatus;
        return this;
    }

    public Map<String, String> getLookupkey() {
        return lookupkey;
    }

    public CpicDrugRecommendation setLookupkey(Map<String, String> lookupkey) {
        this.lookupkey = lookupkey;
        return this;
    }

    public String getRecommendation() {
        return recommendation;
    }

    public CpicDrugRecommendation setRecommendation(String recommendation) {
        this.recommendation = recommendation;
        return this;
    }

    public String getComments() {
        return comments;
    }

    public CpicDrugRecommendation setComments(String comments) {
        this.comments = comments;
        return this;
    }

    public String getPopulation() {
        return population;
    }

    public CpicDrugRecommendation setPopulation(String population) {
        this.population = population;
        return this;
    }

    public String getClassification() {
        return classification;
    }

    public CpicDrugRecommendation setClassification(String classification) {
        this.classification = classification;
        return this;
    }
}
