package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

import java.util.Map;

/**
 * Drug recommendation retrieved from the CPIC /recommendation endpoint.
 * Example: GET https://api.cpicpgx.org/v1/recommendation?lookupkey=cs.{"CYP2C19":"Intermediate Metabolizer"}
 *
 * <p>The drug name is resolved via the CPIC /drug endpoint using the drugid.
 */
public class CpicDrugRecommendation {

    private String source;                    // always "CPIC"
    private String drugid;                    // e.g. "RxNorm:704"
    private String drugName;                  // resolved from /drug endpoint, e.g. "amitriptyline"
    private Integer guidelineid;              // guideline ID, used for matching with /pair
    private String drugrecommendation;        // clinical recommendation text
    private String classification;            // "Strong", "Moderate", "Optional", etc.
    private Map<String, String> implications; // gene -> implication text
    private Map<String, String> phenotypes;   // gene -> phenotype, e.g. {"CYP2D6": "Intermediate Metabolizer"}
    private String population;                // "general", "pediatrics", etc.
    private boolean dosinginformation;
    private boolean alternatedrugavailable;
    private String comments;

    public CpicDrugRecommendation() {
    }

    public CpicDrugRecommendation(String source, String drugid, String drugName, Integer guidelineid,
                                  String drugrecommendation, String classification,
                                  Map<String, String> implications, Map<String, String> phenotypes,
                                  String population, boolean dosinginformation, boolean alternatedrugavailable,
                                  String comments) {
        this.source = source;
        this.drugid = drugid;
        this.drugName = drugName;
        this.guidelineid = guidelineid;
        this.drugrecommendation = drugrecommendation;
        this.classification = classification;
        this.implications = implications;
        this.phenotypes = phenotypes;
        this.population = population;
        this.dosinginformation = dosinginformation;
        this.alternatedrugavailable = alternatedrugavailable;
        this.comments = comments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicDrugRecommendation{");
        sb.append("source='").append(source).append('\'');
        sb.append(", drugid='").append(drugid).append('\'');
        sb.append(", drugName='").append(drugName).append('\'');
        sb.append(", guidelineid=").append(guidelineid);
        sb.append(", drugrecommendation='").append(drugrecommendation).append('\'');
        sb.append(", classification='").append(classification).append('\'');
        sb.append(", implications=").append(implications);
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", population='").append(population).append('\'');
        sb.append(", dosinginformation=").append(dosinginformation);
        sb.append(", alternatedrugavailable=").append(alternatedrugavailable);
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

    public String getDrugName() {
        return drugName;
    }

    public CpicDrugRecommendation setDrugName(String drugName) {
        this.drugName = drugName;
        return this;
    }

    public Integer getGuidelineid() {
        return guidelineid;
    }

    public CpicDrugRecommendation setGuidelineid(Integer guidelineid) {
        this.guidelineid = guidelineid;
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

    public String getPopulation() {
        return population;
    }

    public CpicDrugRecommendation setPopulation(String population) {
        this.population = population;
        return this;
    }

    public boolean isDosinginformation() {
        return dosinginformation;
    }

    public CpicDrugRecommendation setDosinginformation(boolean dosinginformation) {
        this.dosinginformation = dosinginformation;
        return this;
    }

    public boolean isAlternatedrugavailable() {
        return alternatedrugavailable;
    }

    public CpicDrugRecommendation setAlternatedrugavailable(boolean alternatedrugavailable) {
        this.alternatedrugavailable = alternatedrugavailable;
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
