package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

import java.util.List;

/**
 * Drug-gene pair retrieved from the CPIC /pair endpoint, enriched with the resolved
 * drug name (from /drug) and matching dosing recommendations (from /recommendation).
 *
 * <p>Example: GET https://api.cpicpgx.org/v1/pair?genesymbol=eq.CYP2C9
 */
public class CpicDrug {

    private String drugid;                // e.g. "RxNorm:140587"
    private String drugName;              // resolved from /drug, e.g. "celecoxib"
    private String genesymbol;
    private Integer guidelineid;
    private String cpiclevel;             // "A", "B", "C", "D"
    private String pgkbcalevel;           // "1A", "1B", "2A", "2B", "3", "4"
    private String pgxtesting;            // "Actionable PGx", "Informative PGx", etc.
    private boolean usedforrecommendation;

    private List<CpicDrugRecommendation> recommendations;

    public CpicDrug() {
    }

    public CpicDrug(String drugid, String drugName, String genesymbol, Integer guidelineid, String cpiclevel,
                    String pgkbcalevel, String pgxtesting, boolean usedforrecommendation,
                    List<CpicDrugRecommendation> recommendations) {
        this.drugid = drugid;
        this.drugName = drugName;
        this.genesymbol = genesymbol;
        this.guidelineid = guidelineid;
        this.cpiclevel = cpiclevel;
        this.pgkbcalevel = pgkbcalevel;
        this.pgxtesting = pgxtesting;
        this.usedforrecommendation = usedforrecommendation;
        this.recommendations = recommendations;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicDrug{");
        sb.append("drugid='").append(drugid).append('\'');
        sb.append(", drugName='").append(drugName).append('\'');
        sb.append(", genesymbol='").append(genesymbol).append('\'');
        sb.append(", guidelineid=").append(guidelineid);
        sb.append(", cpiclevel='").append(cpiclevel).append('\'');
        sb.append(", pgkbcalevel='").append(pgkbcalevel).append('\'');
        sb.append(", pgxtesting='").append(pgxtesting).append('\'');
        sb.append(", usedforrecommendation=").append(usedforrecommendation);
        sb.append(", recommendations=").append(recommendations);
        sb.append('}');
        return sb.toString();
    }

    public String getDrugid() {
        return drugid;
    }

    public CpicDrug setDrugid(String drugid) {
        this.drugid = drugid;
        return this;
    }

    public String getDrugName() {
        return drugName;
    }

    public CpicDrug setDrugName(String drugName) {
        this.drugName = drugName;
        return this;
    }

    public String getGenesymbol() {
        return genesymbol;
    }

    public CpicDrug setGenesymbol(String genesymbol) {
        this.genesymbol = genesymbol;
        return this;
    }

    public Integer getGuidelineid() {
        return guidelineid;
    }

    public CpicDrug setGuidelineid(Integer guidelineid) {
        this.guidelineid = guidelineid;
        return this;
    }

    public String getCpiclevel() {
        return cpiclevel;
    }

    public CpicDrug setCpiclevel(String cpiclevel) {
        this.cpiclevel = cpiclevel;
        return this;
    }

    public String getPgkbcalevel() {
        return pgkbcalevel;
    }

    public CpicDrug setPgkbcalevel(String pgkbcalevel) {
        this.pgkbcalevel = pgkbcalevel;
        return this;
    }

    public String getPgxtesting() {
        return pgxtesting;
    }

    public CpicDrug setPgxtesting(String pgxtesting) {
        this.pgxtesting = pgxtesting;
        return this;
    }

    public boolean isUsedforrecommendation() {
        return usedforrecommendation;
    }

    public CpicDrug setUsedforrecommendation(boolean usedforrecommendation) {
        this.usedforrecommendation = usedforrecommendation;
        return this;
    }

    public List<CpicDrugRecommendation> getRecommendations() {
        return recommendations;
    }

    public CpicDrug setRecommendations(List<CpicDrugRecommendation> recommendations) {
        this.recommendations = recommendations;
        return this;
    }
}
