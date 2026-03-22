package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

import java.util.List;
import java.util.Map;

/**
 * Allele-level information retrieved from the CPIC /allele endpoint.
 * Example: GET https://api.cpicpgx.org/v1/allele?genesymbol=eq.CYP2D6&name=eq.*4
 */
public class CpicAlleleInfo {

    private String genesymbol;
    private String name;
    private String functionalstatus;
    private String clinicalfunctionalstatus;
    private String activityvalue;
    private String strength;                      // evidence strength: "Strong", "Moderate", etc.
    private String findings;                      // functional evidence summary
    private Map<String, Double> frequency;        // population -> frequency, e.g. {"European": 0.185}
    private List<CpicAlleleLocationValue> location; // variant-level locations from /allele_definition endpoint

    public CpicAlleleInfo() {
    }

    public CpicAlleleInfo(String genesymbol, String name, String functionalstatus, String clinicalfunctionalstatus,
                          String activityvalue, String strength, String findings, Map<String, Double> frequency,
                          List<CpicAlleleLocationValue> location) {
        this.genesymbol = genesymbol;
        this.name = name;
        this.functionalstatus = functionalstatus;
        this.clinicalfunctionalstatus = clinicalfunctionalstatus;
        this.activityvalue = activityvalue;
        this.strength = strength;
        this.findings = findings;
        this.frequency = frequency;
        this.location = location;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicAlleleInfo{");
        sb.append("genesymbol='").append(genesymbol).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", functionalstatus='").append(functionalstatus).append('\'');
        sb.append(", clinicalfunctionalstatus='").append(clinicalfunctionalstatus).append('\'');
        sb.append(", activityvalue='").append(activityvalue).append('\'');
        sb.append(", strength='").append(strength).append('\'');
        sb.append(", findings='").append(findings).append('\'');
        sb.append(", frequency=").append(frequency);
        sb.append(", location=").append(location);
        sb.append('}');
        return sb.toString();
    }

    public String getGenesymbol() {
        return genesymbol;
    }

    public CpicAlleleInfo setGenesymbol(String genesymbol) {
        this.genesymbol = genesymbol;
        return this;
    }

    public String getName() {
        return name;
    }

    public CpicAlleleInfo setName(String name) {
        this.name = name;
        return this;
    }

    public String getFunctionalstatus() {
        return functionalstatus;
    }

    public CpicAlleleInfo setFunctionalstatus(String functionalstatus) {
        this.functionalstatus = functionalstatus;
        return this;
    }

    public String getClinicalfunctionalstatus() {
        return clinicalfunctionalstatus;
    }

    public CpicAlleleInfo setClinicalfunctionalstatus(String clinicalfunctionalstatus) {
        this.clinicalfunctionalstatus = clinicalfunctionalstatus;
        return this;
    }

    public String getActivityvalue() {
        return activityvalue;
    }

    public CpicAlleleInfo setActivityvalue(String activityvalue) {
        this.activityvalue = activityvalue;
        return this;
    }

    public String getStrength() {
        return strength;
    }

    public CpicAlleleInfo setStrength(String strength) {
        this.strength = strength;
        return this;
    }

    public String getFindings() {
        return findings;
    }

    public CpicAlleleInfo setFindings(String findings) {
        this.findings = findings;
        return this;
    }

    public Map<String, Double> getFrequency() {
        return frequency;
    }

    public CpicAlleleInfo setFrequency(Map<String, Double> frequency) {
        this.frequency = frequency;
        return this;
    }

    public List<CpicAlleleLocationValue> getLocation() {
        return location;
    }

    public CpicAlleleInfo setLocation(List<CpicAlleleLocationValue> location) {
        this.location = location;
        return this;
    }
}
