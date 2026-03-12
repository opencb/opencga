package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

/**
 * Allele-level information retrieved from the CPIC /allele endpoint.
 * Example: GET https://api.cpicpgx.org/v1/allele?genesymbol=eq.CYP2C9&name=eq.*2
 */
public class CpicAlleleInfo {

    private String genesymbol;
    private String name;
    private String functionalstatus;
    private String activityvalue;
    private String clinicalfunctionalstatus;

    public CpicAlleleInfo() {
    }

    public CpicAlleleInfo(String genesymbol, String name, String functionalstatus, String activityvalue,
                          String clinicalfunctionalstatus) {
        this.genesymbol = genesymbol;
        this.name = name;
        this.functionalstatus = functionalstatus;
        this.activityvalue = activityvalue;
        this.clinicalfunctionalstatus = clinicalfunctionalstatus;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicAlleleInfo{");
        sb.append("genesymbol='").append(genesymbol).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", functionalstatus='").append(functionalstatus).append('\'');
        sb.append(", activityvalue='").append(activityvalue).append('\'');
        sb.append(", clinicalfunctionalstatus='").append(clinicalfunctionalstatus).append('\'');
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

    public String getActivityvalue() {
        return activityvalue;
    }

    public CpicAlleleInfo setActivityvalue(String activityvalue) {
        this.activityvalue = activityvalue;
        return this;
    }

    public String getClinicalfunctionalstatus() {
        return clinicalfunctionalstatus;
    }

    public CpicAlleleInfo setClinicalfunctionalstatus(String clinicalfunctionalstatus) {
        this.clinicalfunctionalstatus = clinicalfunctionalstatus;
        return this;
    }
}
