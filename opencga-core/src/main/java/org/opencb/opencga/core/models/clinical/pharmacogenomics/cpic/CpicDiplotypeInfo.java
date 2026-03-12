package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

import java.util.Map;

public class CpicDiplotypeInfo {

    // Core diplotype fields
    private String genesymbol;
    private String diplotype;
    private String function1;
    private String function2;
    private String activityvalue1;
    private String activityvalue2;
    private String totalactivityscore;
    private String description;

    // Phenotype classification (from diplotype_phenotype_table)
    private String generesult;       // e.g. "Intermediate Metabolizer"
    private String ehrpriority;      // e.g. "Abnormal/Priority/High Risk"
    private String consultationtext; // long CDS text for EHR display

    // Keys used for recommendation lookups
    private Map<String, Object> diplotypekey; // e.g. {"CYP2C9": {"*1": 1, "*6": 1}}
    private Map<String, String> lookupkey;    // e.g. {"CYP2C9": "1.0"}  ← used to query /recommendation

    public CpicDiplotypeInfo() {
    }

    public CpicDiplotypeInfo(String genesymbol, String diplotype, String function1, String function2, String activityvalue1,
                             String activityvalue2, String totalactivityscore, String description, String generesult, String ehrpriority,
                             String consultationtext, Map<String, Object> diplotypekey, Map<String, String> lookupkey) {
        this.genesymbol = genesymbol;
        this.diplotype = diplotype;
        this.function1 = function1;
        this.function2 = function2;
        this.activityvalue1 = activityvalue1;
        this.activityvalue2 = activityvalue2;
        this.totalactivityscore = totalactivityscore;
        this.description = description;
        this.generesult = generesult;
        this.ehrpriority = ehrpriority;
        this.consultationtext = consultationtext;
        this.diplotypekey = diplotypekey;
        this.lookupkey = lookupkey;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicDiplotypeInfo{");
        sb.append("genesymbol='").append(genesymbol).append('\'');
        sb.append(", diplotype='").append(diplotype).append('\'');
        sb.append(", function1='").append(function1).append('\'');
        sb.append(", function2='").append(function2).append('\'');
        sb.append(", activityvalue1='").append(activityvalue1).append('\'');
        sb.append(", activityvalue2='").append(activityvalue2).append('\'');
        sb.append(", totalactivityscore='").append(totalactivityscore).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", generesult='").append(generesult).append('\'');
        sb.append(", ehrpriority='").append(ehrpriority).append('\'');
        sb.append(", consultationtext='").append(consultationtext).append('\'');
        sb.append(", diplotypekey=").append(diplotypekey);
        sb.append(", lookupkey=").append(lookupkey);
        sb.append('}');
        return sb.toString();
    }

    public String getGenesymbol() {
        return genesymbol;
    }

    public CpicDiplotypeInfo setGenesymbol(String genesymbol) {
        this.genesymbol = genesymbol;
        return this;
    }

    public String getDiplotype() {
        return diplotype;
    }

    public CpicDiplotypeInfo setDiplotype(String diplotype) {
        this.diplotype = diplotype;
        return this;
    }

    public String getFunction1() {
        return function1;
    }

    public CpicDiplotypeInfo setFunction1(String function1) {
        this.function1 = function1;
        return this;
    }

    public String getFunction2() {
        return function2;
    }

    public CpicDiplotypeInfo setFunction2(String function2) {
        this.function2 = function2;
        return this;
    }

    public String getActivityvalue1() {
        return activityvalue1;
    }

    public CpicDiplotypeInfo setActivityvalue1(String activityvalue1) {
        this.activityvalue1 = activityvalue1;
        return this;
    }

    public String getActivityvalue2() {
        return activityvalue2;
    }

    public CpicDiplotypeInfo setActivityvalue2(String activityvalue2) {
        this.activityvalue2 = activityvalue2;
        return this;
    }

    public String getTotalactivityscore() {
        return totalactivityscore;
    }

    public CpicDiplotypeInfo setTotalactivityscore(String totalactivityscore) {
        this.totalactivityscore = totalactivityscore;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CpicDiplotypeInfo setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getGeneresult() {
        return generesult;
    }

    public CpicDiplotypeInfo setGeneresult(String generesult) {
        this.generesult = generesult;
        return this;
    }

    public String getEhrpriority() {
        return ehrpriority;
    }

    public CpicDiplotypeInfo setEhrpriority(String ehrpriority) {
        this.ehrpriority = ehrpriority;
        return this;
    }

    public String getConsultationtext() {
        return consultationtext;
    }

    public CpicDiplotypeInfo setConsultationtext(String consultationtext) {
        this.consultationtext = consultationtext;
        return this;
    }

    public Map<String, Object> getDiplotypekey() {
        return diplotypekey;
    }

    public CpicDiplotypeInfo setDiplotypekey(Map<String, Object> diplotypekey) {
        this.diplotypekey = diplotypekey;
        return this;
    }

    public Map<String, String> getLookupkey() {
        return lookupkey;
    }

    public CpicDiplotypeInfo setLookupkey(Map<String, String> lookupkey) {
        this.lookupkey = lookupkey;
        return this;
    }
}
