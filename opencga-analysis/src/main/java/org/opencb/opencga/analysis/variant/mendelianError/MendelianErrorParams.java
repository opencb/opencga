package org.opencb.opencga.analysis.variant.mendelianError;

import org.opencb.opencga.core.tools.ToolParams;

public class MendelianErrorParams extends ToolParams {

    private String familyId;
    private String individualId;
    private String sampleId;

    public MendelianErrorParams() {
    }

    public MendelianErrorParams(String familyId, String individualId, String sampleId) {
        this.familyId = familyId;
        this.individualId = individualId;
        this.sampleId = sampleId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MendelianErrorParams{");
        sb.append("familyId='").append(familyId).append('\'');
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", sampleId='").append(sampleId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFamilyId() {
        return familyId;
    }

    public MendelianErrorParams setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public MendelianErrorParams setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public MendelianErrorParams setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }
}
