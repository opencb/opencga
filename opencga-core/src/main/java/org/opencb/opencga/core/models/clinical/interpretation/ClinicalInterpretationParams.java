package org.opencb.opencga.core.models.clinical.interpretation;

import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class ClinicalInterpretationParams extends ToolParams {

    @DataField(id = "penetrance", description = FieldConstants.PENETRANCE_DESCRIPTION)
    private String penetrance = ClinicalProperty.Penetrance.COMPLETE.toString();

    @DataField(id = "discardUntieredEvidences", description = FieldConstants.DISCARD_UNTIERED_EVIDENCE_DESCRIPTION, defaultValue = "true")
    private Boolean discardUntieredEvidences;

    @DataField(id = "oneConsequencePerEvidence", description = FieldConstants.ONE_CONSEQUENCE_PER_EVIDENCE_DESCRIPTION,
            defaultValue = "false")
    private Boolean oneConsequencePerEvidence;

    public ClinicalInterpretationParams() {
    }

    public ClinicalInterpretationParams(String penetrance, Boolean discardUntieredEvidences, Boolean oneConsequencePerEvidence) {
        this.penetrance = penetrance;
        this.discardUntieredEvidences = discardUntieredEvidences;
        this.oneConsequencePerEvidence = oneConsequencePerEvidence;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalInterpretationParams{");
        sb.append("penetrance='").append(penetrance).append('\'');
        sb.append(", discardUntieredEvidences=").append(discardUntieredEvidences);
        sb.append(", oneConsequencePerEvidence=").append(oneConsequencePerEvidence);
        sb.append('}');
        return sb.toString();
    }

    public String getPenetrance() {
        return penetrance;
    }

    public ClinicalInterpretationParams setPenetrance(String penetrance) {
        this.penetrance = penetrance;
        return this;
    }

    public Boolean getDiscardUntieredEvidences() {
        return discardUntieredEvidences;
    }

    public ClinicalInterpretationParams setDiscardUntieredEvidences(Boolean discardUntieredEvidences) {
        this.discardUntieredEvidences = discardUntieredEvidences;
        return this;
    }

    public Boolean getOneConsequencePerEvidence() {
        return oneConsequencePerEvidence;
    }

    public ClinicalInterpretationParams setOneConsequencePerEvidence(Boolean oneConsequencePerEvidence) {
        this.oneConsequencePerEvidence = oneConsequencePerEvidence;
        return this;
    }
}
