package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

@Deprecated
public class InterpretationMergeParams {

    @DataField(description = ParamConstants.INTERPRETATION_MERGE_PARAMS_METHOD_DESCRIPTION)
    private InterpretationMethod method;
    @DataField(description = ParamConstants.INTERPRETATION_MERGE_PARAMS_PRIMARY_FINDINGS_DESCRIPTION)
    private List<ClinicalVariant> primaryFindings;
    @DataField(description = ParamConstants.INTERPRETATION_MERGE_PARAMS_SECONDARY_FINDINGS_DESCRIPTION)
    private List<ClinicalVariant> secondaryFindings;

    public InterpretationMergeParams() {
    }

    public InterpretationMergeParams(InterpretationMethod method, List<ClinicalVariant> primaryFindings,
                                     List<ClinicalVariant> secondaryFindings) {
        this.method = method;
        this.primaryFindings = primaryFindings;
        this.secondaryFindings = secondaryFindings;
    }

    public Interpretation toInterpretation() {
        return new Interpretation()
                .setMethod(method)
                .setPrimaryFindings(primaryFindings)
                .setSecondaryFindings(secondaryFindings);
    }

    public InterpretationMethod getMethod() {
        return method;
    }

    public InterpretationMergeParams setMethod(InterpretationMethod method) {
        this.method = method;
        return this;
    }

    public List<ClinicalVariant> getPrimaryFindings() {
        return primaryFindings;
    }

    public InterpretationMergeParams setPrimaryFindings(List<ClinicalVariant> primaryFindings) {
        this.primaryFindings = primaryFindings;
        return this;
    }

    public List<ClinicalVariant> getSecondaryFindings() {
        return secondaryFindings;
    }

    public InterpretationMergeParams setSecondaryFindings(List<ClinicalVariant> secondaryFindings) {
        this.secondaryFindings = secondaryFindings;
        return this;
    }
}
