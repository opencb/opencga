package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;

import java.util.List;

@Deprecated
public class InterpretationMergeParams {

    private InterpretationMethod method;
    private List<ClinicalVariant> primaryFindings;
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
