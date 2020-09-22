package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;

import java.util.List;

public class InterpretationMergeParams {

    private List<InterpretationMethod> methods;
    private List<ClinicalVariant> primaryFindings;
    private List<ClinicalVariant> secondaryFindings;

    public InterpretationMergeParams() {
    }

    public InterpretationMergeParams(List<InterpretationMethod> methods, List<ClinicalVariant> primaryFindings,
                                     List<ClinicalVariant> secondaryFindings) {
        this.methods = methods;
        this.primaryFindings = primaryFindings;
        this.secondaryFindings = secondaryFindings;
    }

    public Interpretation toInterpretation() {
        return new Interpretation()
                .setMethods(methods)
                .setPrimaryFindings(primaryFindings)
                .setSecondaryFindings(secondaryFindings);
    }

    public List<InterpretationMethod> getMethods() {
        return methods;
    }

    public InterpretationMergeParams setMethods(List<InterpretationMethod> methods) {
        this.methods = methods;
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
