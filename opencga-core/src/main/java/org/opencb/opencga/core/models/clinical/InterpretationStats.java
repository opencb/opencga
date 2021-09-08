package org.opencb.opencga.core.models.clinical;

public class InterpretationStats {

    private InterpretationFindingStats primaryFindings;
    private InterpretationFindingStats secondaryFindings;

    public InterpretationStats() {
    }

    public InterpretationStats(InterpretationFindingStats primaryFindings, InterpretationFindingStats secondaryFindings) {
        this.primaryFindings = primaryFindings;
        this.secondaryFindings = secondaryFindings;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationStats{");
        sb.append("primaryFindings=").append(primaryFindings);
        sb.append(", secondaryFindings=").append(secondaryFindings);
        sb.append('}');
        return sb.toString();
    }

    public InterpretationFindingStats getPrimaryFindings() {
        return primaryFindings;
    }

    public InterpretationStats setPrimaryFindings(InterpretationFindingStats primaryFindings) {
        this.primaryFindings = primaryFindings;
        return this;
    }

    public InterpretationFindingStats getSecondaryFindings() {
        return secondaryFindings;
    }

    public InterpretationStats setSecondaryFindings(InterpretationFindingStats secondaryFindings) {
        this.secondaryFindings = secondaryFindings;
        return this;
    }
}
