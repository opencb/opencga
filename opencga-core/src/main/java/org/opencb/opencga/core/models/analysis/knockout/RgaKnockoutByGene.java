package org.opencb.opencga.core.models.analysis.knockout;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class RgaKnockoutByGene extends KnockoutByGene {

    @DataField(description = ParamConstants.RGA_KNOCKOUT_BY_GENE_NUM_INDIVIDUALS_DESCRIPTION)
    private int numIndividuals;
    @DataField(description = ParamConstants.RGA_KNOCKOUT_BY_GENE_HAS_NEXT_INDIVIDUAL_DESCRIPTION)
    private boolean hasNextIndividual;

    public RgaKnockoutByGene() {
    }

    public RgaKnockoutByGene(int numIndividuals, boolean hasNextIndividual) {
        this.numIndividuals = numIndividuals;
        this.hasNextIndividual = hasNextIndividual;
    }

    public int getNumIndividuals() {
        return numIndividuals;
    }

    public RgaKnockoutByGene setNumIndividuals(int numIndividuals) {
        this.numIndividuals = numIndividuals;
        return this;
    }

    public boolean isHasNextIndividual() {
        return hasNextIndividual;
    }

    public RgaKnockoutByGene setHasNextIndividual(boolean hasNextIndividual) {
        this.hasNextIndividual = hasNextIndividual;
        return this;
    }
}
