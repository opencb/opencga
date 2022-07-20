package org.opencb.opencga.core.models.family;

import org.opencb.opencga.core.models.AclParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FamilyAclParams extends AclParams {

    @DataField(description = ParamConstants.FAMILY_ACL_PARAMS_FAMILY_DESCRIPTION)
    private String family;
    @DataField(description = ParamConstants.FAMILY_ACL_PARAMS_INDIVIDUAL_DESCRIPTION)
    private String individual;
    @DataField(description = ParamConstants.FAMILY_ACL_PARAMS_SAMPLE_DESCRIPTION)
    private String sample;
    @DataField(description = ParamConstants.FAMILY_ACL_PARAMS_PROPAGATE_DESCRIPTION)
    private Propagate propagate;

    public enum Propagate {
        NO,                    // Does not propagate permissions to related individuals and samples
        YES,                   // Propagates permissions to related individuals and samples
        YES_AND_VARIANT_VIEW   // Propagates permissions to related individuals and samples adding VARIANT_VIEW permission to Samples
    }

    public FamilyAclParams() {
    }

    public FamilyAclParams(String permissions, String family, String individual, String sample, Propagate propagate) {
        super(permissions);
        this.family = family;
        this.individual = individual;
        this.sample = sample;
        this.propagate = propagate;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyAclParams{");
        sb.append("family='").append(family).append('\'');
        sb.append(", individual='").append(individual).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append(", propagate=").append(propagate);
        sb.append('}');
        return sb.toString();
    }

    public String getFamily() {
        return family;
    }

    public FamilyAclParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getIndividual() {
        return individual;
    }

    public FamilyAclParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public FamilyAclParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public Propagate getPropagate() {
        return propagate;
    }

    public FamilyAclParams setPropagate(Propagate propagate) {
        this.propagate = propagate;
        return this;
    }
}
