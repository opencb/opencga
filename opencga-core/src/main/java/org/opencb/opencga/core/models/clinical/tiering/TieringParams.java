package org.opencb.opencga.core.models.clinical.tiering;

import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

@Deprecated
public class TieringParams {

    @DataField(id = "penetrance", description = FieldConstants.PENETRANCE_DESCRIPTION)
    private String penetrance = ClinicalProperty.Penetrance.COMPLETE.toString();

    public TieringParams() {
    }

    public TieringParams(String penetrance) {
        this.penetrance = penetrance;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TieringParams{");
        sb.append("penetrance=").append(penetrance);
        sb.append('}');
        return sb.toString();
    }

    public String getPenetrance() {
        return penetrance;
    }

    public TieringParams setPenetrance(String penetrance) {
        this.penetrance = penetrance;
        return this;
    }
}
