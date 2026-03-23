package org.opencb.opencga.core.models.individual;

import org.opencb.opencga.core.models.clinical.pharmacogenomics.PharmacogenomicsAnalysis;

import java.util.List;

public class IndividualAnalysis {

    private List<PharmacogenomicsAnalysis> pharmacogenomics;

    public IndividualAnalysis() {
    }

    public IndividualAnalysis(List<PharmacogenomicsAnalysis> pharmacogenomics) {
        this.pharmacogenomics = pharmacogenomics;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualAnalysis{");
        sb.append("pharmacogenomics=").append(pharmacogenomics);
        sb.append('}');
        return sb.toString();
    }

    public List<PharmacogenomicsAnalysis> getPharmacogenomics() {
        return pharmacogenomics;
    }

    public IndividualAnalysis setPharmacogenomics(List<PharmacogenomicsAnalysis> pharmacogenomics) {
        this.pharmacogenomics = pharmacogenomics;
        return this;
    }
}
