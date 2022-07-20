package org.opencb.opencga.core.models.clinical;

import java.util.*;

public enum ClinicalAnalysisPermissions {
    VIEW(Collections.emptyList()),
    WRITE(Collections.singletonList(VIEW)),
    DELETE(Arrays.asList(VIEW, WRITE));

    private List<ClinicalAnalysisPermissions> implicitPermissions;

    ClinicalAnalysisPermissions(List<ClinicalAnalysisPermissions> implicitPermissions) {
        this.implicitPermissions = implicitPermissions;
    }

    public List<ClinicalAnalysisPermissions> getImplicitPermissions() {
        return implicitPermissions;
    }

    public List<ClinicalAnalysisPermissions> getDependentPermissions() {
        List<ClinicalAnalysisPermissions> dependentPermissions = new LinkedList<>();
        for (ClinicalAnalysisPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
            if (permission.getImplicitPermissions().contains(this)) {
                dependentPermissions.add(permission);
            }
        }
        return dependentPermissions;
    }
}
