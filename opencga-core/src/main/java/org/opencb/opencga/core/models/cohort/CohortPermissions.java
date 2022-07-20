package org.opencb.opencga.core.models.cohort;

import java.util.*;

public enum CohortPermissions {
    VIEW(Collections.emptyList()),
    WRITE(Collections.singletonList(VIEW)),
    DELETE(Arrays.asList(VIEW, WRITE)),
    VIEW_ANNOTATIONS(Collections.singletonList(VIEW)),
    WRITE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, VIEW)),
    DELETE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, WRITE_ANNOTATIONS, VIEW));

    private List<CohortPermissions> implicitPermissions;

    CohortPermissions(List<CohortPermissions> implicitPermissions) {
        this.implicitPermissions = implicitPermissions;
    }

    public List<CohortPermissions> getImplicitPermissions() {
        return implicitPermissions;
    }

    public List<CohortPermissions> getDependentPermissions() {
        List<CohortPermissions> dependentPermissions = new LinkedList<>();
        for (CohortPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
            if (permission.getImplicitPermissions().contains(this)) {
                dependentPermissions.add(permission);
            }
        }
        return dependentPermissions;
    }
}
