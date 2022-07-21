package org.opencb.opencga.core.models.individual;

import java.util.*;

public enum IndividualPermissions {
    NONE(Collections.emptyList()),
    VIEW(Collections.emptyList()),
    WRITE(Collections.singletonList(VIEW)),
    DELETE(Arrays.asList(VIEW, WRITE)),
    VIEW_ANNOTATIONS(Collections.singletonList(VIEW)),
    WRITE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, VIEW)),
    DELETE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, WRITE_ANNOTATIONS, VIEW));

    private final List<IndividualPermissions> implicitPermissions;

    IndividualPermissions(List<IndividualPermissions> implicitPermissions) {
        this.implicitPermissions = implicitPermissions;
    }

    public List<IndividualPermissions> getImplicitPermissions() {
        return implicitPermissions;
    }

    public List<IndividualPermissions> getDependentPermissions() {
        List<IndividualPermissions> dependentPermissions = new LinkedList<>();
        for (IndividualPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
            if (permission.getImplicitPermissions().contains(this)) {
                dependentPermissions.add(permission);
            }
        }
        return dependentPermissions;
    }
}
