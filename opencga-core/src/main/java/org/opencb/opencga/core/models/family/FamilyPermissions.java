package org.opencb.opencga.core.models.family;

import java.util.*;

public enum FamilyPermissions {
    NONE(Collections.emptyList()),
    VIEW(Collections.emptyList()),
    WRITE(Collections.singletonList(VIEW)),
    DELETE(Arrays.asList(VIEW, WRITE)),
    VIEW_ANNOTATIONS(Collections.singletonList(VIEW)),
    WRITE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, VIEW)),
    DELETE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, WRITE_ANNOTATIONS, VIEW));

    private final List<FamilyPermissions> implicitPermissions;

    FamilyPermissions(List<FamilyPermissions> implicitPermissions) {
        this.implicitPermissions = implicitPermissions;
    }

    public List<FamilyPermissions> getImplicitPermissions() {
        return implicitPermissions;
    }

    public List<FamilyPermissions> getDependentPermissions() {
        List<FamilyPermissions> dependentPermissions = new LinkedList<>();
        for (FamilyPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
            if (permission.getImplicitPermissions().contains(this)) {
                dependentPermissions.add(permission);
            }
        }
        return dependentPermissions;
    }
}
