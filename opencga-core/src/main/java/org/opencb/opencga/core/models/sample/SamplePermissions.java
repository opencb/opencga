package org.opencb.opencga.core.models.sample;

import java.util.*;

public enum SamplePermissions {
    NONE(Collections.emptyList()),
    VIEW(Collections.emptyList()),
    WRITE(Collections.singletonList(VIEW)),
    DELETE(Arrays.asList(VIEW, WRITE)),
    VIEW_ANNOTATIONS(Collections.singletonList(VIEW)),
    WRITE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, VIEW)),
    DELETE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, WRITE_ANNOTATIONS, VIEW)),
    VIEW_VARIANTS(Arrays.asList(VIEW, VIEW_ANNOTATIONS));

    private final List<SamplePermissions> implicitPermissions;

    SamplePermissions(List<SamplePermissions> implicitPermissions) {
        this.implicitPermissions = implicitPermissions;
    }

    public List<SamplePermissions> getImplicitPermissions() {
        return implicitPermissions;
    }

    public List<SamplePermissions> getDependentPermissions() {
        List<SamplePermissions> dependentPermissions = new LinkedList<>();
        for (SamplePermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
            if (permission.getImplicitPermissions().contains(this)) {
                dependentPermissions.add(permission);
            }
        }
        return dependentPermissions;
    }
}
