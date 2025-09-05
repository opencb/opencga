package org.opencb.opencga.core.models.externalTool;

import java.util.*;

public enum ExternalToolPermissions {
    NONE(Collections.emptyList()),
    VIEW(Collections.emptyList()),
    WRITE(Collections.singletonList(VIEW)),
    DELETE(Arrays.asList(VIEW, WRITE));

    private final List<ExternalToolPermissions> implicitPermissions;

    ExternalToolPermissions(List<ExternalToolPermissions> implicitPermissions) {
        this.implicitPermissions = implicitPermissions;
    }

    public List<ExternalToolPermissions> getImplicitPermissions() {
        return implicitPermissions;
    }

    public List<ExternalToolPermissions> getDependentPermissions() {
        List<ExternalToolPermissions> dependentPermissions = new LinkedList<>();
        for (ExternalToolPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
            if (permission.getImplicitPermissions().contains(this)) {
                dependentPermissions.add(permission);
            }
        }
        return dependentPermissions;
    }
}
