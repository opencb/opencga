package org.opencb.opencga.core.models.panel;

import java.util.*;

public enum PanelPermissions {
    VIEW(Collections.emptyList()),
    WRITE(Collections.singletonList(VIEW)),
    DELETE(Arrays.asList(VIEW, WRITE));

    private List<PanelPermissions> implicitPermissions;

    PanelPermissions(List<PanelPermissions> implicitPermissions) {
        this.implicitPermissions = implicitPermissions;
    }

    public List<PanelPermissions> getImplicitPermissions() {
        return implicitPermissions;
    }

    public List<PanelPermissions> getDependentPermissions() {
        List<PanelPermissions> dependentPermissions = new LinkedList<>();
        for (PanelPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
            if (permission.getImplicitPermissions().contains(this)) {
                dependentPermissions.add(permission);
            }
        }
        return dependentPermissions;
    }
}
