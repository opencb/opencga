package org.opencb.opencga.core.models.workflow;

import java.util.*;

public enum WorkflowPermissions {
    NONE(Collections.emptyList()),
    VIEW(Collections.emptyList()),
    WRITE(Collections.singletonList(VIEW)),
    DELETE(Arrays.asList(VIEW, WRITE));

    private final List<WorkflowPermissions> implicitPermissions;

    WorkflowPermissions(List<WorkflowPermissions> implicitPermissions) {
        this.implicitPermissions = implicitPermissions;
    }

    public List<WorkflowPermissions> getImplicitPermissions() {
        return implicitPermissions;
    }

    public List<WorkflowPermissions> getDependentPermissions() {
        List<WorkflowPermissions> dependentPermissions = new LinkedList<>();
        for (WorkflowPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
            if (permission.getImplicitPermissions().contains(this)) {
                dependentPermissions.add(permission);
            }
        }
        return dependentPermissions;
    }
}
