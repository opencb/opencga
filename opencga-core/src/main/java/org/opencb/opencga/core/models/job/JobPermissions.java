package org.opencb.opencga.core.models.job;

import java.util.*;

public enum JobPermissions {
    VIEW(Collections.emptyList()),
    WRITE(Collections.singletonList(VIEW)),
    DELETE(Arrays.asList(VIEW, WRITE));

    private List<JobPermissions> implicitPermissions;

    JobPermissions(List<JobPermissions> implicitPermissions) {
        this.implicitPermissions = implicitPermissions;
    }

    public List<JobPermissions> getImplicitPermissions() {
        return implicitPermissions;
    }

    public List<JobPermissions> getDependentPermissions() {
        List<JobPermissions> dependentPermissions = new LinkedList<>();
        for (JobPermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
            if (permission.getImplicitPermissions().contains(this)) {
                dependentPermissions.add(permission);
            }
        }
        return dependentPermissions;
    }
}
