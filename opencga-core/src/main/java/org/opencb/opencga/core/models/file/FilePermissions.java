package org.opencb.opencga.core.models.file;

import java.util.*;

public enum FilePermissions {
    NONE(Collections.emptyList()),
    VIEW(Collections.emptyList()),
    VIEW_HEADER(Collections.singletonList(VIEW)),  // Includes permission to view the sample ids from a VCF file.,
    VIEW_CONTENT(Collections.singletonList(VIEW)),
    WRITE(Collections.singletonList(VIEW)),       // If a folder contains this permission for a user, the user will be able to create files under that folder.
    DELETE(Arrays.asList(VIEW, WRITE)),
    DOWNLOAD(Collections.singletonList(VIEW)),
    UPLOAD(Arrays.asList(VIEW, WRITE)),
    VIEW_ANNOTATIONS(Collections.singletonList(VIEW)),
    WRITE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, VIEW)),
    DELETE_ANNOTATIONS(Arrays.asList(VIEW_ANNOTATIONS, WRITE_ANNOTATIONS, VIEW));

    private final List<FilePermissions> implicitPermissions;

    FilePermissions(List<FilePermissions> implicitPermissions) {
        this.implicitPermissions = implicitPermissions;
    }

    public List<FilePermissions> getImplicitPermissions() {
        return implicitPermissions;
    }

    public List<FilePermissions> getDependentPermissions() {
        List<FilePermissions> dependentPermissions = new LinkedList<>();
        for (FilePermissions permission : EnumSet.complementOf(EnumSet.of(this))) {
            if (permission.getImplicitPermissions().contains(this)) {
                dependentPermissions.add(permission);
            }
        }
        return dependentPermissions;
    }
}
