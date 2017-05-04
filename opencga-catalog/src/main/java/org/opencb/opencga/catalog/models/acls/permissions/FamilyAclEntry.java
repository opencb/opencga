package org.opencb.opencga.catalog.models.acls.permissions;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 02/05/17.
 */
public class FamilyAclEntry extends AbstractAclEntry<FamilyAclEntry.FamilyPermissions> {

    public enum FamilyPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE,
        WRITE_ANNOTATIONS,
        VIEW_ANNOTATIONS,
        DELETE_ANNOTATIONS
    }

    public FamilyAclEntry() {
        this("", Collections.emptyList());
    }

    public FamilyAclEntry(String member, EnumSet<FamilyPermissions> permissions) {
        super(member, permissions);
    }

    public FamilyAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(FamilyPermissions.class));

        EnumSet<FamilyPermissions> aux = EnumSet.allOf(FamilyPermissions.class);
        for (FamilyPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public FamilyAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(FamilyPermissions.class));
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(FamilyPermissions::valueOf).collect(Collectors.toList()));
        }
    }
}
