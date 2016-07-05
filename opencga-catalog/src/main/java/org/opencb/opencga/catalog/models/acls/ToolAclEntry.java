package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by sgallego on 6/30/16.
 */
public class ToolAclEntry extends AbstractAclEntry<ToolAclEntry.ToolPermissions> {

    public enum ToolPermissions {
        EXECUTE,
        UPDATE,
        DELETE,
        SHARE
    }

    public ToolAclEntry() {
        this("", Collections.emptyList());
    }

    public ToolAclEntry(String member, EnumSet<ToolPermissions> permissions) {
        super(member, permissions);
    }

    public ToolAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(ToolPermissions.class));

        EnumSet<ToolPermissions> aux = EnumSet.allOf(ToolPermissions.class);
        for (ToolPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public ToolAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(ToolPermissions.class));
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(ToolPermissions::valueOf).collect(Collectors.toList()));
        }
    }

}

