package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 01/06/16.
 */
public class DiseasePanelAclEntry extends AbstractAclEntry<DiseasePanelAclEntry.DiseasePanelPermissions> {

    public enum DiseasePanelPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public DiseasePanelAclEntry() {
        this("", Collections.emptyList());
    }

    public DiseasePanelAclEntry(String member, EnumSet<DiseasePanelPermissions> permissions) {
        super(member, permissions);
    }

    public DiseasePanelAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(DiseasePanelPermissions.class));

        EnumSet<DiseasePanelPermissions> aux = EnumSet.allOf(DiseasePanelPermissions.class);
        for (DiseasePanelPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public DiseasePanelAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(DiseasePanelPermissions.class));
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(DiseasePanelPermissions::valueOf).collect(Collectors.toList()));
        }
    }

}
