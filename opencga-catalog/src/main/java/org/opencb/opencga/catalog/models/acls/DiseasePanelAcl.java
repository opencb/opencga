package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 01/06/16.
 */
public class DiseasePanelAcl extends AbstractAcl<DiseasePanelAcl.DiseasePanelPermissions> {

    public enum DiseasePanelPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public DiseasePanelAcl() {
        this("", Collections.emptyList());
    }

    public DiseasePanelAcl(String member, EnumSet<DiseasePanelPermissions> permissions) {
        super(member, permissions);
    }

    public DiseasePanelAcl(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(DiseasePanelPermissions.class));

        EnumSet<DiseasePanelPermissions> aux = EnumSet.allOf(DiseasePanelPermissions.class);
        for (DiseasePanelPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public DiseasePanelAcl(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(DiseasePanelPermissions.class));
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(DiseasePanelPermissions::valueOf).collect(Collectors.toList()));
        }
    }

}
