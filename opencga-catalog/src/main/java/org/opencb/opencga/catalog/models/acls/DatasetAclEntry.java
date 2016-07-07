package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 12/05/16.
 */
public class DatasetAclEntry extends AbstractAclEntry<DatasetAclEntry.DatasetPermissions> {

    public enum DatasetPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public DatasetAclEntry() {
        this("", Collections.emptyList());
    }

    public DatasetAclEntry(String member, EnumSet<DatasetPermissions> permissions) {
        super(member, permissions);
    }

    public DatasetAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(DatasetPermissions.class));

        EnumSet<DatasetPermissions> aux = EnumSet.allOf(DatasetPermissions.class);
        for (DatasetPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public DatasetAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(DatasetPermissions.class));

        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(DatasetPermissions::valueOf).collect(Collectors.toList()));
        }
    }

}
