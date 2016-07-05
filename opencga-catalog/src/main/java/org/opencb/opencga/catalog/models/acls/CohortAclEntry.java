package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class CohortAclEntry extends AbstractAclEntry<CohortAclEntry.CohortPermissions> {

    public enum CohortPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE,
        CREATE_ANNOTATIONS,
        VIEW_ANNOTATIONS,
        UPDATE_ANNOTATIONS,
        DELETE_ANNOTATIONS
    }

    public CohortAclEntry() {
        this("", Collections.emptyList());
    }

    public CohortAclEntry(String member, EnumSet<CohortPermissions> permissions) {
        super(member, permissions);
    }

    public CohortAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(CohortPermissions.class));

        EnumSet<CohortPermissions> aux = EnumSet.allOf(CohortPermissions.class);
        for (CohortPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public CohortAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(CohortPermissions.class));

        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(CohortPermissions::valueOf).collect(Collectors.toList()));
        }
    }
}
