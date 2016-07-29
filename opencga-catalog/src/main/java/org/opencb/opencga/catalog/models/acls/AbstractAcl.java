package org.opencb.opencga.catalog.models.acls;

import org.opencb.opencga.catalog.models.acls.permissions.AbstractAclEntry;

import java.util.List;

/**
 * Created by pfurio on 29/07/16.
 */
public abstract class AbstractAcl<T extends AbstractAclEntry> {

    protected List<T> acl;

    public List<T> getAcl() {
        return acl;
    }

}
