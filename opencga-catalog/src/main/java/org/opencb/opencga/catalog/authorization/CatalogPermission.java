package org.opencb.opencga.catalog.authorization;

import java.security.acl.Permission;

/**
 * Created on 21/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public enum CatalogPermission implements Permission {
    READ,WRITE,DELETE
}
