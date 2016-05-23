package org.opencb.opencga.catalog.db.api;

/**
 * Created by pfurio on 23/05/16.
 */

import org.opencb.opencga.catalog.exceptions.CatalogDBException;

public interface CatalogMetaDBAdaptor {

    boolean isRegisterOpen();

    String getAdminPassword() throws CatalogDBException;

}
