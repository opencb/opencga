package org.opencb.opencga.catalog.exceptions;

import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * Created on 18/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuditException extends CatalogException {
    public CatalogAuditException(String message) {
        super(message);
    }

    public CatalogAuditException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogAuditException(Throwable cause) {
        super(cause);
    }
}
