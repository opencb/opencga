package org.opencb.opencga.catalog.authorization;

import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.db.CatalogDBException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthorizationException extends CatalogException {
    public CatalogAuthorizationException(String message) {
        super(message);
    }

    public CatalogAuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogAuthorizationException(Throwable cause) {
        super(cause);
    }

    public static CatalogAuthorizationException cantRead(String resource, int id, String name) {
        return denny("read", resource, id, name);
    }

    public static CatalogAuthorizationException cantWrite(String resource, int id, String name) {
        return denny("write", resource, id, name);
    }

    public static CatalogAuthorizationException cantModify(String resource, int id, String name) {
        return denny("modify", resource, id, name);
    }

    public static CatalogAuthorizationException cantExecute(String resource, int id, String name) {
        return denny("execute", resource, id, name);
    }

    public static CatalogAuthorizationException denny(String permission, String resource, int id, String name) {
        return new CatalogAuthorizationException("Permission denied. Can't " + permission + " " +
                resource + "{ id: " + id + (name == null || name.isEmpty()? "" : ", name: \"" + name + "\"") + " }");
    }

}
