package org.opencb.opencga.catalog.exceptions;

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

    public static CatalogAuthorizationException cantRead(String userId, String resource, long id, String name) {
        return deny(userId, "read", resource, id, name);
    }

    public static CatalogAuthorizationException cantWrite(String userId, String resource, long id, String name) {
        return deny(userId, "write", resource, id, name);
    }

    public static CatalogAuthorizationException cantModify(String userId, String resource, long id, String name) {
        return deny(userId, "modify", resource, id, name);
    }

    public static CatalogAuthorizationException cantExecute(String userId, String resource, long id, String name) {
        return deny(userId, "execute", resource, id, name);
    }

    public static CatalogAuthorizationException deny(String userId, String permission, String resource, long id, String name) {
        return new CatalogAuthorizationException("Permission denied."
                + (userId == null || userId.isEmpty() ? "" : "User \"" + userId + "\"")
                + " Can't " + permission + " "
                + resource + "{ id: " + id + (name == null || name.isEmpty() ? "" : ", name: \"" + name + "\"") + " }");
    }

}
