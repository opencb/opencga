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

    public static CatalogAuthorizationException cantRead(String userId, String resource, int id, String name) {
        return denny(userId, "read", resource, id, name);
    }

    public static CatalogAuthorizationException cantWrite(String userId, String resource, int id, String name) {
        return denny(userId, "write", resource, id, name);
    }

    public static CatalogAuthorizationException cantModify(String userId, String resource, int id, String name) {
        return denny(userId, "modify", resource, id, name);
    }

    public static CatalogAuthorizationException cantExecute(String userId, String resource, int id, String name) {
        return denny(userId, "execute", resource, id, name);
    }

    public static CatalogAuthorizationException denny(String userId, String permission, String resource, int id, String name) {
        return new CatalogAuthorizationException("Permission denied." +
                (userId == null || userId.isEmpty()? "" : "User \"" + userId + "\"") +
                " Can't " + permission + " " +
                resource + "{ id: " + id + (name == null || name.isEmpty()? "" : ", name: \"" + name + "\"") + " }");
    }

}
