package org.opencb.opencga.catalog.audit;

import org.opencb.datastore.core.QueryParam;

/**
 * Created on 18/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
enum AuditQueryParam implements QueryParam {
    id("id", "", Type.TEXT),
    timeStamp("timeStamp", "", Type.TEXT),
    resource("resource", "", Type.TEXT),
    action("action", "", Type.TEXT),
    userId("userId", "", Type.TEXT),
    ;

    private final String _key;
    private final String _description;
    private final Type _type;

    AuditQueryParam(String key, String description, Type type) {
        _key = key;
        _description = description;
        _type = type;
    }

    @Override public String key() { return _key;}
    @Override public String description() { return _description;}
    @Override public Type type() { return _type;}
}
