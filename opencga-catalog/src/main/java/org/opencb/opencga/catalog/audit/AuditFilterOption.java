package org.opencb.opencga.catalog.audit;

//import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor.FilterOption;
//
//import static org.opencb.opencga.catalog.db.api.CatalogDBAdaptor.FilterOption.Type.TEXT;

import org.opencb.opencga.catalog.db.AbstractDBAdaptor;

import static org.opencb.opencga.catalog.db.AbstractDBAdaptor.FilterOption.Type.TEXT;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public enum AuditFilterOption implements AbstractDBAdaptor.FilterOption {
    id("id", "", TEXT),
    timeStamp("timeStamp", "", TEXT),
    resource("resource", "", TEXT),
    action("action", "", TEXT),
    userId("userId", "", TEXT);

    private final String _key;
    private final String _description;
    private final Type _type;

    AuditFilterOption(String key, String description, Type type) {
        _key = key;
        _description = description;
        _type = type;
    }

    @Override
    public String getKey() {
        return _key;
    }

    @Override
    public String getDescription() {
        return _description;
    }

    @Override
    public Type getType() {
        return _type;
    }

}
