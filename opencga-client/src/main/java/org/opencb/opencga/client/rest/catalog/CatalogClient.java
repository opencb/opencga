package org.opencb.opencga.client.rest.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.AbstractParentClient;

import java.io.IOException;

/**
 * Created by pfurio on 11/11/16.
 */
public abstract class CatalogClient<T, A> extends AbstractParentClient {

    protected String category;

    protected Class<T> clazz;
    protected Class<A> aclClass;

    protected CatalogClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
    }

    public enum AclParams {
        ADD_PERMISSIONS("addPermissions"),
        REMOVE_PERMISSIONS("removePermissions"),
        SET_PERMISSIONS("setPermissions");

        private String key;

        AclParams(String value) {
            this.key = value;
        }

        public String key() {
            return this.key;
        }
    }

    public QueryResponse<Long> count(Query query) throws IOException {
        return execute(category, "count", query, GET, Long.class);
    }

    public QueryResponse<T> get(String id, QueryOptions options) throws IOException {
        return execute(category, id, "info", options, GET, clazz);
    }

    public QueryResponse<T> search(Query query, QueryOptions options) throws IOException {
        ObjectMap myQuery = new ObjectMap(query);
        myQuery.putAll(options);
        return execute(category, "search", myQuery, GET, clazz);
    }

    public QueryResponse<T> update(String id, ObjectMap params) throws CatalogException, IOException {
        //TODO Check that everything is correct
        if (params.containsKey("method") && params.get("method").equals("GET")) {
            return execute(category, id, "update", params, GET, clazz);
        }
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(params);
        ObjectMap p = new ObjectMap("body", json);
        logger.debug("Json in update client: " + json);
        return execute(category, id, "update", p, POST, clazz);
    }

    public QueryResponse<T> delete(String id, ObjectMap params) throws CatalogException, IOException {
        return execute(category, id, "delete", params, GET, clazz);
    }

    // Acl methods

    public QueryResponse<A> getAcls(String id) throws IOException {
        return execute(category, id, "acl", new ObjectMap(), GET, aclClass);
    }

    public QueryResponse<A> getAcl(String id, String memberId) throws CatalogException, IOException {
        return execute(category, id, "acl", memberId, "info", new ObjectMap(), GET, aclClass);
    }

    public QueryResponse<A> createAcl(String id, String members, ObjectMap params) throws CatalogException,
            IOException {
        params = addParamsToObjectMap(params, "members", members);
        return execute(category, id, "acl", null, "create", params, GET, aclClass);
    }

    public QueryResponse<A> deleteAcl(String id, String memberId) throws CatalogException, IOException {
        return execute(category, id, "acl", memberId, "delete", new ObjectMap(), GET, aclClass);
    }

    public QueryResponse<A> updateAcl(String id, String memberId, ObjectMap params) throws CatalogException, IOException {
        return execute(category, id, "acl", memberId, "update", params, GET, aclClass);
    }

}
