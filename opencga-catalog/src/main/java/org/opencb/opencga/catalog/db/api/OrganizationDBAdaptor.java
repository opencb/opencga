package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface OrganizationDBAdaptor extends Iterable<Organization> {

    String IS_ORGANIZATION_ADMIN_OPTION = "isOrgAdmin";
    String AUTH_ORIGINS_FIELD = "authenticationOrigins";

    enum QueryParams implements QueryParam {
        UID("uid", LONG, ""),
        ID("id", STRING, ""),
        UUID("uuid", STRING, ""),
        NAME("name", STRING, ""),
        OWNER("owner", STRING, ""),
        ADMINS("admins", TEXT_ARRAY, ""),
        INTERNAL("internal", OBJECT, ""),
        INTERNAL_MIGRATION_EXECUTIONS("internal.migrationExecutions", OBJECT, ""),
        FEDERATION("federation", OBJECT, ""),
        FEDERATION_CLIENTS("federation.clients", OBJECT, ""),
        FEDERATION_SERVERS("federation.servers", OBJECT, ""),
        CONFIGURATION("configuration", OBJECT, ""),
        CONFIGURATION_OPTIMIZATIONS("configuration.optimizations", OBJECT, ""),
        CONFIGURATION_AUTHENTICATION_ORIGINS("configuration." + AUTH_ORIGINS_FIELD, OBJECT, ""),
        CONFIGURATION_AUTHENTICATION_ORIGINS_ID("configuration." + AUTH_ORIGINS_FIELD + ".id", STRING, ""),
        CONFIGURATION_AUTHENTICATION_ORIGINS_OPTIONS("configuration." + AUTH_ORIGINS_FIELD + ".options", OBJECT, ""),
        CONFIGURATION_TOKEN("configuration.token", OBJECT, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        PROJECTS("projects", OBJECT, ""),
        NOTES("notes", OBJECT, ""),
        ATTRIBUTES("attributes", OBJECT, "");

        private static Map<String, QueryParams> map = new HashMap<>();

        static {
            for (QueryParams params : QueryParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        QueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

//    default boolean exists(long projectId) throws CatalogDBException {
//        return count(new Query(QueryParams.UID.key(), projectId)).getNumMatches() > 0;
//    }
//
//    default void checkId(long projectId) throws CatalogDBException {
//        if (projectId < 0) {
//            throw CatalogDBException.newInstance("Project id '{}' is not valid: ", projectId);
//        }
//
//        if (!exists(projectId)) {
//            throw CatalogDBException.newInstance("Project id '{}' does not exist", projectId);
//        }
//    }
//
//    OpenCGAResult nativeInsert(Map<String, Object> project, String userId) throws CatalogDBException;
//
    OpenCGAResult<Organization> insert(Organization organization, QueryOptions options) throws CatalogException;

    OpenCGAResult<Organization> get(String userId, QueryOptions options) throws CatalogDBException;

    OpenCGAResult<Organization> get(QueryOptions options) throws CatalogDBException;
//
//    OpenCGAResult incrementCurrentRelease(long projectId) throws CatalogDBException;
//
//    long getId(String userId, String projectAlias) throws CatalogDBException;
//
//    String getOwnerId(long projectId) throws CatalogDBException;
//
//
//    default OpenCGAResult<Long> count() throws CatalogDBException {
//        return count(new Query());
//    }
//
//    OpenCGAResult<Long> count(Query query) throws CatalogDBException;
//
//    OpenCGAResult<Long> count(Query query, String user, StudyPermissions.Permissions studyPermission)
//            throws CatalogDBException, CatalogAuthorizationException;
//
//    default OpenCGAResult distinct(String field) throws CatalogDBException {
//        return distinct(new Query(), field);
//    }
//
//    OpenCGAResult distinct(Query query, String field) throws CatalogDBException;
//
//
//    default OpenCGAResult stats() {
//        return stats(new Query());
//    }
//
//    OpenCGAResult stats(Query query);
//
//
//    OpenCGAResult<Project> get(Query query, QueryOptions options) throws CatalogDBException;
//
//    OpenCGAResult<Project> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogParameterException;
//
//    default List<OpenCGAResult<Project>> get(List<Query> queries, QueryOptions options) throws CatalogDBException {
//        Objects.requireNonNull(queries);
//        List<OpenCGAResult<Project>> queryResults = new ArrayList<>(queries.size());
//        for (Query query : queries) {
//            queryResults.add(get(query, options));
//        }
//        return queryResults;
//    }
//
//    OpenCGAResult nativeGet(Query query, QueryOptions options) throws CatalogDBException;
//
//    OpenCGAResult nativeGet(Query query, QueryOptions options, String user)
//            throws CatalogDBException, CatalogAuthorizationException;
//
//    default List<OpenCGAResult> nativeGet(List<Query> queries, QueryOptions options) throws CatalogDBException {
//        Objects.requireNonNull(queries);
//        List<OpenCGAResult> queryResults = new ArrayList<>(queries.size());
//        for (Query query : queries) {
//            queryResults.add(nativeGet(query, options));
//        }
//        return queryResults;
//    }
//
    OpenCGAResult<Organization> update(String organizationId, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Organization> updateFederationServerParams(String federationId, ObjectMap params) throws CatalogDBException;

    OpenCGAResult<Organization> updateFederationClientParams(String federationId, ObjectMap params) throws CatalogDBException;

//
//    OpenCGAResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException;
//
    OpenCGAResult<Organization> delete(Organization organization) throws CatalogDBException;
//
//    OpenCGAResult delete(Query query) throws CatalogDBException;
//
//    default OpenCGAResult<Project> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
//        throw new NotImplementedException("");
//    }
//
//    @Deprecated
//    default OpenCGAResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
//        throw new NotImplementedException("");
//    }
//
//    @Deprecated
//    default OpenCGAResult<Project> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
//        throw new NotImplementedException("");
//    }
//
//    @Deprecated
//    default OpenCGAResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
//        throw new NotImplementedException("");
//    }
//
//    OpenCGAResult<Project> restore(long id, QueryOptions queryOptions)
//            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;
//
//    OpenCGAResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException;
//
//
////    OpenCGAResult<Long> updateStatus(Query query, Status status) throws CatalogDBException;
//
//
    @Override
    default DBIterator<Organization> iterator() {
        try {
            return iterator(new QueryOptions());
        } catch (CatalogDBException e) {
            throw new RuntimeException(e);
        }
    }

    DBIterator<Organization> iterator(QueryOptions options) throws CatalogDBException;

//    default DBIterator nativeIterator() throws CatalogDBException {
//        return nativeIterator(new Query(), new QueryOptions());
//    }
//
//    DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException;
//
//    DBIterator<Project> iterator(Query query, QueryOptions options, String user)
//            throws CatalogDBException, CatalogAuthorizationException;
//
//    DBIterator nativeIterator(Query query, QueryOptions options, String user)
//            throws CatalogDBException, CatalogAuthorizationException;
//
//    OpenCGAResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException;
//
//    OpenCGAResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException;
//
//    OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException;
//
//    OpenCGAResult groupBy(Query query, String field, QueryOptions options, String user)
//            throws CatalogDBException, CatalogAuthorizationException;
//
//    OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
//            throws CatalogDBException, CatalogAuthorizationException;
//
//    @Override
//    default void forEach(Consumer action) {
//        try {
//            forEach(new Query(), action, new QueryOptions());
//        } catch (CatalogDBException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException;

}
