package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Dataset;
import org.opencb.opencga.catalog.models.acls.permissions.DatasetAclEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by pfurio on 04/05/16.
 */
public interface DatasetDBAdaptor extends AclDBAdaptor<Dataset, DatasetAclEntry> {

    enum QueryParams implements QueryParam {

        ID("id", DOUBLE, ""),
        NAME("name", TEXT, ""),
        CREATION_DATE("creationDate", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        FILES("files", INTEGER_ARRAY, ""),
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        ACL("acl", TEXT_ARRAY, ""),
        ACL_MEMBER("acl.member", TEXT_ARRAY, ""),
        ACL_PERMISSIONS("acl.permissions", TEXT_ARRAY, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"

        STUDY_ID("studyId", INTEGER_ARRAY, "");

        // Fixme: Index attributes
        private static Map<String, QueryParams> map = new HashMap<>();
        static {
            for (QueryParams param : QueryParams.values()) {
                map.put(param.key(), param);
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


    default boolean exists(long datasetId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), datasetId)).first() > 0;
    }

    default void checkId(long datasetId) throws CatalogDBException {
        if (datasetId < 0) {
            throw CatalogDBException.newInstance("Dataset id '{}' is not valid: ", datasetId);
        }

        long count = count(new Query(QueryParams.ID.key(), datasetId)).first();
        if (count <= 0) {
            throw CatalogDBException.newInstance("Dataset id '{}' does not exist", datasetId);
        } else if (count > 1) {
            throw CatalogDBException.newInstance("'{}' documents found with the Dataset id '{}'", count, datasetId);
        }
    }

    long getStudyIdByDatasetId(long datasetId) throws CatalogDBException;

    QueryResult<Dataset> insert(Dataset dataset, long studyId, QueryOptions options) throws CatalogDBException;

    QueryResult<Dataset> get(long datasetId, QueryOptions options) throws CatalogDBException;

    MongoDBCollection getDatasetCollection();

    /**
     * Adds the fileIds to the datasets that matches the query.
     *
     * @param query query.
     * @param fileIds file ids.
     * @return A queryResult object containing the number of datasets matching the query.
     * @throws CatalogDBException CatalogDBException.
     */
    QueryResult<Long> insertFilesIntoDatasets(Query query, List<Long> fileIds) throws CatalogDBException;

    /**
     * Extract the fileIds given from the datasets that matching the query.
     *
     * @param query query.
     * @param fileIds file ids.
     * @return A queryResult object containing the number of datasets matching the query.
     * @throws CatalogDBException CatalogDBException.
     */
    QueryResult<Long> extractFilesFromDatasets(Query query, List<Long> fileIds) throws CatalogDBException;

    /**
     * Remove all the Acls defined for the member in the resource.
     *
     * @param studyId study id where the Acls will be removed from.
     * @param member member from whom the Acls will be removed.
     * @throws CatalogDBException if any problem occurs during the removal.
     */
    void removeAclsFromStudy(long studyId, String member) throws CatalogDBException;

}
