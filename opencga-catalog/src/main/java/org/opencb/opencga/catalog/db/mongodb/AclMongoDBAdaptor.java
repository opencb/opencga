package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.auth.authorization.AclDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.GenericConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.acls.AbstractAcl;
import org.opencb.opencga.catalog.models.acls.permissions.AbstractAclEntry;
import org.slf4j.Logger;

import java.util.*;

import static org.opencb.commons.datastore.core.QueryParam.Type.INTEGER_ARRAY;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.PRIVATE_ID;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.PRIVATE_STUDY_ID;

/**
 * Created by pfurio on 29/07/16.
 */
public class AclMongoDBAdaptor<T extends AbstractAclEntry> implements AclDBAdaptor<T> {

    private MongoDBCollection collection;
    private GenericConverter<? extends AbstractAcl, Document> converter;
    private Logger logger;

    public AclMongoDBAdaptor(MongoDBCollection collection, GenericConverter<? extends AbstractAcl, Document> converter,
                             Logger logger) {
        this.collection = collection;
        this.converter = converter;
        this.logger = logger;
    }

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        ACL("acl", TEXT_ARRAY, ""),
        ACL_MEMBER("acl.member", TEXT_ARRAY, ""),
        ACL_PERMISSIONS("acl.permissions", TEXT_ARRAY, "");

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

    @Override
    public T createAcl(long resourceId, T acl) throws CatalogDBException {
        // Push the new acl to the list of acls.
        Document queryDocument = new Document(PRIVATE_ID, resourceId);
        Document update = new Document("$push", new Document(QueryParams.ACL.key(),
                MongoDBUtils.getMongoDBDocument(acl, "ACL")));
        QueryResult<UpdateResult> updateResult = collection.update(queryDocument, update, null);

        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("create Acl: An error occurred when trying to create acl for " + resourceId + " for "
                    + acl.getMember());
        }

        logger.debug("Create Acl: {}", acl.toString());
        return acl;
    }

    @Override
    public List<T> getAcl(long resourceId, List<String> members) {
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_ID, resourceId)));
        aggregation.add(Aggregates.project(Projections.include(QueryParams.ID.key(),
                QueryParams.ACL.key())));
        aggregation.add(Aggregates.unwind("$" + QueryParams.ACL.key()));

        List<Bson> filters = new ArrayList<>();
        if (members != null && members.size() > 0) {
            filters.add(Filters.in(QueryParams.ACL_MEMBER.key(), members));
        }

        if (filters.size() > 0) {
            Bson filter = filters.size() == 1 ? filters.get(0) : Filters.and(filters);
            aggregation.add(Aggregates.match(filter));
        }

        for (Bson bson : aggregation) {
            logger.debug("Get Acl: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        }

        QueryResult<Document> aggregate = collection.aggregate(aggregation, null);

        AbstractAcl<T> aclResource = converter.convertToDataModelType(aggregate.first());

        if (aclResource != null) {
            return aclResource.getAcl();
        }

        return Collections.emptyList();
    }

    @Override
    public void removeAcl(long resourceId, String member) throws CatalogDBException {
        Document query = new Document()
                .append(PRIVATE_ID, resourceId)
                .append(QueryParams.ACL_MEMBER.key(), member);
        Bson update = new Document().append("$pull", new Document("acl", new Document("member", member)));
        QueryResult<UpdateResult> updateResult = collection.update(query, update, null);
        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("remove ACL: An error occurred when trying to remove the ACL defined for " + member);
        }
        logger.debug("Remove Acl in {} for member {}", resourceId, member);
    }

    @Override
    public void removeAclsFromStudy(long studyId, String member) throws CatalogDBException {
        Document query = new Document()
                .append(PRIVATE_STUDY_ID, studyId)
                .append(QueryParams.ACL_MEMBER.key(), member);
        Bson update = new Document("$pull", new Document("acl", new Document("member", member)));
        QueryResult<UpdateResult> updateResult = collection.update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));
//        if (updateResult.first().getModifiedCount() == 0) {
//            throw new CatalogDBException("remove ACL: An error occurred when trying to remove the ACLs defined for " + member);
//        }
        logger.debug("Remove all the Acls for member {} in study {}", member, studyId);
    }

    @Override
    public T setAclsToMember(long resourceId, String member, List<String> permissions) throws CatalogDBException {
        Document query = new Document()
                .append(PRIVATE_ID, resourceId)
                .append(QueryParams.ACL_MEMBER.key(), member);
        Document update = new Document("$set", new Document("acl.$.permissions", permissions));
        QueryResult<UpdateResult> queryResult = collection.update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Unable to set the new permissions to " + member);
        }

        logger.debug("Set Acl for member {}: {}", member, StringUtils.join(permissions, ","));
        return getAcl(resourceId, Arrays.asList(member)).get(0);
    }

    @Override
    public T addAclsToMember(long resourceId, String member, List<String> permissions) throws CatalogDBException {
        Document query = new Document()
                .append(PRIVATE_ID, resourceId)
                .append(QueryParams.ACL_MEMBER.key(), member);
        Document update = new Document("$addToSet", new Document("acl.$.permissions", new Document("$each", permissions)));
        QueryResult<UpdateResult> queryResult = collection.update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Unable to add new permissions to " + member + ". Maybe the member already had those"
                    + " permissions?");
        }

        logger.debug("Add Acl for member {}: {}", member, StringUtils.join(permissions, ","));
        return getAcl(resourceId, Arrays.asList(member)).get(0);
    }

    @Override
    public T removeAclsFromMember(long resourceId, String member, List<String> permissions) throws CatalogDBException {
        Document query = new Document()
                .append(PRIVATE_ID, resourceId)
                .append(QueryParams.ACL_MEMBER.key(), member);
        Bson pull = Updates.pullAll("acl.$.permissions", permissions);
        QueryResult<UpdateResult> update = collection.update(query, pull, null);
        if (update.first().getModifiedCount() != 1) {
            throw new CatalogDBException("Unable to remove the permissions from " + member + ". Maybe it didn't have those permissions?");
        }

        logger.debug("Remove Acl for member {}: {}", member, StringUtils.join(permissions, ","));
        return getAcl(resourceId, Arrays.asList(member)).get(0);
    }

}
