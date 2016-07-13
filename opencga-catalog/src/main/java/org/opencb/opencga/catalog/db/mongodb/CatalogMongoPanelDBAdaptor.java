package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.DuplicateKeyException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.CatalogDBIterator;
import org.opencb.opencga.catalog.db.api.CatalogPanelDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.PanelConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.DiseasePanel;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.acls.DiseasePanelAclEntry;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;
import static org.opencb.opencga.catalog.utils.CatalogMemberValidator.checkMembers;

/**
 * Created by pfurio on 01/06/16.
 */
public class CatalogMongoPanelDBAdaptor extends CatalogMongoDBAdaptor implements CatalogPanelDBAdaptor {

    private final MongoDBCollection panelCollection;
    private PanelConverter panelConverter;

    public CatalogMongoPanelDBAdaptor(MongoDBCollection panelCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoJobDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.panelCollection = panelCollection;
        this.panelConverter = new PanelConverter();
    }

    @Override
    public QueryResult<DiseasePanel> getPanel(long diseasePanelId, QueryOptions options) throws CatalogDBException {
        checkPanelId(diseasePanelId);
        Query query = new Query(QueryParams.ID.key(), diseasePanelId);
        return get(query, options);
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> getPanelAcl(long panelId, List<String> members) throws CatalogDBException {
        long startTime = startQuery();

        checkPanelId(panelId);

        Bson match = Aggregates.match(Filters.eq(PRIVATE_ID, panelId));
        Bson unwind = Aggregates.unwind("$" + QueryParams.ACL.key());
        Bson match2 = Aggregates.match(Filters.in(QueryParams.ACL_MEMBER.key(), members));
        Bson project = Aggregates.project(Projections.include(QueryParams.ID.key(), QueryParams.ACL.key()));

        List<DiseasePanelAclEntry> panelAcl = null;
        QueryResult<Document> aggregate = panelCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);
        DiseasePanel panel = panelConverter.convertToDataModelType(aggregate.first());

        if (panel != null) {
            panelAcl = panel.getAcl();
        }

        return endQuery("Get panel Acl", startTime, panelAcl);
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> setPanelAcl(long panelId, DiseasePanelAclEntry acl, boolean override)
            throws CatalogDBException {
        long startTime = startQuery();
        long studyId = getStudyIdByPanelId(panelId);

        String member = acl.getMember();

        // If there is a group in acl.getMember(), we will obtain all the users belonging to the groups and will check if any of them
        // already have permissions on its own.
        if (member.startsWith("@")) {
            Group group = dbAdaptorFactory.getCatalogStudyDBAdaptor().getGroup(studyId, member, Collections.emptyList()).first();

            // Check if any user already have permissions set on their own.
            QueryResult<DiseasePanelAclEntry> fileAcl = getPanelAcl(panelId, group.getUserIds());
            if (fileAcl.getNumResults() > 0) {
                throw new CatalogDBException("Error when adding permissions in panel. At least one user in " + group.getName()
                        + " has already defined permissions for panel " + panelId);
            }
        } else {
            // Check if the members of the new acl already have some permissions set
            QueryResult<DiseasePanelAclEntry> panelAcls = getPanelAcl(panelId, acl.getMember());

            if (panelAcls.getNumResults() > 0 && override) {
                unsetPanelAcl(panelId, Arrays.asList(member), Collections.emptyList());
            } else if (panelAcls.getNumResults() > 0 && !override) {
                throw new CatalogDBException("setDiseasePanelAcl: " + member + " already had an Acl set. If you "
                        + "still want to set a new Acl and remove the old one, please use the override parameter.");
            }
        }

        // Push the new acl to the list of acls.
        Document queryDocument = new Document(PRIVATE_ID, panelId);
        Document update = new Document("$push", new Document(QueryParams.ACL.key(), getMongoDBDocument(acl, "DiseasePanelAcl")));
        QueryResult<UpdateResult> updateResult = panelCollection.update(queryDocument, update, null);

        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("setDiseasePanelAcl: An error occurred when trying to share file " + panelId + " with " + member);
        }

        return endQuery("setDiseasePanelAcl", startTime, Arrays.asList(acl));
    }

    @Override
    public void unsetPanelAcl(long panelId, List<String> members, List<String> permissions) throws CatalogDBException {
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, getStudyIdByPanelId(panelId), members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_ID, panelId).append(QueryParams.ACL_MEMBER.key(), member);
            Bson update;
            if (permissions.size() == 0) {
                update = new Document("$pull", new Document("acl", new Document("member", member)));
            } else {
                update = new Document("$pull", new Document("acl.$.permissions", new Document("$in", permissions)));
            }
            QueryResult<UpdateResult> updateResult = panelCollection.update(query, update, null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("unsetPanelAcl: An error occurred when trying to stop sharing panel " + panelId
                        + " with other " + member + ".");
            }
        }

        // Remove possible panelAcls that might have permissions defined but no users
//        Bson queryBson = new Document(QueryParams.ID.key(), panelId)
//                .append(QueryParams.ACL_MEMBER.key(),
//                        new Document("$exists", true).append("$eq", Collections.emptyList()));
//        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
//        panelCollection.update(queryBson, update, null);
    }

    @Override
    public void unsetPanelAclsInStudy(long studyId, List<String> members) throws CatalogDBException {
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, studyId, members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_STUDY_ID, studyId).append(QueryParams.ACL_MEMBER.key(), member);
            Bson update = new Document("$pull", new Document("acl", new Document("member", member)));
            panelCollection.update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));
        }
//
//        // Remove possible CohortAcls that might have permissions defined but no users
//        Bson queryBson = new Document(PRIVATE_STUDY_ID, studyId)
//                .append(CatalogSampleDBAdaptor.QueryParams.ACL_MEMBER.key(),
//                        new Document("$exists", true).append("$eq", Collections.emptyList()));
//        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
//        panelCollection.update(queryBson, update, new QueryOptions(MongoDBCollection.MULTI, true));
    }

    @Override
    public long getStudyIdByPanelId(long panelId) throws CatalogDBException {
        checkPanelId(panelId);
        QueryResult queryResult = nativeGet(new Query(QueryParams.ID.key(), panelId),
                new QueryOptions(QueryOptions.INCLUDE, PRIVATE_STUDY_ID));
        if (queryResult.getResult().isEmpty()) {
            throw CatalogDBException.idNotFound("Panel", panelId);
        } else {
            return ((Document) queryResult.first()).getLong(PRIVATE_STUDY_ID);
        }
    }

    @Override
    public QueryResult<DiseasePanel> createPanel(long studyId, DiseasePanel diseasePanel, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);

        //new Panel Id
        long newPanelId = getNewId();
        diseasePanel.setId(newPanelId);

        Document panelDocument = panelConverter.convertToStorageType(diseasePanel);
        panelDocument.append(PRIVATE_STUDY_ID, studyId);
        panelDocument.append(PRIVATE_ID, newPanelId);

        try {
            panelCollection.insert(panelDocument, null);
        } catch (DuplicateKeyException e) {
            throw CatalogDBException.alreadyExists("Panel", studyId, "name", diseasePanel.getName());
        }

        return endQuery("Create panel", startTime, getPanel(newPanelId, options));
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        return panelCollection.count(parseQuery(query));
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        return panelCollection.distinct(field, parseQuery(query));
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<DiseasePanel> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson;
        try {
            bson = parseQuery(query);
        } catch (NumberFormatException e) {
            throw new CatalogDBException("Get panel: Could not parse all the arguments from query - " + e.getMessage(), e.getCause());
        }
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_PANELS);

        QueryResult<DiseasePanel> panelQueryResult = panelCollection.find(bson, panelConverter, qOptions);
        logger.debug("Panel get: query : {}, project: {}, dbTime: {}", bson, qOptions == null ? "" : qOptions.toJson(),
                panelQueryResult.getDbTime());
        return endQuery("get Panel", startTime, panelQueryResult);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson;
        try {
            bson = parseQuery(query);
        } catch (NumberFormatException e) {
            throw new CatalogDBException("Get panel: Could not parse all the arguments from query - " + e.getMessage(), e.getCause());
        }
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_PANELS);

        return panelCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<DiseasePanel> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.ID.key(), id), parameters);
        if (update.getNumTotalResults() != 1) {
            throw new CatalogDBException("Could not update panel with id " + id);
        }
        return endQuery("Update panel", startTime, getPanel(id, null));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Document updateOperations = new Document();
        Map<String, Object> panelSetParameters = new HashMap<>();

        // We read the string parameters
//        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.DISEASE.key(), QueryParams.DESCRIPTION.key()};
        final String[] acceptedParams = {QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, panelSetParameters, acceptedParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            panelSetParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            panelSetParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTimeMillis());
        }
        // Create the update with set
        if (!panelSetParameters.isEmpty()) {
            updateOperations.put("$set", panelSetParameters);
        }

        // We read the list parameters (variants, genes & regions)
        Document panelAddToSetParameters = new Document();
        if (parameters.containsKey(QueryParams.GENES.key())) {
            List<String> genes = parameters.getAsStringList(QueryParams.GENES.key());
            if (genes.size() > 0) {
                panelAddToSetParameters.put(QueryParams.GENES.key(), new Document("$each", genes));
            }
        }
        if (parameters.containsKey(QueryParams.REGIONS.key())) {
            List<String> regions = parameters.getAsStringList(QueryParams.REGIONS.key());
            if (regions.size() > 0) {
                panelAddToSetParameters.put(QueryParams.REGIONS.key(), new Document("$each", regions));
            }
        }
        if (parameters.containsKey(QueryParams.VARIANTS.key())) {
            List<String> variants = parameters.getAsStringList(QueryParams.VARIANTS.key());
            if (variants.size() > 0) {
                panelAddToSetParameters.put(QueryParams.VARIANTS.key(), new Document("$each", variants));
            }
        }
        // Create the update with addToSet
        if (panelAddToSetParameters.size() > 0) {
            updateOperations.put("$addToSet", panelAddToSetParameters);
        }

        if (updateOperations.size() > 0) {
            QueryResult<UpdateResult> update = panelCollection.update(parseQuery(query), updateOperations, null);
            return endQuery("Update panel", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        return endQuery("Update sample", startTime, new QueryResult<>());
    }

    @Override
    public QueryResult<DiseasePanel> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Delete not yet implemented.");
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Delete not yet implemented.");
    }

    @Override
    public QueryResult<DiseasePanel> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<DiseasePanel> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Restore not yet implemented.");
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Restore not yet implemented.");
    }

    @Override
    public CatalogDBIterator<DiseasePanel> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = panelCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, panelConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = panelCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(panelCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(panelCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(panelCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        CatalogDBIterator<DiseasePanel> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_ID:
                        addOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                }
            } catch (Exception e) {
                logger.error("Error with " + entry.getKey() + " " + entry.getValue());
                throw new CatalogDBException(e);
            }
        }

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> createAcl(long id, DiseasePanelAclEntry acl) throws CatalogDBException {
        long startTime = startQuery();
        CatalogMongoDBUtils.createAcl(id, acl, panelCollection, "DiseasePanelAcl");
        return endQuery("create panel Acl", startTime, Arrays.asList(acl));
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> getAcl(long id, List<String> members) throws CatalogDBException {
        long startTime = startQuery();

        List<DiseasePanelAclEntry> acl = null;
        QueryResult<Document> aggregate = CatalogMongoDBUtils.getAcl(id, members, panelCollection, logger);
        DiseasePanel panel = panelConverter.convertToDataModelType(aggregate.first());

        if (panel != null) {
            acl = panel.getAcl();
        }

        return endQuery("get panel Acl", startTime, acl);
    }

    @Override
    public void removeAcl(long id, String member) throws CatalogDBException {
        CatalogMongoDBUtils.removeAcl(id, member, panelCollection);
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> setAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException {
        long startTime = startQuery();
        CatalogMongoDBUtils.setAclsToMember(id, member, permissions, panelCollection);
        return endQuery("Set Acls to member", startTime, getAcl(id, Arrays.asList(member)));
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> addAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException {
        long startTime = startQuery();
        CatalogMongoDBUtils.addAclsToMember(id, member, permissions, panelCollection);
        return endQuery("Add Acls to member", startTime, getAcl(id, Arrays.asList(member)));
    }

    @Override
    public void removeAclsFromMember(long id, String member, List<String> permissions) throws CatalogDBException {
        CatalogMongoDBUtils.removeAclsFromMember(id, member, permissions, panelCollection);
    }
}
