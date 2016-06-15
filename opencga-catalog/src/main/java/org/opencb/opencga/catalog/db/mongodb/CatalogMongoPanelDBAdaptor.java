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
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.PanelConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.DiseasePanel;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.acls.DiseasePanelAcl;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

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
    public QueryResult<DiseasePanelAcl> getPanelAcl(long panelId, List<String> members) throws CatalogDBException {
        long startTime = startQuery();

        checkPanelId(panelId);

        Bson match = Aggregates.match(Filters.eq(PRIVATE_ID, panelId));
        Bson unwind = Aggregates.unwind("$" + QueryParams.ACLS.key());
        Bson match2 = Aggregates.match(Filters.in(QueryParams.ACLS_USERS.key(), members));
        Bson project = Aggregates.project(Projections.include(QueryParams.ID.key(), QueryParams.ACLS.key()));

        List<DiseasePanelAcl> panelAcl = null;
        QueryResult<Document> aggregate = panelCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);
        DiseasePanel panel = panelConverter.convertToDataModelType(aggregate.first());

        if (panel != null) {
            panelAcl = panel.getAcls();
        }

        return endQuery("Get panel Acl", startTime, panelAcl);
    }

    @Override
    public QueryResult<DiseasePanelAcl> setPanelAcl(long panelId, DiseasePanelAcl acl, boolean override) throws CatalogDBException {
        long startTime = startQuery();

        checkPanelId(panelId);
        long studyId = getStudyIdByPanelId(panelId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, studyId, acl.getUsers());

        // If there are groups in acl.getUsers(), we will obtain all the users belonging to the groups and will check if any of them
        // already have permissions on its own.
        Map<String, List<String>> groups = new HashMap<>();
        Set<String> users = new HashSet<>();

        for (String member : acl.getUsers()) {
            if (member.startsWith("@")) {
                Group group = dbAdaptorFactory.getCatalogStudyDBAdaptor().getGroup(studyId, member, Collections.emptyList()).first();
                groups.put(group.getId(), group.getUserIds());
            } else {
                users.add(member);
            }
        }
        if (groups.size() > 0) {
            // Check if any user already have permissions set on their own.
            for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
                QueryResult<DiseasePanelAcl> panelAcl = getPanelAcl(panelId, entry.getValue());
                if (panelAcl.getNumResults() > 0) {
                    throw new CatalogDBException("Error when adding permissions in panel. At least one user in " + entry.getKey()
                            + " has already defined permissions for panel " + panelId);
                }
            }
        }

        // Check if any of the users in the set of users also belongs to any introduced group. In that case, we will remove the user
        // because the group will be given the permission.
        for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
            for (String userId : entry.getValue()) {
                if (users.contains(userId)) {
                    users.remove(userId);
                }
            }
        }

        // Create the definitive list of members that will be added in the acl
        List<String> members = new ArrayList<>(users.size() + groups.size());
        members.addAll(users.stream().collect(Collectors.toList()));
        members.addAll(groups.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList()));
        acl.setUsers(members);

        // Check if the members of the new acl already have some permissions set
        QueryResult<DiseasePanelAcl> panelAcls = getPanelAcl(panelId, acl.getUsers());
        if (panelAcls.getNumResults() > 0 && override) {
            Set<String> usersSet = new HashSet<>(acl.getUsers().size());
            usersSet.addAll(acl.getUsers().stream().collect(Collectors.toList()));

            List<String> usersToOverride = new ArrayList<>();
            for (DiseasePanelAcl panelAcl : panelAcls.getResult()) {
                for (String member : panelAcl.getUsers()) {
                    if (usersSet.contains(member)) {
                        // Add the user to the list of users that will be taken out from the Acls.
                        usersToOverride.add(member);
                    }
                }
            }

            // Now we remove the old permissions set for the users that already existed so the permissions are overriden by the new ones.
            unsetPanelAcl(panelId, usersToOverride, Collections.emptyList());
        } else if (panelAcls.getNumResults() > 0 && !override) {
            throw new CatalogDBException("setPanelAcl: " + panelAcls.getNumResults() + " of the members already had an Acl set. If you "
                    + "still want to set the Acls for them and remove the old one, please use the override parameter.");
        }

        // Append the users to the existing acl.
        List<String> permissions = acl.getPermissions().stream().map(DiseasePanelAcl.DiseasePanelPermissions::name)
                .collect(Collectors.toList());

        // Check if the permissions found on acl already exist on cohort id
        Document queryDocument = new Document(PRIVATE_ID, panelId);
        if (permissions.size() > 0) {
            queryDocument.append(QueryParams.ACLS_PERMISSIONS.key(), new Document("$size", permissions.size()).append("$all", permissions));
        } else {
            queryDocument.append(QueryParams.ACLS_PERMISSIONS.key(), new Document("$size", 0));
        }

        Bson update;
        if (panelCollection.count(queryDocument).first() > 0) {
            // Append the users to the existing acl.
            update = new Document("$addToSet", new Document("acls.$.users", new Document("$each", acl.getUsers())));
        } else {
            queryDocument = new Document(PRIVATE_ID, panelId);
            // Push the new acl to the list of acls.
            update = new Document("$push", new Document(QueryParams.ACLS.key(), getMongoDBDocument(acl, "CohortAcl")));
        }

        QueryResult<UpdateResult> updateResult = panelCollection.update(queryDocument, update, null);
        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("setPanelAcl: An error occurred when trying to share panel " + panelId
                    + " with other members.");
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, QueryParams.ACLS.key());
        DiseasePanel panel = panelConverter.convertToDataModelType(panelCollection.find(queryDocument, queryOptions).first());

        return endQuery("setPanelAcl", startTime, panel.getAcls());
    }

    @Override
    public void unsetPanelAcl(long panelId, List<String> members, List<String> permissions) throws CatalogDBException {
        checkPanelId(panelId);

        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, getStudyIdByPanelId(panelId), members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_ID, panelId)
                    .append("acls", new Document("$elemMatch", new Document("users", member)));
            Bson update;
            if (permissions.size() == 0) {
                update = new Document("$pull", new Document("acls.$.users", member));
            } else {
                update = new Document("$pull", new Document("acls.$.permissions", new Document("$in", permissions)));
            }
            QueryResult<UpdateResult> updateResult = panelCollection.update(query, update, null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("unsetPanelAcl: An error occurred when trying to stop sharing panel " + panelId
                        + " with other " + member + ".");
            }
        }

        // Remove possible panelAcls that might have permissions defined but no users
        Bson queryBson = new Document(QueryParams.ID.key(), panelId)
                .append(QueryParams.ACLS_USERS.key(),
                        new Document("$exists", true).append("$eq", Collections.emptyList()));
        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
        panelCollection.update(queryBson, update, null);
    }

    @Override
    public void unsetPanelAclsInStudy(long studyId, List<String> members) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, studyId, members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_STUDY_ID, studyId)
                    .append("acls", new Document("$elemMatch", new Document("users", member)));
            Bson update = new Document("$pull", new Document("acls.$.users", member));
            panelCollection.update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));
        }

        // Remove possible CohortAcls that might have permissions defined but no users
        Bson queryBson = new Document(PRIVATE_STUDY_ID, studyId)
                .append(CatalogSampleDBAdaptor.QueryParams.ACLS_USERS.key(),
                        new Document("$exists", true).append("$eq", Collections.emptyList()));
        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
        panelCollection.update(queryBson, update, new QueryOptions(MongoDBCollection.MULTI, true));
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
            throw CatalogDBException.alreadyExists("Panel from study { id:" + studyId + "}", "name", diseasePanel.getName());
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
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
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
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
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

        if (parameters.containsKey(QueryParams.STATUS_STATUS.key())) {
            panelSetParameters.put(QueryParams.STATUS_STATUS.key(), parameters.get(QueryParams.STATUS_STATUS.key()));
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

}
