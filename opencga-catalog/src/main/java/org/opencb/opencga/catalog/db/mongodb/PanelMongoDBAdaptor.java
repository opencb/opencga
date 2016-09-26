package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.DuplicateKeyException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.PanelConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.DiseasePanel;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.acls.permissions.DiseasePanelAclEntry;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.filterOptions;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.filterStringParams;

/**
 * Created by pfurio on 01/06/16.
 */
public class PanelMongoDBAdaptor extends MongoDBAdaptor implements PanelDBAdaptor {

    private final MongoDBCollection panelCollection;
    private PanelConverter panelConverter;
    private AclMongoDBAdaptor<DiseasePanelAclEntry> aclDBAdaptor;

    public PanelMongoDBAdaptor(MongoDBCollection panelCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(JobMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.panelCollection = panelCollection;
        this.panelConverter = new PanelConverter();
        this.aclDBAdaptor = new AclMongoDBAdaptor<>(panelCollection, panelConverter, logger);
    }

    @Override
    public QueryResult<DiseasePanel> get(long diseasePanelId, QueryOptions options) throws CatalogDBException {
        checkId(diseasePanelId);
        Query query = new Query(QueryParams.ID.key(), diseasePanelId);
        return get(query, options);
    }

    @Override
    public long getStudyId(long panelId) throws CatalogDBException {
        checkId(panelId);
        QueryResult queryResult = nativeGet(new Query(QueryParams.ID.key(), panelId),
                new QueryOptions(QueryOptions.INCLUDE, PRIVATE_STUDY_ID));
        if (queryResult.getResult().isEmpty()) {
            throw CatalogDBException.idNotFound("Panel", panelId);
        } else {
            return ((Document) queryResult.first()).getLong(PRIVATE_STUDY_ID);
        }
    }

    @Override
    public QueryResult<DiseasePanel> insert(DiseasePanel diseasePanel, long studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);

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

        return endQuery("Create panel", startTime, get(newPanelId, options));
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
        return endQuery("Update panel", startTime, get(id, null));
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
    public DBIterator<DiseasePanel> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = panelCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator, panelConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = panelCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator);
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
        DBIterator<DiseasePanel> catalogDBIterator = iterator(query, options);
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
//        CatalogMongoDBUtils.createAcl(id, acl, panelCollection, "DiseasePanelAcl");
        return endQuery("create panel Acl", startTime, Arrays.asList(aclDBAdaptor.createAcl(id, acl)));
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> getAcl(long id, List<String> members) throws CatalogDBException {
        long startTime = startQuery();
//
//        List<DiseasePanelAclEntry> acl = null;
//        QueryResult<Document> aggregate = CatalogMongoDBUtils.getAcl(id, members, panelCollection, logger);
//        DiseasePanel panel = panelConverter.convertToDataModelType(aggregate.first());
//
//        if (panel != null) {
//            acl = panel.getAcl();
//        }

        return endQuery("get panel Acl", startTime, aclDBAdaptor.getAcl(id, members));
    }

    @Override
    public void removeAcl(long id, String member) throws CatalogDBException {
//        CatalogMongoDBUtils.removeAcl(id, member, panelCollection);
        aclDBAdaptor.removeAcl(id, member);
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> setAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.setAclsToMember(id, member, permissions, panelCollection);
        return endQuery("Set Acls to member", startTime, Arrays.asList(aclDBAdaptor.setAclsToMember(id, member, permissions)));
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> addAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.addAclsToMember(id, member, permissions, panelCollection);
        return endQuery("Add Acls to member", startTime, Arrays.asList(aclDBAdaptor.addAclsToMember(id, member, permissions)));
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> removeAclsFromMember(long id, String member, List<String> permissions)
            throws CatalogDBException {
//        CatalogMongoDBUtils.removeAclsFromMember(id, member, permissions, panelCollection);
        long startTime = startQuery();
        return endQuery("Remove Acls from member", startTime, Arrays.asList(aclDBAdaptor.removeAclsFromMember(id, member, permissions)));
    }

    public void removeAclsFromStudy(long studyId, String member) throws CatalogDBException {
        aclDBAdaptor.removeAclsFromStudy(studyId, member);
    }
}
