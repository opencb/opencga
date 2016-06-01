package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.DuplicateKeyException;
import com.mongodb.client.model.Filters;
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
import org.opencb.opencga.catalog.models.Status;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.filterOptions;

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
        return null;
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
        return null;
    }

    @Override
    public QueryResult<DiseasePanel> update(long id, ObjectMap parameters) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<DiseasePanel> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<DiseasePanel> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> restore(Query query) throws CatalogDBException {
        return null;
    }

    @Override
    public CatalogDBIterator<DiseasePanel> iterator(Query query, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {

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
