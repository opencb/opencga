/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.PanelConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.MongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.acls.permissions.DiseasePanelAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;


public class PanelMongoDBAdaptor extends MongoDBAdaptor implements PanelDBAdaptor {

    private final MongoDBCollection panelCollection;
    private PanelConverter diseasePanelConverter;

    public PanelMongoDBAdaptor(MongoDBCollection panelCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(JobMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.panelCollection = panelCollection;
        this.diseasePanelConverter = new PanelConverter();
    }

    public MongoDBCollection getCollection() {
        return panelCollection;
    }

    @Override
    public QueryResult<Panel> insert(long studyId, Panel panel, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), panel.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_ID, studyId));
        filterList.add(Filters.eq(QueryParams.STATUS_NAME.key(), Status.READY));

        Bson bson = Filters.and(filterList);
        QueryResult<Long> count = panelCollection.count(bson);

        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Panel { id: '" + panel.getId() + "'} already exists.");
        }

        //new Panel Id
        long newPanelId = getNewId();
        panel.setUid(newPanelId);
        panel.setStudyUid(studyId);

        Document panelDocument = diseasePanelConverter.convertToStorageType(panel);
        // Versioning private parameters
        panelDocument.put(RELEASE_FROM_VERSION, Arrays.asList(panel.getRelease()));
        panelDocument.put(LAST_OF_VERSION, true);
        panelDocument.put(LAST_OF_RELEASE, true);

        if (StringUtils.isNotEmpty(panel.getCreationDate())) {
            panelDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(panel.getCreationDate()));
        } else {
            panelDocument.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        panelDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        try {
            panelCollection.insert(panelDocument, null);
        } catch (DuplicateKeyException e) {
            throw CatalogDBException.alreadyExists("Panel", studyId, "name", panel.getName());
        }

        return endQuery("Create panel", startTime, get(newPanelId, options));    }

    @Override
    public QueryResult<Panel> get(long panelUid, QueryOptions options) throws CatalogDBException {
        checkUid(panelUid);
        Query query = new Query(QueryParams.UID.key(), panelUid).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        return get(query, options);
    }

    @Override
    public QueryResult<Panel> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Panel> documentList = new ArrayList<>();
        QueryResult<Panel> queryResult;
        try (DBIterator<Panel> dbIterator = iterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query, user, StudyAclEntry.StudyPermissions.VIEW_PANELS);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Panel> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Panel> documentList = new ArrayList<>();
        try (DBIterator<Panel> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        QueryResult<Panel> queryResult = endQuery("Get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Document> nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Native get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }


    @Override
    public long getStudyId(long panelUid) throws CatalogDBException {
        Bson query = new Document(PRIVATE_UID, panelUid);
        Bson projection = Projections.include(PRIVATE_STUDY_ID);
        QueryResult<Document> queryResult = panelCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(PRIVATE_STUDY_ID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("Panel", panelUid);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return panelCollection.count(bson);
    }

    @Override
    public QueryResult<Long> count(final Query query, final String user, final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_PANELS : studyPermissions);

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getDiseasePanelPermission().name());
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        logger.debug("Panel count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return panelCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        return panelCollection.distinct(field, parseQuery(query, false));
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Panel> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.UID.key(), id), parameters, queryOptions);
        if (update.getNumTotalResults() != 1) {
            throw new CatalogDBException("Could not update panel");
        }
        Query query = new Query()
                .append(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), "!=EMPTY");
        return endQuery("Update panel", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        Document panelParameters = parseAndValidateUpdateParams(parameters, query);

        if (panelParameters.containsKey(QueryParams.STATUS_NAME.key())) {
            query.put(Constants.ALL_VERSIONS, true);
            QueryResult<UpdateResult> update = panelCollection.update(parseQuery(query, false),
                    new Document("$set", panelParameters), new QueryOptions("multi", true));

            return endQuery("Update panel", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        if (!queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            if (!panelParameters.isEmpty()) {
                QueryResult<UpdateResult> update = panelCollection.update(parseQuery(query, false),
                        new Document("$set", panelParameters), new QueryOptions("multi", true));
                return endQuery("Update panel", startTime, Arrays.asList(update.getNumTotalResults()));
            }
        } else {
            return updateAndCreateNewVersion(query, panelParameters, parameters, queryOptions);
        }

        return endQuery("Update panel", startTime, new QueryResult<>());
    }

    private QueryResult<Long> updateAndCreateNewVersion(Query query, Document panelParameters, ObjectMap parameters,
                                                        QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Document> queryResult = nativeGet(query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));
        int release = queryOptions.getInt(Constants.CURRENT_RELEASE, -1);
        if (release == -1) {
            throw new CatalogDBException("Internal error. Mandatory " + Constants.CURRENT_RELEASE + " parameter not passed to update "
                    + "method");
        }

        for (Document panelDocument : queryResult.getResult()) {
            Document updateOldVersion = new Document();

            List<Integer> supportedReleases = (List<Integer>) panelDocument.get(RELEASE_FROM_VERSION);
            if (supportedReleases.size() > 1) {
                // If it contains several releases, it means this is the first update on the current release, so we just need to take the
                // current release number out
                supportedReleases.remove(supportedReleases.size() - 1);
            } else {
                // If it is 1, it means that the previous version being checked was made on this same release as well, so it won't be the
                // last version of the release
                updateOldVersion.put(LAST_OF_RELEASE, false);
            }
            updateOldVersion.put(RELEASE_FROM_VERSION, supportedReleases);
            updateOldVersion.put(LAST_OF_VERSION, false);

            // Perform the update on the previous version
            Document queryDocument = new Document()
                    .append(PRIVATE_STUDY_ID, panelDocument.getLong(PRIVATE_STUDY_ID))
                    .append(QueryParams.VERSION.key(), panelDocument.getInteger(QueryParams.VERSION.key()))
                    .append(PRIVATE_UID, panelDocument.getLong(PRIVATE_UID));
            QueryResult<UpdateResult> updateResult = panelCollection.update(queryDocument, new Document("$set", updateOldVersion), null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("Internal error: Could not update panel");
            }

            // We update the information for the new version of the document
            panelDocument.put(LAST_OF_RELEASE, true);
            panelDocument.put(LAST_OF_VERSION, true);
            panelDocument.put(RELEASE_FROM_VERSION, Arrays.asList(release));
            panelDocument.put(QueryParams.VERSION.key(), panelDocument.getInteger(QueryParams.VERSION.key()) + 1);

            // We apply the updates the user wanted to apply (if any)
            mergeDocument(panelDocument, panelParameters);

            // Insert the new version document
            panelCollection.insert(panelDocument, QueryOptions.empty());
        }

        return endQuery("Update panel", startTime, Arrays.asList(queryResult.getNumTotalResults()));
    }

    private Document parseAndValidateUpdateParams(ObjectMap parameters, Query query) throws CatalogDBException {
        Document panelParameters = new Document();

        final String[] acceptedParams = {UpdateParams.NAME.key(), UpdateParams.DESCRIPTION.key(), UpdateParams.AUTHOR.key()};
        filterStringParams(parameters, panelParameters, acceptedParams);

        final String[] acceptedMapParams = {UpdateParams.ATTRIBUTES.key()};
        filterMapParams(parameters, panelParameters, acceptedMapParams);

        final String[] acceptedObjectParams = {UpdateParams.PHENOTYPES.key(), UpdateParams.REGIONS.key(), UpdateParams.GENES.key(),
            UpdateParams.SOURCE.key()};
        filterObjectParams(parameters, panelParameters, acceptedObjectParams);

        final String[] acceptedStringListParams = {UpdateParams.VARIANTS.key()};
        filterStringListParams(parameters, panelParameters, acceptedStringListParams);

        if (parameters.containsKey(UpdateParams.ID.key())) {
            // That can only be done to one panel...

            Query tmpQuery = new Query(query);
            // We take out ALL_VERSION from query just in case we get multiple results...
            tmpQuery.remove(Constants.ALL_VERSIONS);

            QueryResult<Panel> panelQueryResult = get(tmpQuery, new QueryOptions());
            if (panelQueryResult.getNumResults() == 0) {
                throw new CatalogDBException("Update panel: No panel found to be updated");
            }
            if (panelQueryResult.getNumResults() > 1) {
                throw new CatalogDBException("Update panel: Cannot update " + UpdateParams.ID.key() + " parameter. More than one panel "
                        + "found to be updated.");
            }

            // Check that the new sample name is still unique
            long studyId = panelQueryResult.first().getStudyUid();

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            QueryResult<Long> count = count(tmpQuery);
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Cannot update the " + QueryParams.ID.key() + ". Panel "
                        + parameters.get(QueryParams.ID.key()) + " already exists.");
            }

            panelParameters.put(UpdateParams.ID.key(), parameters.get(UpdateParams.ID.key()));
        }

        if (parameters.containsKey(UpdateParams.STATUS_NAME.key())) {
            panelParameters.put(UpdateParams.STATUS_NAME.key(), parameters.get(UpdateParams.STATUS_NAME.key()));
            panelParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        return panelParameters;
    }

    @Override
    public void delete(long id) throws CatalogDBException {
        throw new UnsupportedOperationException("Delete not yet implemented.");
    }

    @Override
    public void delete(Query query) throws CatalogDBException {
        throw new UnsupportedOperationException("Delete not yet implemented.");
    }

    @Override
    public QueryResult<Panel> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Delete not yet implemented.");
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Delete not yet implemented.");
    }

    @Override
    public QueryResult<Panel> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Panel> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Restore not yet implemented.");
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Restore not yet implemented.");
    }

    @Override
    public DBIterator<Panel> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new MongoDBIterator<>(mongoCursor, diseasePanelConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new MongoDBIterator(mongoCursor);
    }

    @Override
    public DBIterator<Panel> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        return new MongoDBIterator<>(mongoCursor, diseasePanelConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        return new MongoDBIterator<>(mongoCursor);
    }


    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> documentMongoCursor;
        try {
            documentMongoCursor = getMongoCursor(query, options, null, null);
        } catch (CatalogAuthorizationException e) {
            throw new CatalogDBException(e);
        }
        return documentMongoCursor;
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options, Document studyDocument, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document queryForAuthorisedEntries = null;
        if (studyDocument != null && user != null) {
            // Get the document query needed to check the permissions as well
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_PANELS.name(), DiseasePanelAclEntry.DiseasePanelPermissions.VIEW.name());
        }

        Query finalQuery = new Query(query);
        filterOutDeleted(finalQuery);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        Bson bson = parseQuery(finalQuery, false, queryForAuthorisedEntries);
        logger.debug("Panel query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return panelCollection.nativeQuery().find(bson, qOptions).iterator();
    }

    private Document getStudyDocument(Query query) throws CatalogDBException {
        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }
        return queryResult.first();
    }

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        }
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return rank(panelCollection, bsonQuery, field, QueryParams.ID.key(), numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(panelCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(panelCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user, StudyAclEntry.StudyPermissions.VIEW_PANELS
                .name(), DiseasePanelAclEntry.DiseasePanelPermissions.VIEW.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(panelCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user, StudyAclEntry.StudyPermissions.VIEW_PANELS
                .name(), DiseasePanelAclEntry.DiseasePanelPermissions.VIEW.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(panelCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<Panel> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    private Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        return parseQuery(query, isolated, null);
    }

    private Bson parseQuery(Query query, boolean isolated, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        if (isolated) {
            andBsonList.add(new Document("$isolated", 1));
        }

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), query);

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam =  QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                if (Constants.ALL_VERSIONS.equals(entry.getKey())) {
                    continue;
                }
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addOrQuery(PRIVATE_UID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case SNAPSHOT:
                        addAutoOrQuery(RELEASE_FROM_VERSION, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case GENES:
                        addAutoOrQuery(QueryParams.GENES_ID.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case PHENOTYPES:
                        addAutoOrQuery(QueryParams.PHENOTYPES_ID.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case REGIONS:
                        addAutoOrQuery(QueryParams.REGIONS_LOCATION.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case NAME:
                    case RELEASE:
                    case VERSION:
                    case DESCRIPTION:
                    case AUTHOR:
                    case VARIANTS:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                    case PHENOTYPES_SOURCE:
                    case GENES_ID:
                    case GENES_NAME:
                    case GENES_CONFIDENCE:
                    case REGIONS_LOCATION:
                    case REGIONS_SCORE:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                if (e instanceof CatalogDBException) {
                    throw e;
                } else {
                    throw new CatalogDBException("Error parsing query : " + query.toJson(), e);
                }
            }
        }

        // If the user doesn't look for a concrete version...
        if (!query.getBoolean(Constants.ALL_VERSIONS) && !query.containsKey(QueryParams.VERSION.key())) {
            if (query.containsKey(QueryParams.SNAPSHOT.key())) {
                // If the user looks for anything from some release, we will try to find the latest from the release (snapshot)
                andBsonList.add(Filters.eq(LAST_OF_RELEASE, true));
            } else {
                // Otherwise, we will always look for the latest version
                andBsonList.add(Filters.eq(LAST_OF_VERSION, true));
            }
        }

        if (authorisation != null && authorisation.size() > 0) {
            andBsonList.add(authorisation);
        }
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
