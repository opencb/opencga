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

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.PanelConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.MongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.acls.permissions.PanelAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;


public class PanelMongoDBAdaptor extends MongoDBAdaptor implements PanelDBAdaptor {

    private final MongoDBCollection panelCollection;
    private PanelConverter panelConverter;

    public PanelMongoDBAdaptor(MongoDBCollection panelCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(JobMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.panelCollection = panelCollection;
        this.panelConverter = new PanelConverter();
    }

    /**
     * @return MongoDB connection to the disease panel collection.
     */
    public MongoDBCollection getPanelCollection() {
        return panelCollection;
    }

    @Override
    public DataResult insert(Panel panel, boolean overwrite) throws CatalogDBException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();

            logger.debug("Starting insert transaction of global panel id '{}'", panel.getId());
            // Check the panel id does not exist
            Query query = new Query()
                    .append(QueryParams.STUDY_UID.key(), -1)
                    .append(QueryParams.ID.key(), panel.getId());

            if (count(clientSession, query).first() > 0) {
                if (overwrite) {
                    // Delete the panel id
                    logger.debug("Global panel '" + panel.getId() + "' already existed. Replacing panel...");

                    Document panelDocument = getPanelDocumentForInsertion(clientSession, panel, -1);
                    panelCollection.update(clientSession, parseQuery(query), new Document("$set", panelDocument), null);
                } else {
                    throw CatalogDBException.alreadyExists("panel", QueryParams.ID.key(), panel.getId());
                }
            } else {
                logger.debug("Inserting new global panel '" + panel.getId() + "'");

                Document panelDocument = getPanelDocumentForInsertion(clientSession, panel, -1);
                panelCollection.insert(clientSession, panelDocument, null);
            }

            logger.info("Global panel '" + panel.getId() + "(" + panel.getUid() + ")' successfully created");
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create global panel {}: {}", panel.getId(), e.getMessage()));
    }

    @Override
    public DataResult insert(long studyUid, Panel panel, QueryOptions options) throws CatalogDBException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting insert transaction of panel id '{}'", panel.getId());

            dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyUid);
            insert(clientSession, studyUid, panel);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create global panel {}: {}", panel.getId(), e.getMessage()));
    }

    void insert(ClientSession clientSession, long studyUid, Panel panel) throws CatalogDBException {
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), panel.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyUid));
        filterList.add(Filters.eq(QueryParams.STATUS_NAME.key(), Status.READY));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = panelCollection.count(bson);

        if (count.first() > 0) {
            throw CatalogDBException.alreadyExists("panel", QueryParams.ID.key(), panel.getId());
        }

        logger.debug("Inserting new panel '" + panel.getId() + "'");

        Document panelDocument = getPanelDocumentForInsertion(clientSession, panel, studyUid);
        panelCollection.insert(clientSession, panelDocument, null);

        logger.info("Panel '" + panel.getId() + "(" + panel.getUid() + ")' successfully created");
    }

    Document getPanelDocumentForInsertion(ClientSession clientSession, Panel panel, long studyUid) {
        //new Panel Id
        long panelUid = getNewUid(clientSession);
        panel.setUid(panelUid);
        panel.setStudyUid(studyUid);
        if (StringUtils.isEmpty(panel.getUuid())) {
            panel.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.PANEL));
        }
        if (StringUtils.isEmpty(panel.getCreationDate())) {
            panel.setCreationDate(TimeUtils.getTime());
        }

        Document panelDocument = panelConverter.convertToStorageType(panel);
        // Versioning private parameters
        panelDocument.put(RELEASE_FROM_VERSION, Arrays.asList(panel.getRelease()));
        panelDocument.put(LAST_OF_VERSION, true);
        panelDocument.put(LAST_OF_RELEASE, true);
        panelDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(panel.getCreationDate()));
        panelDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        return panelDocument;
    }

    @Override
    public DataResult<Panel> get(long panelUid, QueryOptions options) throws CatalogDBException {
        checkUid(panelUid);
        Query query = new Query(QueryParams.UID.key(), panelUid).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED)
                .append(QueryParams.STUDY_UID.key(), getStudyId(panelUid));
        return get(query, options);
    }

    @Override
    public DataResult<Panel> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return get(null, query, options, user);
    }

    DataResult<Panel> get(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Panel> documentList = new ArrayList<>();
        DataResult<Panel> queryResult;
        try (DBIterator<Panel> dbIterator = iterator(clientSession, query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            DataResult<Long> count = count(clientSession, query, user, StudyAclEntry.StudyPermissions.VIEW_PANELS);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DataResult<Panel> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    DataResult<Panel> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Panel> documentList = new ArrayList<>();
        try (DBIterator<Panel> dbIterator = iterator(clientSession, query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        DataResult<Panel> queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            DataResult<Long> count = count(clientSession, query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DataResult<Document> nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        return nativeGet(null, query, options);
    }

    DataResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        DataResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            DataResult<Long> count = count(clientSession, query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DataResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        DataResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            DataResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public long getStudyId(long panelUid) throws CatalogDBException {
        Bson query = new Document(PRIVATE_UID, panelUid);
        Bson projection = Projections.include(PRIVATE_STUDY_UID);
        DataResult<Document> queryResult = panelCollection.find(query, projection, null);

        if (!queryResult.getResults().isEmpty()) {
            Object studyId = queryResult.getResults().get(0).get(PRIVATE_STUDY_UID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("Panel", panelUid);
        }
    }

    @Override
    public DataResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    DataResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return panelCollection.count(clientSession, bson);
    }

    @Override
    public DataResult<Long> count(final Query query, final String user, final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        return count(null, query, user, studyPermissions);
    }

    DataResult<Long> count(ClientSession clientSession, final Query query, final String user,
                           final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_PANELS : studyPermissions);

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        DataResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(clientSession, studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getPanelPermission().name(), Entity.DISEASE_PANEL.name());
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        logger.debug("Panel count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return panelCollection.count(clientSession, bson);
    }

    @Override
    public DataResult distinct(Query query, String field) throws CatalogDBException {
        return panelCollection.distinct(field, parseQuery(query));
    }

    @Override
    public DataResult stats(Query query) {
        return null;
    }

    @Override
    public DataResult update(long panelUid, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        DataResult<Panel> dataResult = get(panelUid, options);

        if (dataResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update panel. Panel '" + panelUid + "' not found.");
        }

        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, dataResult.first(), parameters, queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update panel {}: {}", dataResult.first().getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not update panel '" + dataResult.first().getId() + "': " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public DataResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single panel
            if (count(query).first() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one panel");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Panel> iterator = iterator(query, options);

        DataResult<Panel> result = DataResult.empty();

        while (iterator.hasNext()) {
            Panel panel = iterator.next();
            try {
                result.append(runTransaction(clientSession -> privateUpdate(clientSession, panel, parameters, queryOptions)));
            } catch (CatalogDBException e) {
                logger.error("Could not update panel {}: {}", panel.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, panel.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    private DataResult<Object> privateUpdate(ClientSession clientSession, Panel panel, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException {
        long tmpStartTime = startQuery();
        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), panel.getStudyUid())
                .append(QueryParams.UID.key(), panel.getUid());

        if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            createNewVersion(clientSession, panel.getStudyUid(), panel.getUid());
        }

        Document panelUpdate = parseAndValidateUpdateParams(clientSession, parameters, tmpQuery);

        if (panelUpdate.isEmpty()) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to be updated");
        }

        Bson finalQuery = parseQuery(tmpQuery);
        logger.debug("Panel update: query : {}, update: {}",
                finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                panelUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        DataResult result = panelCollection.update(clientSession, finalQuery, new Document("$set", panelUpdate),
                new QueryOptions("multi", true));

        if (result.getNumMatches() == 0) {
            throw new CatalogDBException("Panel " + panel.getId() + " not found");
        }
        List<Event> events = new ArrayList<>();
        if (result.getNumUpdated() == 0) {
            events.add(new Event(Event.Type.WARNING, panel.getId(), "Panel was already updated"));
        }
        logger.debug("Panel {} successfully updated", panel.getId());

        return endWrite(tmpStartTime, 1, 1, events);
    }

    private void createNewVersion(ClientSession clientSession, long studyUid, long panelUid) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), panelUid);
        DataResult<Document> queryResult = nativeGet(clientSession, query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not find panel '" + panelUid + "'");
        }

        createNewVersion(clientSession, panelCollection, queryResult.first());
    }


    private Document parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query)
            throws CatalogDBException {
        Document panelParameters = new Document();

        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.DESCRIPTION.key(), QueryParams.AUTHOR.key()};
        filterStringParams(parameters, panelParameters, acceptedParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, panelParameters, acceptedMapParams);

        String[] acceptedParamsList = { QueryParams.TAGS.key()};
        filterStringListParams(parameters, panelParameters, acceptedParamsList);

        final String[] acceptedObjectParams = {QueryParams.VARIANTS.key(), QueryParams.PHENOTYPES.key(), QueryParams.REGIONS.key(),
                QueryParams.GENES.key(), QueryParams.SOURCE.key(), QueryParams.CATEGORIES.key()};
        filterObjectParams(parameters, panelParameters, acceptedObjectParams);

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one panel...

            Query tmpQuery = new Query(query);
            // We take out ALL_VERSION from query just in case we get multiple results...
            tmpQuery.remove(Constants.ALL_VERSIONS);

            DataResult<Panel> panelDataResult = get(clientSession, tmpQuery, new QueryOptions());
            if (panelDataResult.getNumResults() == 0) {
                throw new CatalogDBException("Update panel: No panel found to be updated");
            }
            if (panelDataResult.getNumResults() > 1) {
                throw new CatalogDBException("Update panel: Cannot update " + QueryParams.ID.key() + " parameter. More than one panel "
                        + "found to be updated.");
            }

            // Check that the new sample name is still unique
            long studyId = panelDataResult.first().getStudyUid();

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            DataResult<Long> count = count(clientSession, tmpQuery);
            if (count.getResults().get(0) > 0) {
                throw new CatalogDBException("Cannot update the " + QueryParams.ID.key() + ". Panel "
                        + parameters.get(QueryParams.ID.key()) + " already exists.");
            }

            panelParameters.put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            panelParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            panelParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (!panelParameters.isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            panelParameters.put(QueryParams.MODIFICATION_DATE.key(), time);
            panelParameters.put(PRIVATE_MODIFICATION_DATE, date);
        }

        return panelParameters;
    }

    @Override
    public DataResult delete(Panel panel) throws CatalogDBException {
        try {
            return runTransaction(clientSession -> privateDelete(clientSession, panel));
        } catch (CatalogDBException e) {
            logger.error("Could not delete panel {}: {}", panel.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete panel '" + panel.getId() + "': " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public DataResult delete(Query query) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Panel> iterator = iterator(query, options);

        DataResult<Panel> result = DataResult.empty();

        while (iterator.hasNext()) {
            Panel panel = iterator.next();
            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, panel)));
            } catch (CatalogDBException e) {
                logger.error("Could not delete panel {}: {}", panel.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, panel.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    private DataResult<Object> privateDelete(ClientSession clientSession, Panel panel) throws CatalogDBException {
        long tmpStartTime = startQuery();
        logger.debug("Deleting panel {} ({})", panel.getId(), panel.getUid());

        String deleteSuffix = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

        Query panelQuery = new Query()
                .append(QueryParams.UID.key(), panel.getUid())
                .append(QueryParams.VERSION.key(), panel.getVersion())
                .append(QueryParams.STUDY_UID.key(), panel.getStudyUid());
        // Mark the panel as deleted
        ObjectMap updateParams = new ObjectMap()
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED)
                .append(QueryParams.STATUS_DATE.key(), TimeUtils.getTime())
                .append(QueryParams.ID.key(), panel.getId() + deleteSuffix);

        Bson bsonQuery = parseQuery(panelQuery);
        Document updateDocument = parseAndValidateUpdateParams(clientSession, updateParams, panelQuery);

        logger.debug("Delete panel '{}': Query: {}, update: {}", panel.getId(),
                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult result = panelCollection.update(clientSession, bsonQuery, new Document("$set", updateDocument),
                QueryOptions.empty());

        if (result.getNumMatches() == 0) {
            throw new CatalogDBException("Panel " + panel.getId() + " not found");
        }
        List<Event> events = new ArrayList<>();
        if (result.getNumUpdated() == 0) {
            events.add(new Event(Event.Type.WARNING, panel.getId(), "Panel was already deleted"));
        }
        logger.debug("Panel {} successfully deleted", panel.getId());

        return endWrite(tmpStartTime, 1, 0, 0, 1, events);
    }

    @Override
    public DataResult remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public DataResult remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public DataResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Restore not yet implemented.");
    }

    @Override
    public DataResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Restore not yet implemented.");
    }

    @Override
    public DBIterator<Panel> iterator(Query query, QueryOptions options) throws CatalogDBException {
        return iterator(null, query, options);
    }

    DBIterator<Panel> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new MongoDBIterator<>(mongoCursor, panelConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        return nativeIterator(null, query, options);
    }

    DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new MongoDBIterator(mongoCursor);
    }

    @Override
    public DBIterator<Panel> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return iterator(null, query, options, user);
    }

    DBIterator<Panel> iterator(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(clientSession, query, options, studyDocument, user);
        return new MongoDBIterator<>(mongoCursor, panelConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(null, query, queryOptions, studyDocument, user);
        return new MongoDBIterator<>(mongoCursor);
    }


    private MongoCursor<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> documentMongoCursor;
        try {
            documentMongoCursor = getMongoCursor(clientSession, query, options, null, null);
        } catch (CatalogAuthorizationException e) {
            throw new CatalogDBException(e);
        }
        return documentMongoCursor;
    }

    private MongoCursor<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, Document studyDocument,
                                                 String user) throws CatalogDBException, CatalogAuthorizationException {
        Document queryForAuthorisedEntries = null;
        if (studyDocument != null && user != null) {
            // Get the document query needed to check the permissions as well
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user, StudyAclEntry.StudyPermissions.VIEW_PANELS.name(),
                    PanelAclEntry.PanelPermissions.VIEW.name(), Entity.DISEASE_PANEL.name());
        }

        Query finalQuery = new Query(query);
        filterOutDeleted(finalQuery);

        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        Bson bson = parseQuery(finalQuery, queryForAuthorisedEntries);
        logger.debug("Panel query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return panelCollection.nativeQuery().find(clientSession, bson, qOptions).iterator();
    }

    private Document getStudyDocument(Query query) throws CatalogDBException {
        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        DataResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
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
    public DataResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return rank(panelCollection, bsonQuery, field, QueryParams.ID.key(), numResults, asc);
    }

    @Override
    public DataResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(panelCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public DataResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(panelCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public DataResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_PANELS.name(), PanelAclEntry.PanelPermissions.VIEW.name(), Entity.DISEASE_PANEL.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(panelCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public DataResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_PANELS.name(), PanelAclEntry.PanelPermissions.VIEW.name(), Entity.DISEASE_PANEL.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
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

    @Override
    public DataResult updateProjectRelease(long studyId, int release) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query);

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);

        return panelCollection.update(bson, update, queryOptions);
    }

    @Override
    public DataResult unmarkPermissionRule(long studyId, String permissionRuleId) {
        return unmarkPermissionRule(panelCollection, studyId, permissionRuleId);
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        return parseQuery(query, null);
    }

    private Bson parseQuery(Query query, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        Query queryCopy = new Query(query);

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), queryCopy);

        boolean uidVersionQueryFlag = generateUidVersionQuery(queryCopy, andBsonList);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
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
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case SNAPSHOT:
                        addAutoOrQuery(RELEASE_FROM_VERSION, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case GENES:
                        addAutoOrQuery(QueryParams.GENES_ID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case PHENOTYPES:
                        addAutoOrQuery(QueryParams.PHENOTYPES_ID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case REGIONS:
                        addAutoOrQuery(QueryParams.REGIONS_LOCATION.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case VARIANTS:
                        addAutoOrQuery(QueryParams.VARIANTS_ID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CATEGORIES:
                        addAutoOrQuery(QueryParams.CATEGORIES_NAME.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(Status.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case NAME:
                    case RELEASE:
                    case VERSION:
                    case DESCRIPTION:
                    case AUTHOR:
                    case TAGS:
                    case CATEGORIES_NAME:
                    case VARIANTS_ID:
                    case VARIANTS_PHENOTYPE:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                    case PHENOTYPES_SOURCE:
                    case GENES_ID:
                    case GENES_NAME:
                    case GENES_CONFIDENCE:
                    case REGIONS_LOCATION:
                    case REGIONS_SCORE:
                    case STATUS_MSG:
                    case STATUS_DATE:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                if (e instanceof CatalogDBException) {
                    throw e;
                } else {
                    throw new CatalogDBException("Error parsing query : " + queryCopy.toJson(), e);
                }
            }
        }

        // If the user doesn't look for a concrete version...
        if (!uidVersionQueryFlag && !queryCopy.getBoolean(Constants.ALL_VERSIONS) && !queryCopy.containsKey(QueryParams.VERSION.key())) {
            if (queryCopy.containsKey(QueryParams.SNAPSHOT.key())) {
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
