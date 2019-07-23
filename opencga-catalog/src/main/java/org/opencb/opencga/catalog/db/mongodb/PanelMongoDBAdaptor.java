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
import com.mongodb.client.TransactionBody;
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
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.datastore.core.result.WriteResult;
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
    public void insert(Panel panel, boolean overwrite) throws CatalogDBException {
        ClientSession clientSession = getClientSession();
        TransactionBody<WriteResult> txnBody = () -> {
            long tmpStartTime = startQuery();

            logger.debug("Starting insert transaction of global panel id '{}'", panel.getId());

            try {
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

                return endWrite(String.valueOf(panel.getUid()), tmpStartTime, 1, 1, null);
            } catch (CatalogDBException e) {
                logger.error("Could not create global panel {}: {}", panel.getId(), e.getMessage());
                clientSession.abortTransaction();
                return endWrite(panel.getId(), tmpStartTime, 1, 0,
                        Collections.singletonList(new WriteResult.Fail(panel.getId(), e.getMessage())));
            }
        };

        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumModified() == 0) {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
    }

    @Override
    public QueryResult<Panel> insert(long studyUid, Panel panel, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        ClientSession clientSession = getClientSession();
        TransactionBody<WriteResult> txnBody = () -> {
            long tmpStartTime = startQuery();

            logger.debug("Starting insert transaction of panel id '{}'", panel.getId());

            try {
                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyUid);
                List<Bson> filterList = new ArrayList<>();
                filterList.add(Filters.eq(QueryParams.ID.key(), panel.getId()));
                filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyUid));
                filterList.add(Filters.eq(QueryParams.STATUS_NAME.key(), Status.READY));

                Bson bson = Filters.and(filterList);
                QueryResult<Long> count = panelCollection.count(bson);

                if (count.first() > 0) {
                    throw CatalogDBException.alreadyExists("panel", QueryParams.ID.key(), panel.getId());
                }

                logger.debug("Inserting new panel '" + panel.getId() + "'");

                Document panelDocument = getPanelDocumentForInsertion(clientSession, panel, studyUid);
                panelCollection.insert(clientSession, panelDocument, null);

                logger.info("Panel '" + panel.getId() + "(" + panel.getUid() + ")' successfully created");

                return endWrite(String.valueOf(panel.getUid()), tmpStartTime, 1, 1, null);
            } catch (CatalogDBException e) {
                logger.error("Could not create panel {}: {}", panel.getId(), e.getMessage());
                clientSession.abortTransaction();
                return endWrite(panel.getId(), tmpStartTime, 1, 0,
                        Collections.singletonList(new WriteResult.Fail(panel.getId(), e.getMessage())));
            }
        };

        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumModified() == 1) {
            Query query = new Query()
                    .append(QueryParams.STUDY_UID.key(), studyUid)
                    .append(QueryParams.UID.key(), Long.parseLong(result.getId()));
            return endQuery("Create panel", startTime, get(query, options));
        } else {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
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
    public QueryResult<Panel> get(long panelUid, QueryOptions options) throws CatalogDBException {
        checkUid(panelUid);
        Query query = new Query(QueryParams.UID.key(), panelUid).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED)
                .append(QueryParams.STUDY_UID.key(), getStudyId(panelUid));
        return get(query, options);
    }

    @Override
    public QueryResult<Panel> get(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        return get(null, query, options, user);
    }

    QueryResult<Panel> get(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Panel> documentList = new ArrayList<>();
        QueryResult<Panel> queryResult;
        try (DBIterator<Panel> dbIterator = iterator(clientSession, query, options, user)) {
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
            QueryResult<Long> count = count(clientSession, query, user, StudyAclEntry.StudyPermissions.VIEW_PANELS);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Panel> get(Query query, QueryOptions options) throws CatalogDBException {
        return get(null, query, options);
    }

    QueryResult<Panel> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Panel> documentList = new ArrayList<>();
        try (DBIterator<Panel> dbIterator = iterator(clientSession, query, options)) {
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
            QueryResult<Long> count = count(clientSession, query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<Document> nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        return nativeGet(null, query, options);
    }

    QueryResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
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
            QueryResult<Long> count = count(clientSession, query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options, user)) {
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
        Bson projection = Projections.include(PRIVATE_STUDY_UID);
        QueryResult<Document> queryResult = panelCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(PRIVATE_STUDY_UID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("Panel", panelUid);
        }
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    QueryResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return panelCollection.count(clientSession, bson);
    }

    @Override
    public QueryResult<Long> count(final Query query, final String user, final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        return count(null, query, user, studyPermissions);
    }

    QueryResult<Long> count(ClientSession clientSession, final Query query, final String user,
                            final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_PANELS : studyPermissions);

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(clientSession, studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getPanelPermission().name(), Entity.PANEL.name());
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        logger.debug("Panel count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return panelCollection.count(clientSession, bson);
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
    public QueryResult<Panel> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        WriteResult update = update(new Query(QueryParams.UID.key(), id), parameters, queryOptions);
        if (update.getNumModified() != 1) {
            throw new CatalogDBException("Could not update panel with id " + id);
        }
        Query query = new Query()
                .append(QueryParams.UID.key(), id)
                .append(QueryParams.STUDY_UID.key(), getStudyId(id))
                .append(QueryParams.STATUS_NAME.key(), "!=EMPTY");
        return endQuery("Update panel", startTime, get(query, queryOptions));
    }

    @Override
    public WriteResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single panel
            if (count(query).first() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one panel");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Panel> iterator = iterator(query, options);

        int numMatches = 0;
        int numModified = 0;
        List<WriteResult.Fail> failList = new ArrayList<>();

        while (iterator.hasNext()) {
            Panel panel = iterator.next();
            numMatches += 1;

            ClientSession clientSession = getClientSession();
            TransactionBody<WriteResult> txnBody = () -> {
                long tmpStartTime = startQuery();
                try {
                    Query tmpQuery = new Query()
                            .append(QueryParams.STUDY_UID.key(), panel.getStudyUid())
                            .append(QueryParams.UID.key(), panel.getUid());

                    if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
                        createNewVersion(clientSession, panel.getStudyUid(), panel.getUid());
                    }

                    Document panelUpdate = parseAndValidateUpdateParams(clientSession, parameters, tmpQuery);

                    if (!panelUpdate.isEmpty()) {
                        Bson finalQuery = parseQuery(tmpQuery);

                        logger.debug("Panel update: query : {}, update: {}",
                                finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                                panelUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

                        panelCollection.update(clientSession, finalQuery, new Document("$set", panelUpdate),
                                new QueryOptions("multi", true));
                    }

                    return endWrite(panel.getId(), tmpStartTime, 1, 1, null);
                } catch (CatalogDBException e) {
                    logger.error("Error updating panel {}({}). {}", panel.getId(), panel.getUid(), e.getMessage(), e);
                    clientSession.abortTransaction();
                    return endWrite(panel.getId(), tmpStartTime, 1, 0,
                            Collections.singletonList(new WriteResult.Fail(panel.getId(), e.getMessage())));
                }
            };

            WriteResult result = commitTransaction(clientSession, txnBody);

            if (result.getNumModified() == 1) {
                logger.info("Panel {} successfully updated", panel.getId());
                numModified += 1;
            } else {
                if (result.getFailed() != null && !result.getFailed().isEmpty()) {
                    logger.error("Could not update panel {}: {}", panel.getId(), result.getFailed().get(0).getMessage());
                    failList.addAll(result.getFailed());
                } else {
                    logger.error("Could not update panel {}", panel.getId());
                }
            }
        }

        Error error = null;
        if (!failList.isEmpty()) {
            error = new Error(-1, "update", (numModified == 0
                    ? "None of the panels could be updated"
                    : "Some of the panels could not be updated"));
        }

        return endWrite("update", startTime, numMatches, numModified, failList, null, error);
    }

    private void createNewVersion(ClientSession clientSession, long studyUid, long panelUid) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), panelUid);
        QueryResult<Document> queryResult = nativeGet(clientSession, query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not find panel '" + panelUid + "'");
        }

        createNewVersion(clientSession, panelCollection, queryResult.first());
    }


    private Document parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query)
            throws CatalogDBException {
        Document panelParameters = new Document();

        final String[] acceptedParams = {UpdateParams.NAME.key(), UpdateParams.DESCRIPTION.key(), UpdateParams.AUTHOR.key()};
        filterStringParams(parameters, panelParameters, acceptedParams);

        final String[] acceptedMapParams = {UpdateParams.ATTRIBUTES.key(), UpdateParams.STATS.key()};
        filterMapParams(parameters, panelParameters, acceptedMapParams);

        String[] acceptedParamsList = { UpdateParams.TAGS.key()};
        filterStringListParams(parameters, panelParameters, acceptedParamsList);

        final String[] acceptedObjectParams = {UpdateParams.VARIANTS.key(), UpdateParams.PHENOTYPES.key(), UpdateParams.REGIONS.key(),
                UpdateParams.GENES.key(), UpdateParams.SOURCE.key(), UpdateParams.CATEGORIES.key()};
        filterObjectParams(parameters, panelParameters, acceptedObjectParams);

        if (parameters.containsKey(UpdateParams.ID.key())) {
            // That can only be done to one panel...

            Query tmpQuery = new Query(query);
            // We take out ALL_VERSION from query just in case we get multiple results...
            tmpQuery.remove(Constants.ALL_VERSIONS);

            QueryResult<Panel> panelQueryResult = get(clientSession, tmpQuery, new QueryOptions());
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
            QueryResult<Long> count = count(clientSession, tmpQuery);
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
    public WriteResult delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);
        return delete(query);
    }

    @Override
    public WriteResult delete(Query query) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Panel> iterator = iterator(query, options);

        long startTime = startQuery();
        int numMatches = 0;
        int numModified = 0;
        List<WriteResult.Fail> failList = new ArrayList<>();

        while (iterator.hasNext()) {
            Panel panel = iterator.next();
            numMatches += 1;

            ClientSession clientSession = getClientSession();
            TransactionBody<WriteResult> txnBody = () -> {
                long tmpStartTime = startQuery();
                try {
                    logger.info("Deleting panel {} ({})", panel.getId(), panel.getUid());

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
                    UpdateResult panelResult = panelCollection.update(clientSession, bsonQuery, new Document("$set", updateDocument),
                            QueryOptions.empty()).first();
                    if (panelResult.getModifiedCount() == 1) {
                        logger.info("Panel {}({}) deleted", panel.getId(), panel.getUid());
                        return endWrite(panel.getId(), tmpStartTime, 1, 1, null);
                    } else {
                        logger.error("Panel {}({}) could not be deleted", panel.getId(), panel.getUid());
                        return endWrite(panel.getId(), tmpStartTime, 1, 0, null);
                    }
                } catch (CatalogDBException e) {
                    logger.error("Error deleting panel {}({}). {}", panel.getId(), panel.getUid(), e.getMessage(), e);
                    clientSession.abortTransaction();
                    return endWrite(panel.getId(), tmpStartTime, 1, 0,
                            Collections.singletonList(new WriteResult.Fail(panel.getId(), e.getMessage())));
                }
            };

            WriteResult result = commitTransaction(clientSession, txnBody);

            if (result.getNumModified() == 1) {
                logger.info("Panel {} successfully deleted", panel.getId());
                numModified += 1;
            } else {
                if (result.getFailed() != null && !result.getFailed().isEmpty()) {
                    logger.error("Could not delete panel {}: {}", panel.getId(), result.getFailed().get(0).getMessage());
                    failList.addAll(result.getFailed());
                } else {
                    logger.error("Could not delete panel {}", panel.getId());
                }
            }
        }

        Error error = null;
        if (!failList.isEmpty()) {
            error = new Error(-1, "delete", (numModified == 0
                    ? "None of the panels could be deleted"
                    : "Some of the panels could not be deleted"));
        }

        return endWrite("delete", startTime, numMatches, numModified, failList, null, error);
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
                    PanelAclEntry.PanelPermissions.VIEW.name(), Entity.PANEL.name());
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
        Bson bsonQuery = parseQuery(query);
        return rank(panelCollection, bsonQuery, field, QueryParams.ID.key(), numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(panelCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(panelCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_PANELS.name(), PanelAclEntry.PanelPermissions.VIEW.name(), Entity.PANEL.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(panelCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_PANELS.name(), PanelAclEntry.PanelPermissions.VIEW.name(), Entity.PANEL.name());
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
    public void updateProjectRelease(long studyId, int release) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query);

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);

        panelCollection.update(bson, update, queryOptions);
    }

    @Override
    public void unmarkPermissionRule(long studyId, String permissionRuleId) {
        unmarkPermissionRule(panelCollection, studyId, permissionRuleId);
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
