/*
 * Copyright 2015-2020 OpenCB
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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.PanelConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelAclEntry;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;


public class PanelMongoDBAdaptor extends MongoDBAdaptor implements PanelDBAdaptor {

    private final MongoDBCollection panelCollection;
    private final MongoDBCollection panelArchiveCollection;
    private final MongoDBCollection deletedPanelCollection;
    private PanelConverter panelConverter;

    public PanelMongoDBAdaptor(MongoDBCollection panelCollection, MongoDBCollection panelArchiveCollection,
                               MongoDBCollection deletedPanelCollection, Configuration configuration,
                               MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(JobMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.panelCollection = panelCollection;
        this.panelArchiveCollection = panelArchiveCollection;
        this.deletedPanelCollection = deletedPanelCollection;
        this.panelConverter = new PanelConverter();
    }

    /**
     * @return MongoDB connection to the disease panel collection.
     */
    public MongoDBCollection getPanelCollection() {
        return panelCollection;
    }

    public MongoDBCollection getPanelArchiveCollection() {
        return panelArchiveCollection;
    }

    @Override
    public OpenCGAResult insert(long studyUid, List<Panel> panelList) throws CatalogDBException, CatalogParameterException,
            CatalogAuthorizationException {
        if (panelList == null || panelList.isEmpty()) {
            throw new CatalogDBException("Missing panel list");
        }
        if (studyUid <= 0) {
            throw new CatalogDBException("Missing study uid");
        }
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting insert transaction of {} panels", panelList.size());
            dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyUid);

            for (Panel panel : panelList) {
                insert(clientSession, studyUid, panel);
            }
            return endWrite(tmpStartTime, panelList.size(), panelList.size(), 0, 0, null);
        }, e -> logger.error("Could not insert {} panels: {}", panelList.size(), e.getMessage()));
    }

    @Override
    public OpenCGAResult insert(long studyUid, Panel panel, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting insert transaction of panel id '{}'", panel.getId());

            dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyUid);
            insert(clientSession, studyUid, panel);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create panel {}: {}", panel.getId(), e.getMessage()));
    }

    void insert(ClientSession clientSession, long studyUid, Panel panel) throws CatalogDBException {
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), panel.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyUid));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = panelCollection.count(clientSession, bson);
        if (count.getNumMatches() > 0) {
            throw CatalogDBException.alreadyExists("panel", QueryParams.ID.key(), panel.getId());
        }
        panel.setStats(fetchStats(panel));

        logger.debug("Inserting new panel '" + panel.getId() + "'");

        Document panelDocument = getPanelDocumentForInsertion(clientSession, panel, studyUid);
        insertVersionedModel(clientSession, panelDocument, panelCollection, panelArchiveCollection);
        logger.info("Panel '" + panel.getId() + "(" + panel.getUid() + ")' successfully created");
    }

    private Map<String, Integer> fetchStats(Panel panel) {
        Map<String, Integer> stats = new HashMap<>();
        stats.put("numberOfVariants", panel.getVariants() != null ? panel.getVariants().size() : 0);
        stats.put("numberOfGenes", panel.getGenes() != null ? panel.getGenes().size() : 0);
        stats.put("numberOfRegions", panel.getRegions() != null ? panel.getRegions().size() : 0);
        return stats;
    }

    Document getPanelDocumentForInsertion(ClientSession clientSession, Panel panel, long studyUid) {
        //new Panel Id
        long panelUid = getNewUid(clientSession);
        panel.setUid(panelUid);
        panel.setStudyUid(studyUid);
        if (StringUtils.isEmpty(panel.getUuid())) {
            panel.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PANEL));
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
        panelDocument.put(PRIVATE_MODIFICATION_DATE, panelDocument.get(PRIVATE_CREATION_DATE));
        panelDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        return panelDocument;
    }

    @Override
    public OpenCGAResult<Panel> get(long panelUid, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkUid(panelUid);
        Query query = new Query(QueryParams.UID.key(), panelUid)
                .append(QueryParams.STUDY_UID.key(), getStudyId(panelUid));
        return get(query, options);
    }

    @Override
    public OpenCGAResult<Panel> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return get(null, studyUid, query, options, user);
    }

    OpenCGAResult<Panel> get(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Panel> dbIterator = iterator(clientSession, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Panel> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(null, query, options);
    }

    OpenCGAResult<Panel> get(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Panel> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Document> nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeGet(null, query, options);
    }

    public OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeGet(null, studyUid, query, options, user);
    }

    public OpenCGAResult<Document> nativeGet(ClientSession session, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(session, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
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
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(panelCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult<Long> count(final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query, user);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        logger.debug("Panel count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(panelCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult update(long panelUid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        OpenCGAResult<Panel> dataResult = get(panelUid, options);

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
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single panel
            if (count(query).getNumMatches() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one panel");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Panel> iterator = iterator(query, options);

        OpenCGAResult<Panel> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Panel panel = iterator.next();
            try {
                result.append(runTransaction(clientSession -> privateUpdate(clientSession, panel, parameters, queryOptions)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not update panel {}: {}", panel.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, panel.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    private OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Panel panel, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), panel.getStudyUid())
                .append(QueryParams.UID.key(), panel.getUid());

        Document panelUpdate = parseAndValidateUpdateParams(clientSession, parameters, tmpQuery);

        if (panelUpdate.isEmpty()) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to be updated");
        }

        Bson finalQuery = parseQuery(tmpQuery);
        return updateVersionedModel(clientSession, finalQuery, panelCollection, panelArchiveCollection, () -> {
                    logger.debug("Panel update: query : {}, update: {}",
                            finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                            panelUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

                    DataResult result = panelCollection.update(clientSession, finalQuery, new Document("$set", panelUpdate),
                            new QueryOptions("multi", true));

                    if (parameters.containsKey(QueryParams.VARIANTS.key()) || parameters.containsKey(QueryParams.GENES.key())
                            || parameters.containsKey(QueryParams.REGIONS.key())) {
                        // Recalculate stats
                        QueryOptions statsOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(QueryParams.VARIANTS_ID.key(),
                                QueryParams.GENES_ID.key(), QueryParams.REGIONS_ID.key()));

                        Panel tmpPanel = get(clientSession, tmpQuery, statsOptions).first();
                        Map<String, Integer> stats = fetchStats(tmpPanel);
                        panelCollection.update(clientSession, finalQuery,
                                new Document("$set", new Document(QueryParams.STATS.key(), stats)), QueryOptions.empty());
                    }

                    if (result.getNumMatches() == 0) {
                        throw new CatalogDBException("Panel " + panel.getId() + " not found");
                    }

                    List<Event> events = new ArrayList<>();
                    if (result.getNumUpdated() == 0) {
                        events.add(new Event(Event.Type.WARNING, panel.getId(), "Panel was already updated"));
                    }
                    logger.debug("Panel {} successfully updated", panel.getId());

                    return endWrite(tmpStartTime, 1, 1, events);
                }, (MongoDBIterator<Document> iterator) -> updateReferencesAfterPanelVersionIncrement(clientSession, iterator)
        );
    }

    private void updateReferencesAfterPanelVersionIncrement(ClientSession clientSession, MongoDBIterator<Document> iterator)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        while (iterator.hasNext()) {
            Panel panel = panelConverter.convertToDataModelType(iterator.next());
            dbAdaptorFactory.getClinicalAnalysisDBAdaptor().updateClinicalAnalysisPanelReferences(clientSession, panel);
        }
    }

    private Document parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Document panelParameters = new Document();

        final String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.DESCRIPTION.key(), QueryParams.SOURCE_AUTHOR.key(),
                QueryParams.SOURCE_ID.key(), QueryParams.SOURCE_NAME.key(), QueryParams.SOURCE_VERSION.key(),
                QueryParams.SOURCE_PROJECT.key()};
        filterStringParams(parameters, panelParameters, acceptedParams);

        final String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, panelParameters, acceptedMapParams);

        String[] acceptedParamsList = {QueryParams.TAGS.key()};
        filterStringListParams(parameters, panelParameters, acceptedParamsList);

        final String[] acceptedObjectParams = {QueryParams.VARIANTS.key(), QueryParams.DISORDERS.key(), QueryParams.REGIONS.key(),
                QueryParams.GENES.key(), QueryParams.CATEGORIES.key(), QueryParams.INTERNAL_STATUS.key()};
        filterObjectParams(parameters, panelParameters, acceptedObjectParams);

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one panel...

            Query tmpQuery = new Query(query);
            // We take out ALL_VERSION from query just in case we get multiple results...
            tmpQuery.remove(Constants.ALL_VERSIONS);

            OpenCGAResult<Panel> panelDataResult = get(clientSession, tmpQuery, new QueryOptions());
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
            OpenCGAResult<Long> count = count(clientSession, tmpQuery);
            if (count.getNumMatches() > 0) {
                throw new CatalogDBException("Cannot update the " + QueryParams.ID.key() + ". Panel "
                        + parameters.get(QueryParams.ID.key()) + " already exists.");
            }

            panelParameters.put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        if (parameters.containsKey(QueryParams.STATUS_ID.key())) {
            panelParameters.put(QueryParams.STATUS_ID.key(), parameters.get(QueryParams.STATUS_ID.key()));
            panelParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (!panelParameters.isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                panelParameters.put(QueryParams.MODIFICATION_DATE.key(), time);
                panelParameters.put(PRIVATE_MODIFICATION_DATE, date);
            }
            panelParameters.put(INTERNAL_LAST_MODIFIED, time);
        }

        return panelParameters;
    }

    @Override
    public OpenCGAResult delete(Panel panel) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            Query query = new Query()
                    .append(QueryParams.UID.key(), panel.getUid())
                    .append(QueryParams.STUDY_UID.key(), panel.getStudyUid());
            OpenCGAResult<Document> result = nativeGet(query, new QueryOptions());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Could not find panel " + panel.getId() + " with uid " + panel.getUid());
            }
            return runTransaction(clientSession -> privateDelete(clientSession, result.first()));
        } catch (CatalogDBException e) {
            logger.error("Could not delete panel {}: {}", panel.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete panel '" + panel.getId() + "': " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        DBIterator<Document> iterator = nativeIterator(query, QueryOptions.empty());

        OpenCGAResult<Panel> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Document panel = iterator.next();
            String panelId = panel.getString(QueryParams.ID.key());
            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, panel)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete panel {}: {}", panelId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, panelId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    private OpenCGAResult<Object> privateDelete(ClientSession clientSession, Document panelDocument)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        String panelId = panelDocument.getString(QueryParams.ID.key());
        long panelUid = panelDocument.getLong(PRIVATE_UID);
        long studyUid = panelDocument.getLong(PRIVATE_STUDY_UID);

        logger.debug("Deleting panel {} ({})", panelId, panelUid);

        // Check if panel is in use in a case
        Query queryCheck = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.PANELS_UID.key(), panelUid)
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        OpenCGAResult<Long> count = dbAdaptorFactory.getClinicalAnalysisDBAdaptor().count(clientSession, queryCheck);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Could not delete panel. Panel is in use in " + count.getNumMatches() + " cases");
        }

        // Look for all the different panel versions
        Query panelQuery = new Query()
                .append(QueryParams.UID.key(), panelUid)
                .append(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(panelQuery);
        deleteVersionedModel(clientSession, bson, panelCollection, panelArchiveCollection, deletedPanelCollection);
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
    }

    @Override
    public OpenCGAResult remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public OpenCGAResult remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Restore not yet implemented.");
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Restore not yet implemented.");
    }

    @Override
    public DBIterator<Panel> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return iterator(null, query, options);
    }

    DBIterator<Panel> iterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, panelConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(null, query, options);
    }

    DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new CatalogMongoDBIterator(mongoCursor);
    }

    @Override
    public DBIterator<Panel> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return iterator(null, studyUid, query, options, user);
    }

    DBIterator<Panel> iterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options, user);
        return new CatalogMongoDBIterator<>(mongoCursor, panelConverter);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeIterator(null, studyUid, query, options, user);
    }

    public DBIterator<Document> nativeIterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions, user);
        return new CatalogMongoDBIterator<>(mongoCursor);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(clientSession, query, options, null);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = new Query(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

        Bson bson = parseQuery(finalQuery, user);
        logger.debug("Panel query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        MongoDBCollection collection = getQueryCollection(query, panelCollection, panelArchiveCollection, deletedPanelCollection);
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return rank(panelCollection, bsonQuery, field, QueryParams.ID.key(), numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(panelCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(panelCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query, user);
        return groupBy(panelCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public <T> OpenCGAResult<T> distinct(long studyUid, String field, Query query, String userId, Class<T> clazz)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        return new OpenCGAResult(panelCollection.distinct(field, bson, clazz));
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(action);
        try (DBIterator<Panel> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    @Override
    public OpenCGAResult updateProjectRelease(long studyId, int release)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query);

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));
        QueryOptions queryOptions = new QueryOptions("multi", true);

        return updateVersionedModelNoVersionIncrement(bson, panelCollection, panelArchiveCollection,
                () -> new OpenCGAResult<Panel>(panelCollection.update(bson, update, queryOptions)));
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) {
        return unmarkPermissionRule(panelCollection, studyId, permissionRuleId);
    }

    private Bson parseQuery(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, null);
    }

    private Bson parseQuery(Query query, String user) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, user);
    }

    protected Bson parseQuery(Query query, Document extraQuery)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, extraQuery, null);
    }

    private Bson parseQuery(Query query, Document extraQuery, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Bson> andBsonList = new ArrayList<>();

        if (query.containsKey(QueryParams.STUDY_UID.key())
                && (StringUtils.isNotEmpty(user) || query.containsKey(ParamConstants.ACL_PARAM))) {
            Document studyDocument = getStudyDocument(null, query.getLong(QueryParams.STUDY_UID.key()));

            if (query.containsKey(ParamConstants.ACL_PARAM)) {
                andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.DISEASE_PANEL, user,
                        configuration));
            } else {
                // Get the document query needed to check the permissions as well
                andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, PanelAclEntry.PanelPermissions.VIEW.name(),
                        Enums.Resource.DISEASE_PANEL, configuration));
            }

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        if ("all".equalsIgnoreCase(queryCopy.getString(QueryParams.VERSION.key()))) {
            queryCopy.put(Constants.ALL_VERSIONS, true);
            queryCopy.remove(QueryParams.VERSION.key());
        }

        boolean uidVersionQueryFlag = generateUidVersionQuery(queryCopy, andBsonList);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
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
                        addDefaultOrQueryFilter(queryParam.key(), queryParam.key(), queryCopy, andBsonList);
                        break;
                    case DISORDERS:
                        addDefaultOrQueryFilter(queryParam.key(), queryParam.key(), queryCopy, andBsonList);
                        break;
                    case REGIONS:
                    case REGIONS_ID:
                        addAutoOrQuery(QueryParams.REGIONS_ID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case VARIANTS:
                    case VARIANTS_ID:
                        addAutoOrQuery(QueryParams.VARIANTS_ID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CATEGORIES:
                    case CATEGORIES_NAME:
                        addAutoOrQuery(QueryParams.CATEGORIES_NAME.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case STATUS:
                    case STATUS_ID:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(InternalStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.STATUS_ID.key(), queryParam.key(), query, QueryParams.STATUS_ID.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        query.put(queryParam.key(), InternalStatus.getPositiveStatus(InternalStatus.STATUS_LIST,
                                query.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_ID.key(), queryParam.key(), query,
                                QueryParams.INTERNAL_STATUS_ID.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case NAME:
                    case RELEASE:
                    case VERSION:
                    case TAGS:
                    case DISORDERS_ID:
                    case DISORDERS_NAME:
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
        if (!uidVersionQueryFlag && !queryCopy.getBoolean(Constants.ALL_VERSIONS) && !queryCopy.containsKey(QueryParams.VERSION.key())
                && queryCopy.containsKey(QueryParams.SNAPSHOT.key())) {
            // If the user looks for anything from some release, we will try to find the latest from the release (snapshot)
            andBsonList.add(Filters.eq(LAST_OF_RELEASE, true));
        }

        if (extraQuery != null && extraQuery.size() > 0) {
            andBsonList.add(extraQuery);
        }
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
