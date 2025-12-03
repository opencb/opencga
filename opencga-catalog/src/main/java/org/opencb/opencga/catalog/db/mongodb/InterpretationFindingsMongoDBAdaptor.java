package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.InterpretationFindingsDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.InterpretationFindingConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.CatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InterpretationFindingsMongoDBAdaptor extends CatalogMongoDBAdaptor implements InterpretationFindingsDBAdaptor {

    private final MongoDBCollection findingsCollection;
    private final MongoDBCollection archivedFindingsCollection;
    private final MongoDBCollection deletedFindingsCollection;
    private final InterpretationFindingConverter findingConverter;
    private final VersionedMongoDBAdaptor versionedMongoDBAdaptor;

    private static final String VERSION = "version";
    private static final String PRIVATE_ID = "_privateId";
    private static final String INTERPRETATION_ID = "_interpretationId";

    public InterpretationFindingsMongoDBAdaptor(MongoDBCollection findingsCollection, MongoDBCollection archivedFindingsCollection,
                                                MongoDBCollection deletedFindingsCollection, Configuration configuration,
                                                OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(InterpretationFindingsMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.findingsCollection = findingsCollection;
        this.archivedFindingsCollection = archivedFindingsCollection;
        this.deletedFindingsCollection = deletedFindingsCollection;
        this.findingConverter = new InterpretationFindingConverter();
        this.versionedMongoDBAdaptor = new VersionedMongoDBAdaptor(findingsCollection, archivedFindingsCollection,
                deletedFindingsCollection);
    }

    public MongoDBCollection getCollection() {
        return findingsCollection;
    }

    public MongoDBCollection getArchiveCollection() {
        return archivedFindingsCollection;
    }

    public MongoDBCollection getDeleteCollection() {
        return deletedFindingsCollection;
    }

    void insertMany(ClientSession clientSession, List<ClinicalVariant> clinicalVariantList, Interpretation interpretation)
            throws CatalogDBException {
        List<Document> documents = new ArrayList<>(clinicalVariantList.size());
        for (ClinicalVariant clinicalVariant : clinicalVariantList) {
            Document document = findingConverter.convertToStorageType(clinicalVariant);
            documents.add(document);
        }
        nativeInsertMany(clientSession, documents, interpretation);
    }

    void nativeInsertMany(ClientSession clientSession, List<Document> documents, Interpretation interpretation) throws CatalogDBException {
        for (Document document : documents) {
            document.put(PRIVATE_ID, generatePrivateId(interpretation.getStudyUid(), interpretation.getId(),
                    document.getString(QueryParams.ID.key())));
            document.put(VERSION, 1);
            long newUid = getNewUid(clientSession);
            document.put(PRIVATE_UID, newUid);
            document.put(PRIVATE_STUDY_UID, interpretation.getStudyUid());
            document.put(INTERPRETATION_ID, interpretation.getId());
            document.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
            document.put(PRIVATE_MODIFICATION_DATE, TimeUtils.getDate());
        }
        versionedMongoDBAdaptor.insertMany(clientSession, documents);
    }

    @Override
    OpenCGAResult<Document> nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return null;
    }

    @Override
    public OpenCGAResult<ClinicalVariant> get(Query query, QueryOptions options) {
        return null;
    }

    public OpenCGAResult<ClinicalVariant> get(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<ClinicalVariant> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    public OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    public DBIterator<ClinicalVariant> iterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new CatalogMongoDBIterator<>(mongoCursor, clientSession, findingConverter, null);
    }

    public DBIterator<Document> nativeIterator(ClientSession clientSession, Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new CatalogMongoDBIterator<>(mongoCursor, clientSession, null, null);
    }

    /**
     * Update the findings of an interpretation.
     * @param clientSession  MongoDB client session.
     * @param interpretation Interpretation object to which the findings belong.
     * @param function       Function to obtain the list of findings from the interpretation.
     * @param findings       List of findings to be updated.
     * @param action         Update action to be performed.
     * @return List containing all findings that belong to the interpretation after the update.
     * @throws CatalogParameterException Parameter exception
     * @throws CatalogDBException        Database exception
     * @throws CatalogAuthorizationException Authorization exception
     */
    public List<ClinicalVariant> updateFindings(ClientSession clientSession, Interpretation interpretation,
                                         Function<Interpretation, List<ClinicalVariant>> function, List<Document> findings,
                                         ParamUtils.UpdateAction action)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        Map<String, ClinicalVariant> currentFindingsMap = new HashMap<>();
        function.apply(interpretation).forEach(finding -> currentFindingsMap.put(finding.getId(), finding));

        switch (action) {
            case SET:
                // Empty the current findings map and add all the new findings as if the action was ADD
                currentFindingsMap.clear();
            case ADD:
                // Look for all the findings to check if any exist
                Map<String, Document> findingMap = new java.util.HashMap<>();
                for (Document finding : findings) {
                    findingMap.put(finding.getString(QueryParams.ID.key()), finding);
                }
                Query query = new Query()
                        .append(QueryParams.STUDY_UID.key(), interpretation.getStudyUid())
                        .append(QueryParams.INTERPRETATION_ID.key(), interpretation.getId())
                        .append(QueryParams.ID.key(), new ArrayList<>(findingMap.keySet()));
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(PRIVATE_UID, QueryParams.ID.key(),
                        QueryParams.VERSION.key()));

                List<Document> existingFindings = new LinkedList<>();
                OpenCGAResult<Document> queryResult = nativeGet(clientSession, query, options);
                for (Document result : queryResult.getResults()) {
                    String findingId = result.getString(QueryParams.ID.key());
                    existingFindings.add(findingMap.get(findingId));
                    findingMap.remove(findingId);
                }

                if (!findingMap.isEmpty()) {
                    // There are new findings to be added
                    List<Document> newFindings = new ArrayList<>(findingMap.values());
                    nativeInsertMany(clientSession, newFindings, interpretation);
                }

                if (!existingFindings.isEmpty()) {
                    // There are existing findings to be updated
                    nativeUpdateMany(clientSession, existingFindings, interpretation);
                }

                // Add new findings to the current findings map
                findingMap.entrySet().forEach(entry -> {
                    ClinicalVariant clinicalVariant = new ClinicalVariant();
                    clinicalVariant.setId(entry.getKey());
                    clinicalVariant.setVersion(entry.getValue().getInteger(QueryParams.VERSION.key()));

                    currentFindingsMap.put(entry.getKey(), clinicalVariant);
                });

                // Query and edit current findings to reflect the updated versions
                if (!existingFindings.isEmpty()) {
                    query = new Query()
                            .append(QueryParams.STUDY_UID.key(), interpretation.getStudyUid())
                            .append(QueryParams.INTERPRETATION_ID.key(), interpretation.getId())
                            .append(QueryParams.ID.key(),
                                    existingFindings.stream().map(d -> d.getString(QueryParams.ID.key())).collect(Collectors.toList()));
                    queryResult = nativeGet(clientSession, query, options);
                    for (Document existingFinding : queryResult.getResults()) {
                        ClinicalVariant clinicalVariant = new ClinicalVariant();
                        clinicalVariant.setId(existingFinding.getString(QueryParams.ID.key()));
                        clinicalVariant.setVersion(existingFinding.getInteger(QueryParams.VERSION.key()));

                        currentFindingsMap.put(clinicalVariant.getId(), clinicalVariant);
                    }
                }

                break;
            case REMOVE:
                // We don't touch the collection as findings should still be there for historical purposes unless the interpretation is
                // deleted.
                for (Document finding : findings) {
                    String findingId = finding.getString(QueryParams.ID.key());
                    currentFindingsMap.remove(findingId);
                }
                break;
            case REPLACE:
                // All findings that need to be replaced should be present in the currentFindingsMap
                for (Document finding : findings) {
                    if (!currentFindingsMap.containsKey(finding.getString(QueryParams.ID.key()))) {
                        throw new CatalogDBException("Cannot replace finding '" + finding.getString(QueryParams.ID.key())
                                + "' as it is not part of the current findings.");
                    }
                }
                nativeUpdateMany(clientSession, findings, interpretation);

                // Query and edit findings to reflect the updated versions
                query = new Query()
                        .append(QueryParams.STUDY_UID.key(), interpretation.getStudyUid())
                        .append(QueryParams.INTERPRETATION_ID.key(), interpretation.getId())
                        .append(QueryParams.ID.key(),
                                findings.stream().map(d -> d.getString(QueryParams.ID.key())).collect(Collectors.toList()));
                options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(PRIVATE_UID, QueryParams.ID.key(),
                        QueryParams.VERSION.key()));
                queryResult = nativeGet(clientSession, query, options);
                for (Document existingFinding : queryResult.getResults()) {
                    String findingId = existingFinding.getString(QueryParams.ID.key());
                    ClinicalVariant clinicalVariant = currentFindingsMap.get(findingId);
                    clinicalVariant.setVersion(clinicalVariant.getVersion());
                }

                break;
            default:
                throw new UnsupportedOperationException("Update action " + action + " not supported for interpretation findings");
        }

        return new ArrayList<>(currentFindingsMap.values());
    }

    private void nativeUpdateMany(ClientSession clientSession, List<Document> existingFindings, Interpretation interpretation)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        for (Document existingFinding : existingFindings) {
            // Remove version field from existingFinding as we don't want to set it, but increment it instead
            existingFinding.remove(VERSION);

            String findingId = existingFinding.getString(QueryParams.ID.key());
            Query query = new Query()
                    .append(QueryParams.STUDY_UID.key(), interpretation.getStudyUid())
                    .append(QueryParams.INTERPRETATION_ID.key(), interpretation.getId())
                    .append(QueryParams.ID.key(), findingId);
            Bson bsonQuery = parseQuery(query);
            long tmpStartTime = startQuery();
            versionedMongoDBAdaptor.update(clientSession, bsonQuery, findings -> {
                UpdateDocument updateDocument = new UpdateDocument();
                updateDocument.getSet().put(PRIVATE_MODIFICATION_DATE, TimeUtils.getDate());
                updateDocument.getSet().putAll(existingFinding);

                Document update = updateDocument.toFinalUpdateDocument();
                logger.debug("Update interpretation finding. Query: {}, Update: {}", bsonQuery.toBsonDocument(), update);

                DataResult<?> updateResult = findingsCollection.update(clientSession, bsonQuery, update, null);
                if (updateResult.getNumMatches() == 0) {
                    throw new CatalogDBException("Could not update finding '" + findingId + "'. Not found.");
                }
                return endWrite(tmpStartTime, updateResult);
            });
        }
    }

    public void delete(ClientSession clientSession, Interpretation interpretation) throws CatalogDBException {
        Bson query = Filters.and(
                Filters.eq(PRIVATE_STUDY_UID, interpretation.getStudyUid()),
                Filters.eq(INTERPRETATION_ID, interpretation.getId())
        );
        versionedMongoDBAdaptor.delete(clientSession, query);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException {
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }

//        qOptions = filterQueryOptionsToIncludeKeys(qOptions, Arrays.asList(QueryParams.ID.key(), QueryParams.UUID.key(),
//                QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.CLINICAL_ANALYSIS_ID.key()));

        logger.debug("Interpretation findings query : {}", bson.toBsonDocument());
        MongoDBCollection collection = getQueryCollection(query, findingsCollection, archivedFindingsCollection, deletedFindingsCollection);
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }


    protected Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        convertIdToPrivateId(queryCopy);
        versionedMongoDBAdaptor.generateIdVersionQuery(queryCopy, andBsonList, PRIVATE_ID);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case ID:
                        addAutoOrQuery(PRIVATE_ID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case VERSION:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                logger.error("Error with " + entry.getKey() + " " + entry.getValue());
                throw new CatalogDBException(e);
            }
        }

        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    // Queries cannot be made only with the variant id, we need to query using the private id instead
    private void convertIdToPrivateId(Query query) throws CatalogDBException {
        if (!query.containsKey(QueryParams.INTERPRETATION_ID.key())) {
            throw new CatalogDBException("Missing interpretation id to query interpretation findings.");
        }
        if (!query.containsKey(QueryParams.STUDY_UID.key())) {
            throw new CatalogDBException("Missing study uid to query interpretation findings.");
        }
        if (!query.containsKey(QueryParams.ID.key())) {
            throw new CatalogDBException("Missing finding id to query interpretation findings.");
        }
        long studyUid = query.getLong(QueryParams.STUDY_UID.key());
        String interpretationId = query.getString(QueryParams.INTERPRETATION_ID.key());
        List<String> ids = query.getAsStringList(QueryParams.ID.key());
        List<String> privateIds = new ArrayList<>(ids.size());
        for (String id : ids) {
            privateIds.add(generatePrivateId(studyUid, interpretationId, id));
        }
        query.put(QueryParams.ID.key(), privateIds);
        query.remove(QueryParams.INTERPRETATION_ID.key());
        query.remove(QueryParams.STUDY_UID.key());
    }

    private String generatePrivateId(long studyUid, String interpretationId, String findingId) {
        return studyUid + "__" + interpretationId + "__" + findingId;
    }
}
