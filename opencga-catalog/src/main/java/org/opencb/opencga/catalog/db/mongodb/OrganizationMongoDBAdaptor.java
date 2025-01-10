package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.OrganizationConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.OrganizationCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.federation.FederationServer;
import org.opencb.opencga.core.models.federation.FederationClient;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

public class OrganizationMongoDBAdaptor extends MongoDBAdaptor implements OrganizationDBAdaptor {

    private final MongoDBCollection organizationCollection;
    private final OrganizationConverter organizationConverter;

    private final String ID_COUNTER = "_idCounter";

    public OrganizationMongoDBAdaptor(MongoDBCollection organizationCollection, Configuration configuration,
                                      OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(OrganizationMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.organizationCollection = organizationCollection;
        this.organizationConverter = new OrganizationConverter();
    }

    @Override
    public OpenCGAResult<Organization> insert(Organization organization, QueryOptions options) throws CatalogException {
        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Starting organization insert transaction for organization id '{}'", organization.getId());
            insert(clientSession, organization);
            return endWrite(tmpStartTime, 1, 1, 0, 0, null);
        }, e -> logger.error("Could not create sample {}: {}", organization.getId(), e.getMessage()));
    }

    Organization insert(ClientSession clientSession, Organization organization)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {

        if (StringUtils.isEmpty(organization.getId())) {
            throw new CatalogDBException("Missing organization id");
        }

        // Check the organization does not exist
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), organization.getId()));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = organizationCollection.count(clientSession, bson);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Organization { id: '" + organization.getId() + "'} already exists.");
        }
        if (StringUtils.isEmpty(organization.getUuid())) {
            organization.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.ORGANIZATION));
        }

        Document organizationObject = organizationConverter.convertToStorageType(organization);

        // Counter private parameter
        organizationObject.put(ID_COUNTER, 0L);

        // Versioning private parameters
        organizationObject.put(PRIVATE_CREATION_DATE, StringUtils.isNotEmpty(organization.getCreationDate())
                ? TimeUtils.toDate(organization.getCreationDate()) : TimeUtils.getDate());
        organizationObject.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(organization.getModificationDate())
                ? TimeUtils.toDate(organization.getModificationDate()) : TimeUtils.getDate());

        logger.debug("Inserting organization '{}'...", organization.getId());
        organizationCollection.insert(clientSession, organizationObject, null);
        logger.debug("Organization '{}' successfully inserted", organization.getId());

        return organization;
    }

    @Override
    public OpenCGAResult<Organization> get(String user, QueryOptions options) throws CatalogDBException {
        return get(null, user, options);
    }

    @Override
    public OpenCGAResult<Organization> get(QueryOptions options) throws CatalogDBException {
        return get(null, null, options);
    }

    OpenCGAResult<Organization> get(ClientSession clientSession, String user, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        try (DBIterator<Organization> dbIterator = iterator(clientSession, user, options)) {
            return endQuery(startTime, dbIterator);
        }
    }


    @Override
    public OpenCGAResult<Organization> update(String organizationId, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            QueryOptions options = queryOptions != null ? new QueryOptions(queryOptions) : QueryOptions.empty();
            return runTransaction(clientSession -> privateUpdate(clientSession, organizationId, parameters, options));
        } catch (CatalogException e) {
            logger.error("Could not update organization {}: {}", organizationId, e.getMessage(), e);
            throw new CatalogDBException("Could not update organization " + organizationId + ": " + e.getMessage(),
                    e.getCause());
        }
    }

    private OpenCGAResult<Organization> privateUpdate(ClientSession clientSession, String organizationId, ObjectMap parameters,
                                                      QueryOptions queryOptions) throws CatalogParameterException, CatalogDBException {
        long tmpStartTime = startQuery();

        UpdateDocument updateDocument = getValidatedUpdateParams(clientSession, parameters, queryOptions);
        Document organizationUpdate = updateDocument.toFinalUpdateDocument();

        if (organizationUpdate.isEmpty() && CollectionUtils.isEmpty(updateDocument.getNestedUpdateList())) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
                throw new CatalogDBException("Update could not be performed. Some fields could not be processed.");
            }
            throw new CatalogDBException("Nothing to be updated");
        }

        // Update study admins need to be executed before the actual update because we need to fetch the previous owner/admins in case
        // of an update on these fields.
        updateStudyAdmins(clientSession, parameters, queryOptions);

        List<Event> events = new ArrayList<>();
        DataResult<?> updateResult;
        if (!organizationUpdate.isEmpty()) {
            Bson queryBson = Filters.eq(QueryParams.ID.key(), organizationId);
            logger.debug("Update organization. Query: {}, Update: {}", queryBson.toBsonDocument(), organizationUpdate.toBsonDocument());

            updateResult = organizationCollection.update(clientSession, queryBson, organizationUpdate, null);

            if (updateResult.getNumMatches() == 0) {
                throw new CatalogDBException("Organization not found");
            }
            if (updateResult.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, organizationId, "Organization was already updated"));
            }
        }
        if (CollectionUtils.isNotEmpty(updateDocument.getNestedUpdateList())) {
            for (NestedArrayUpdateDocument nestedDocument : updateDocument.getNestedUpdateList()) {
                Bson bsonQuery = new Document(nestedDocument.getQuery());
                logger.debug("Update nested element from Organization. Query: {}, Update: {}", bsonQuery.toBsonDocument(),
                        nestedDocument.getSet());
                DataResult<?> result = organizationCollection.update(clientSession, bsonQuery, nestedDocument.getSet(), null);
                if (result.getNumMatches() == 0) {
                    throw new CatalogDBException("Couldn't update organization. Nothing could be found for query "
                            + bsonQuery.toBsonDocument());
                }
            }
            /*
            for (NestedArrayUpdateDocument nestedDocument : updateDocument.getNestedUpdateList()) {
                            Bson nestedBsonQuery = parseQuery(nestedDocument.getQuery()
                                    .append(QueryParams.UID.key(), interpretation.getUid()));
                            logger.debug("Update nested element from interpretation. Query: {}, Update: {}",
                                    nestedBsonQuery.toBsonDocument(), nestedDocument.getSet());

                            update = interpretationCollection.update(clientSession, nestedBsonQuery, nestedDocument.getSet(), null);

                            if (update.getNumMatches() == 0) {
                                throw CatalogDBException.uidNotFound("Interpretation", interpretationUid);
                            }
                        }
            * */
        }

        logger.debug("Organization {} successfully updated", organizationId);


        return endWrite(tmpStartTime, 1, 1, events);
    }

    private void updateStudyAdmins(ClientSession clientSession, ObjectMap parameters, QueryOptions options) throws CatalogDBException {
        if (!parameters.containsKey(QueryParams.OWNER.key()) && !parameters.containsKey(QueryParams.ADMINS.key())) {
            return;
        }

        Organization organization = get(clientSession, null, OrganizationManager.INCLUDE_ORGANIZATION_ADMINS).first();

        if (parameters.containsKey(QueryParams.OWNER.key())) {
            // Owner has changed
            String newOwner = parameters.getString(QueryParams.OWNER.key());
            // Only do changes if the owner actually changes
            if (!newOwner.equals(organization.getOwner())) {
                if (!organization.getAdmins().contains(organization.getOwner())) {
                    // Remove Owner from all @admins groups
                    logger.info("Removing old owner '{}' from all '{}' groups", organization.getOwner(), ParamConstants.ADMINS_GROUP);
                    dbAdaptorFactory.getCatalogStudyDBAdaptor().removeUsersFromAdminsGroup(clientSession,
                            Collections.singletonList(organization.getOwner()));
                }
                // Add new owner to @admins group
                logger.info("Adding new owner '{}' to all '{}' groups", newOwner, ParamConstants.ADMINS_GROUP);
                dbAdaptorFactory.getCatalogStudyDBAdaptor().addUsersToAdminsAndMembersGroup(clientSession,
                        Collections.singletonList(newOwner));
            }
        }

        if (parameters.containsKey(QueryParams.ADMINS.key())) {
            List<String> admins = parameters.getAsStringList(QueryParams.ADMINS.key());
            if (CollectionUtils.isNotEmpty(admins)) {
                Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
                ParamUtils.AddRemoveAction operation = ParamUtils.AddRemoveAction.from(actionMap, QueryParams.ADMINS.key(),
                        ParamUtils.AddRemoveAction.ADD);

                switch (operation) {
                    case ADD:
                        // Add new admins to @admins group
                        logger.info("Adding new admins '{}' to all '{}' groups", admins, ParamConstants.ADMINS_GROUP);
                        dbAdaptorFactory.getCatalogStudyDBAdaptor().addUsersToAdminsAndMembersGroup(clientSession, admins);
                        break;
                    case REMOVE:
                        // Fetch current organization owner
                        String newOwner = parameters.getString(QueryParams.OWNER.key());
                        String owner = StringUtils.isNotEmpty(newOwner) ? newOwner : organization.getOwner();

                        // Remove organization owner in case is one of the removed admins
                        admins = admins.stream().filter(a -> !owner.equals(a)).collect(Collectors.toList());
                        logger.info("Removing old admins '{}' from all '{}' groups", admins, ParamConstants.ADMINS_GROUP);
                        dbAdaptorFactory.getCatalogStudyDBAdaptor().removeUsersFromAdminsGroup(clientSession, admins);
                        break;
                    default:
                        throw new CatalogDBException("Unexpected " + QueryParams.ADMINS.key() + " action");
                }
            }
        }
    }

    private UpdateDocument getValidatedUpdateParams(ClientSession clientSession, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogParameterException, CatalogDBException {
        checkUpdatedParams(parameters, Arrays.asList(QueryParams.NAME.key(), QueryParams.OWNER.key(),
                QueryParams.CREATION_DATE.key(), QueryParams.MODIFICATION_DATE.key(), QueryParams.ADMINS.key(),
                QueryParams.CONFIGURATION_OPTIMIZATIONS.key(), QueryParams.CONFIGURATION_TOKEN.key(),
                QueryParams.CONFIGURATION_AUTHENTICATION_ORIGINS.key(), QueryParams.FEDERATION_CLIENTS.key(),
                QueryParams.FEDERATION_SERVERS.key(), QueryParams.CONFIGURATION.key(), QueryParams.ATTRIBUTES.key()));

        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = { QueryParams.NAME.key() };
        filterStringParams(parameters, document.getSet(), acceptedParams);

        String[] acceptedObjectParams = { QueryParams.CONFIGURATION_OPTIMIZATIONS.key(), QueryParams.CONFIGURATION_TOKEN.key() };
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        // Authentication Origins action
        if (parameters.containsKey(QueryParams.CONFIGURATION_AUTHENTICATION_ORIGINS.key())) {
            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            ParamUtils.UpdateAction operation = ParamUtils.UpdateAction.from(actionMap, OrganizationDBAdaptor.AUTH_ORIGINS_FIELD);
            String[] authOriginsParams = {QueryParams.CONFIGURATION_AUTHENTICATION_ORIGINS.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), authOriginsParams);
                    break;
                case REMOVE:
                    fixAuthOriginsForRemoval(parameters);
                    filterObjectParams(parameters, document.getPull(), authOriginsParams);
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), authOriginsParams);
                    break;
                case REPLACE:
                    filterReplaceParams(parameters.getAsList(QueryParams.CONFIGURATION_AUTHENTICATION_ORIGINS.key(), Map.class), document,
                            m -> String.valueOf(m.get("id")), QueryParams.CONFIGURATION_AUTHENTICATION_ORIGINS_ID.key());
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        // FederationClient action
        if (parameters.containsKey(QueryParams.FEDERATION_CLIENTS.key())) {
            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            ParamUtils.AddRemoveAction operation = ParamUtils.AddRemoveAction.from(actionMap, QueryParams.FEDERATION_CLIENTS.key());
            String[] fedClients = {QueryParams.FEDERATION_CLIENTS.key()};
            switch (operation) {
                case REMOVE:
                    fixFederationClientForRemoval(parameters);
                    filterObjectParams(parameters, document.getPull(), fedClients);
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), fedClients);
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        // FederationServer action
        if (parameters.containsKey(QueryParams.FEDERATION_SERVERS.key())) {
            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            ParamUtils.AddRemoveAction operation = ParamUtils.AddRemoveAction.from(actionMap, QueryParams.FEDERATION_SERVERS.key());
            String[] fedClients = {QueryParams.FEDERATION_SERVERS.key()};
            switch (operation) {
                case REMOVE:
                    fixFederationServerForRemoval(parameters);
                    filterObjectParams(parameters, document.getPull(), fedClients);
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), fedClients);
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        String owner = parameters.getString(QueryParams.OWNER.key(), null);
        if (StringUtils.isNotEmpty(owner)) {
            // Check user exists
            OpenCGAResult<Long> count = dbAdaptorFactory.getCatalogUserDBAdaptor().count(clientSession,
                    new Query(UserDBAdaptor.QueryParams.ID.key(), owner));
            if (count.getNumMatches() == 0) {
                throw new CatalogDBException("Could not update owner. User not found.");
            }
            // Fetch current owner
            Organization organization = get(clientSession, null, OrganizationManager.INCLUDE_ORGANIZATION_ADMINS).first();
            if (owner.equals(organization.getOwner())) {
                logger.warn("Organization owner is already '{}'.", owner);
            } else {
                document.getSet().put(QueryParams.OWNER.key(), owner);
            }
        }

        if (StringUtils.isNotEmpty(parameters.getString(QueryParams.CREATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.CREATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.CREATION_DATE.key(), time);
            document.getSet().put(PRIVATE_CREATION_DATE, date);
        }
        if (StringUtils.isNotEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.MODIFICATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        if (parameters.containsKey(QueryParams.ADMINS.key())) {
            List<String> adminList = parameters.getAsStringList(QueryParams.ADMINS.key()).stream().distinct().collect(Collectors.toList());

            // Check users exist
            OpenCGAResult<Long> count = dbAdaptorFactory.getCatalogUserDBAdaptor().count(clientSession,
                    new Query(UserDBAdaptor.QueryParams.ID.key(), adminList));
            if (count.getNumMatches() < adminList.size()) {
                throw new CatalogDBException("Could not update admins. Some users were not found.");
            }

            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            ParamUtils.AddRemoveAction operation = ParamUtils.AddRemoveAction.from(actionMap, QueryParams.ADMINS.key(),
                    ParamUtils.AddRemoveAction.ADD);
            if (!adminList.isEmpty()) {
                switch (operation) {
                    case REMOVE:
                        document.getPullAll().put(QueryParams.ADMINS.key(), adminList);
                        break;
                    case ADD:
                        document.getAddToSet().put(QueryParams.ADMINS.key(), adminList);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown operation " + operation);
                }
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), };
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    private void fixFederationClientForRemoval(ObjectMap parameters) {
        if (parameters.get(QueryParams.FEDERATION_CLIENTS.key()) == null) {
            return;
        }
        List<Document> federationParamList = new LinkedList<>();
        for (Object federationClient : parameters.getAsList(QueryParams.FEDERATION_CLIENTS.key())) {
            if (federationClient instanceof FederationServer) {
                federationParamList.add(new Document("id", ((FederationServer) federationClient).getId()));
            } else {
                federationParamList.add(new Document("id", ((Map) federationClient).get("id")));
            }
        }
        parameters.putNested(QueryParams.FEDERATION_CLIENTS.key(), federationParamList, false);
    }

    private void fixFederationServerForRemoval(ObjectMap parameters) {
        if (parameters.get(QueryParams.FEDERATION_SERVERS.key()) == null) {
            return;
        }
        List<Document> federationParamList = new LinkedList<>();
        for (Object federationServer : parameters.getAsList(QueryParams.FEDERATION_SERVERS.key())) {
            if (federationServer instanceof FederationClient) {
                federationParamList.add(new Document("id", ((FederationClient) federationServer).getId()));
            } else {
                federationParamList.add(new Document("id", ((Map) federationServer).get("id")));
            }
        }
        parameters.putNested(QueryParams.FEDERATION_SERVERS.key(), federationParamList, false);
    }

    private void fixAuthOriginsForRemoval(ObjectMap parameters) {
        if (parameters.get(QueryParams.CONFIGURATION_AUTHENTICATION_ORIGINS.key()) == null) {
            return;
        }
        List<Document> authOriginParamList = new LinkedList<>();
        for (Object authOrigin : parameters.getAsList(QueryParams.CONFIGURATION_AUTHENTICATION_ORIGINS.key())) {
            if (authOrigin instanceof AuthenticationOrigin) {
                authOriginParamList.add(new Document("id", ((AuthenticationOrigin) authOrigin).getId()));
            } else {
                authOriginParamList.add(new Document("id", ((Map) authOrigin).get("id")));
            }
        }
        parameters.putNested(QueryParams.CONFIGURATION_AUTHENTICATION_ORIGINS.key(), authOriginParamList, false);
    }

    @Override
    public OpenCGAResult<Organization> delete(Organization organization) throws CatalogDBException {
        return null;
    }

    @Override
    public DBIterator<Organization> iterator(QueryOptions options) throws CatalogDBException {
        return iterator(null, null, options);
    }

    public DBIterator<Organization> iterator(ClientSession clientSession, String user, QueryOptions options) throws CatalogDBException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, options);
        return new OrganizationCatalogMongoDBIterator<>(mongoCursor, clientSession, organizationConverter, dbAdaptorFactory, options, user);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, QueryOptions options) {
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterQueryOptionsToIncludeKeys(qOptions,
                OrganizationManager.INCLUDE_ORGANIZATION_IDS.getAsStringList(QueryOptions.INCLUDE));

        return organizationCollection.iterator(clientSession, new Document(), null, null, qOptions);
    }

    public List<String> getOwnerAndAdmins(ClientSession clientSession) throws CatalogDBException {
        Organization organization = get(clientSession, null, OrganizationManager.INCLUDE_ORGANIZATION_ADMINS).first();
        List<String> members = new ArrayList<>(organization.getAdmins().size() + 1);
        if (StringUtils.isNotEmpty(organization.getOwner())) {
            members.add(organization.getOwner());
        } else {
            logger.warn("No owner found for organization {}", organization.getId());
        }
        members.addAll(organization.getAdmins());
        return members;
    }

    public boolean isOwnerOrAdmin(ClientSession clientSession, String userId) throws CatalogDBException {
        return getOwnerAndAdmins(clientSession).contains(userId);
    }

    public long getNewAutoIncrementId() {
        return getNewAutoIncrementId(null, ID_COUNTER); //, metaCollection
    }

    public long getNewAutoIncrementId(ClientSession clientSession) {
        return getNewAutoIncrementId(clientSession, ID_COUNTER); //, metaCollection
    }

    public long getNewAutoIncrementId(ClientSession clientSession, String field) { //, MongoDBCollection metaCollection
        Bson query = new Document();
        Document projection = new Document(field, true);
        Bson inc = Updates.inc(field, 1L);
        QueryOptions queryOptions = new QueryOptions("returnNew", true);
        DataResult<Document> result = organizationCollection.findAndUpdate(clientSession, query, projection, null, inc, queryOptions);
        return result.getResults().get(0).getLong(field);
    }

//    private Bson parseQuery(Query query) throws CatalogDBException {
//        List<Bson> andBsonList = new ArrayList<>();
//
//        Query queryCopy = new Query(query);
//        queryCopy.remove(ParamConstants.DELETED_PARAM);
//
//        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
//            String key = entry.getKey().split("\\.")[0];
//            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
//                    : QueryParams.getParam(key);
//            if (queryParam == null) {
//                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
//                        + "queried for.");
//            }
//            try {
//                switch (queryParam) {
//                    case UID:
//                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
//                        break;
//                    case CREATION_DATE:
//                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
//                        break;
//                    case MODIFICATION_DATE:
//                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
//                        break;
//                    case ID:
//                    case UUID:
//                    case NAME:
//                    case DOMAIN:
//                    case OWNER:
//                    case ADMINS:
//                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
//                        break;
//                    default:
//                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
//                }
//            } catch (Exception e) {
//                if (e instanceof CatalogDBException) {
//                    throw e;
//                } else {
//                    throw new CatalogDBException("Error parsing query : " + queryCopy.toJson(), e);
//                }
//            }
//        }
//
//        if (andBsonList.size() > 0) {
//            return Filters.and(andBsonList);
//        } else {
//            return new Document();
//        }
//    }
}
