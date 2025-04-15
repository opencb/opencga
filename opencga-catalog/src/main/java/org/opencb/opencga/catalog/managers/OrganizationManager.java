package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.jsonwebtoken.security.InvalidKeyException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.auth.authentication.azure.AuthenticationFactory;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JwtUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.Optimizations;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.federation.Federation;
import org.opencb.opencga.core.models.organizations.*;
import org.opencb.opencga.core.models.user.OrganizationUserUpdateParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class OrganizationManager extends AbstractManager {

    public static final QueryOptions INCLUDE_ORGANIZATION_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            OrganizationDBAdaptor.QueryParams.ID.key(), OrganizationDBAdaptor.QueryParams.UID.key(),
            OrganizationDBAdaptor.QueryParams.UUID.key()));
    public static final QueryOptions INCLUDE_ORGANIZATION_ADMINS = keepFieldsInQueryOptions(INCLUDE_ORGANIZATION_IDS,
            Arrays.asList(OrganizationDBAdaptor.QueryParams.OWNER.key(), OrganizationDBAdaptor.QueryParams.ADMINS.key()));
    public static final QueryOptions INCLUDE_ORGANIZATION_CONFIGURATION = keepFieldsInQueryOptions(INCLUDE_ORGANIZATION_ADMINS,
            Collections.singletonList(OrganizationDBAdaptor.QueryParams.CONFIGURATION.key()));
    protected static Logger logger = LoggerFactory.getLogger(OrganizationManager.class);
    private final CatalogIOManager catalogIOManager;
    private final AuthenticationFactory authenticationFactory;

    OrganizationManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                        DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManager catalogIOManager,
                        AuthenticationFactory authenticationFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.catalogIOManager = catalogIOManager;
        this.authenticationFactory = authenticationFactory;
    }

//    OpenCGAResult<Organization> internalGet(String organizationId, QueryOptions options, String user) throws CatalogException {
//        return internalGet(Collections.singletonList(organizationId), null, options, user, false);
//    }
//
//    OpenCGAResult<Organization> internalGet(List<String> organizationList, @Nullable Query query, QueryOptions options, String user,
//                                            boolean ignoreException) throws CatalogException {
//        if (CollectionUtils.isEmpty(organizationList)) {
//            throw new CatalogException("Missing organization entries.");
//        }
//        List<String> uniqueList = ListUtils.unique(organizationList);
//
//        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));
//
//        Query queryCopy = query == null ? new Query() : new Query(query);
//
//        OrganizationDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
//        queryCopy.put(idQueryParam.key(), uniqueList);
//
//        if (!authorizationManager.isInstallationAdministrator(user)) {
//            // Only admins and owner are allowed to see the organizations
//            queryCopy.put(OrganizationDBAdaptor.QueryParams.ADMINS.key(), user);
//        }
//
//        // Ensure the field by which we are querying for will be kept in the results
//        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());
//
//        OpenCGAResult<Organization> organizationDataResult = getOrganizationDBAdaptor(organization).get(queryCopy, queryOptions);
//
//        Function<Organization, String> organizationStringFunction = Organization::getId;
//        if (idQueryParam.equals(OrganizationDBAdaptor.QueryParams.UUID)) {
//            organizationStringFunction = Organization::getUuid;
//        }
//
//        if (ignoreException || organizationDataResult.getNumResults() == uniqueList.size()) {
//            return organizationDataResult;
//        }
//
//        List<String> missingOrganizations = new ArrayList<>(organizationList.size());
//        for (Organization organization : organizationDataResult.getResults()) {
//            if (!uniqueList.contains(organizationStringFunction.apply(organization))) {
//                missingOrganizations.add(organizationStringFunction.apply(organization));
//            }
//        }
//
//        throw CatalogException.notFound("organizations", missingOrganizations);
//    }

//    OrganizationDBAdaptor.QueryParams getFieldFilter(List<String> idList) throws CatalogException {
//        OrganizationDBAdaptor.QueryParams idQueryParam = null;
//        for (String entry : idList) {
//            OrganizationDBAdaptor.QueryParams param = OrganizationDBAdaptor.QueryParams.ID;
//            if (UuidUtils.isOpenCgaUuid(entry)) {
//                param = OrganizationDBAdaptor.QueryParams.UUID;
//            }
//            if (idQueryParam == null) {
//                idQueryParam = param;
//            }
//            if (idQueryParam != param) {
//                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
//            }
//        }
//        return idQueryParam;
//    }

    public OpenCGAResult<Organization> get(String organizationId, QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String userId = tokenPayload.getUserId(organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Organization> queryResult;
        try {
            authorizationManager.checkCanViewOrganization(organizationId, userId);
            ParamUtils.checkParameter(organizationId, "organization id");
            QueryOptions queryOptions = ParamUtils.defaultObject(options, QueryOptions::new);
            boolean isOrgAdmin = authorizationManager.isAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
            queryOptions.put(OrganizationDBAdaptor.IS_ORGANIZATION_ADMIN_OPTION, isOrgAdmin);
            queryResult = getOrganizationDBAdaptor(organizationId).get(userId, queryOptions);
            privatizeResults(queryResult);
        } catch (CatalogException e) {
            auditManager.auditInfo(organizationId, userId, Enums.Resource.ORGANIZATION, organizationId, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
        auditManager.auditInfo(organizationId, userId, Enums.Resource.ORGANIZATION, organizationId, "", "", "", auditParams,
                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        return queryResult;
    }

    public OpenCGAResult<Organization> create(OrganizationCreateParams organizationCreateParams, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("organizationCreateParams", organizationCreateParams)
                .append("options", options)
                .append("token", token);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        OpenCGAResult<Organization> queryResult;
        Organization organization;
        String userId = null;
        try {
            // The first time we create the ADMIN_ORGANIZATION as there are no users yet, we should not check anything
            if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationCreateParams.getId())) {
                JwtPayload jwtPayload = this.catalogManager.getUserManager().validateToken(token);
                userId = jwtPayload.getUserId(organizationCreateParams.getId());

                if (!ParamConstants.ADMIN_ORGANIZATION.equals(jwtPayload.getOrganization())) {
                    throw CatalogAuthorizationException.opencgaAdminOnlySupportedOperation();
                }
            }

            ParamUtils.checkObj(organizationCreateParams, "organizationCreateParams");

            organization = organizationCreateParams.toOrganization();
            validateOrganizationForCreation(organization, userId);

            queryResult = getCatalogDBAdaptorFactory().createOrganization(organization, options, userId);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                OpenCGAResult<Organization> result = getOrganizationDBAdaptor(organization.getId()).get(options);
                organization = result.first();
                // Fetch created organization
                queryResult.setResults(result.getResults());
            }
            // Add required authentication manager for the new organization
            authenticationFactory.configureOrganizationAuthenticationManager(organization);

            privatizeResults(queryResult);
        } catch (CatalogException e) {
            if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationCreateParams.getId())) {
                auditManager.auditCreate(ParamConstants.ADMIN_ORGANIZATION, userId, Enums.Resource.ORGANIZATION,
                        organizationCreateParams.getId(), "", "", "", auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
            throw e;
        }

        try {
            catalogIOManager.createOrganization(organization.getId());
        } catch (CatalogIOException e) {
            auditManager.auditCreate(ParamConstants.ADMIN_ORGANIZATION, userId, Enums.Resource.ORGANIZATION, organization.getId(), "", "",
                    "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            try {
                getCatalogDBAdaptorFactory().deleteOrganization(organization);
            } catch (Exception e1) {
                logger.error("Error deleting organization from catalog after failing creating the folder in the filesystem", e1);
                throw e;
            }
            throw e;
        }

        if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationCreateParams.getId())) {
            // Skip old available migrations
            catalogManager.getMigrationManager().skipPendingMigrations(organizationCreateParams.getId(), token);
        }

        auditManager.auditCreate(ParamConstants.ADMIN_ORGANIZATION, userId, Enums.Resource.ORGANIZATION, organization.getId(),
                organization.getUuid(), "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

        return queryResult;
    }

    public OpenCGAResult<Organization> update(String organizationId, OrganizationUpdateParams updateParams, QueryOptions options,
                                              String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String userId = tokenPayload.getUserId(organizationId);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse OrganizationUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("updateParams", updateMap)
                .append("options", options)
                .append("token", token);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        OpenCGAResult<Organization> result = OpenCGAResult.empty(Organization.class);
        try {
            ParamUtils.checkObj(updateParams, "OrganizationUpdateParams");
            if (StringUtils.isNotEmpty(updateParams.getOwner()) || CollectionUtils.isNotEmpty(updateParams.getAdmins())) {
                authorizationManager.checkIsAtLeastOrganizationOwner(organizationId, userId);
                checkIsNotAFederatedUser(organizationId, updateParams);
            } else {
                authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
            }

            OpenCGAResult<Organization> internalResult = get(organizationId, INCLUDE_ORGANIZATION_ADMINS, token);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Organization '" + organizationId + "' not found");
            }
            Organization organization = internalResult.first();

            // We set the proper values for the audit
            organizationId = organization.getId();

            result = getOrganizationDBAdaptor(organizationId).update(organizationId, updateMap, options);

            auditManager.auditUpdate(organizationId, userId, Enums.Resource.ORGANIZATION, organization.getId(), organization.getUuid(), "",
                    "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated organization
                OpenCGAResult<Organization> queryResult = getOrganizationDBAdaptor(organizationId).get(options);
                result.setResults(queryResult.getResults());
            }
            privatizeResults(result);
        } catch (Exception e) {
            Event event = new Event(Event.Type.ERROR, organizationId, e.getMessage());
            result.getEvents().add(event);
            result.setNumErrors(result.getNumErrors() + 1);

            logger.error("Cannot update organization {}: {}", organizationId, e.getMessage());
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.ORGANIZATION, organizationId, organizationId, "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Update organization",
                            e.getMessage())));
            throw e;
        }
        return result;
    }

    private void checkIsNotAFederatedUser(String organizationId, OrganizationUpdateParams updateParams) throws CatalogException {
        Set<String> users = new HashSet<>();
        if (StringUtils.isNotEmpty(updateParams.getOwner())) {
            users.add(updateParams.getOwner());
        }
        if (CollectionUtils.isNotEmpty(updateParams.getAdmins())) {
            users.addAll(updateParams.getAdmins());
        }
        if (CollectionUtils.isNotEmpty(users)) {
            try {
                checkIsNotAFederatedUser(organizationId, new ArrayList<>(users));
            } catch (CatalogException e) {
                throw new CatalogException("Cannot set a federated user as owner or admin of an organization.", e);
            }
        }
    }

    public OpenCGAResult<User> updateUser(@Nullable String organizationId, String userId, OrganizationUserUpdateParams updateParams,
                                          QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("userId", userId)
                .append("updateParams", updateParams)
                .append("options", options)
                .append("token", token);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String myOrganizationId = StringUtils.isNotEmpty(organizationId) ? organizationId : tokenPayload.getOrganization();
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(myOrganizationId, tokenPayload.getUserId(myOrganizationId));
            ParamUtils.checkObj(updateParams, "OrganizationUserUpdateParams");
            getUserDBAdaptor(myOrganizationId).checkId(userId);

            if (StringUtils.isNotEmpty(updateParams.getEmail())) {
                ParamUtils.checkEmail(updateParams.getEmail());
            }
            if (updateParams.getQuota() != null) {
                if (updateParams.getQuota().getDiskUsage() < 0) {
                    throw new CatalogException("Disk usage cannot be negative");
                }
                if (updateParams.getQuota().getCpuUsage() < 0) {
                    throw new CatalogException("CPU usage cannot be negative");
                }
                if (updateParams.getQuota().getMaxDisk() < 0) {
                    throw new CatalogException("Max disk cannot be negative");
                }
                if (updateParams.getQuota().getMaxCpu() < 0) {
                    throw new CatalogException("Max CPU cannot be negative");
                }
            }

            ObjectMap updateMap;
            try {
                updateMap = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse OrganizationUserUpdateParams object: " + e.getMessage(), e);
            }

            if (updateParams.getInternal() != null && updateParams.getInternal().getAccount() != null
                    && StringUtils.isNotEmpty(updateParams.getInternal().getAccount().getExpirationDate())) {
                ParamUtils.checkDateIsNotExpired(updateParams.getInternal().getAccount().getExpirationDate(), "expirationDate");
            }

            OpenCGAResult<User> updateResult = getUserDBAdaptor(myOrganizationId).update(userId, updateMap);
            auditManager.auditUpdate(myOrganizationId, tokenPayload.getUserId(myOrganizationId), Enums.Resource.USER, userId, "", "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated user
                OpenCGAResult<User> result = getUserDBAdaptor(myOrganizationId).get(userId, options);
                updateResult.setResults(result.getResults());
            }

            return updateResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(myOrganizationId, tokenPayload.getUserId(myOrganizationId), Enums.Resource.USER, userId, "", "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<OrganizationConfiguration> updateConfiguration(String organizationId, OrganizationConfiguration updateParams,
                                                                        QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String userId = tokenPayload.getUserId(organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("updateParams", updateParams)
                .append("options", options)
                .append("token", token);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        OpenCGAResult<OrganizationConfiguration> result = OpenCGAResult.empty(OrganizationConfiguration.class);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            ParamUtils.checkObj(updateParams, "OrganizationConfiguration");
            ObjectMap updateConfigurationMap;
            try {
                updateConfigurationMap = new ObjectMap(getUpdateObjectMapper().writeValueAsString(updateParams));
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse OrganizationConfiguration object: " + e.getMessage(), e);
            }

            OpenCGAResult<Organization> internalResult = get(organizationId, INCLUDE_ORGANIZATION_CONFIGURATION, token);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Organization '" + organizationId + "' not found");
            }
            Organization organization = internalResult.first();

            // We set the proper values for the audit
            organizationId = organization.getId();

            if (CollectionUtils.isNotEmpty(updateParams.getAuthenticationOrigins())) {
                // Check action
                ParamUtils.UpdateAction authOriginsAction = null;
                Map<String, Object> map = queryOptions.getMap(Constants.ACTIONS);
                if (map == null || map.get(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD) == null) {
                    // Write default option
                    authOriginsAction = ParamUtils.UpdateAction.ADD;
                    Map<String, Object> actionMap = new HashMap<>();
                    actionMap.put(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD, authOriginsAction);
                    queryOptions.put(Constants.ACTIONS, actionMap);
                } else {
                    authOriginsAction = ParamUtils.UpdateAction.valueOf(map.get(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD).toString());
                }

                Set<String> currentAuthOriginIds = organization.getConfiguration().getAuthenticationOrigins()
                        .stream()
                        .map(AuthenticationOrigin::getId)
                        .collect(Collectors.toSet());

                Set<String> updateAuthOriginIds = new HashSet<>();
                StringBuilder authOriginUpdateBuilder = new StringBuilder();
                List<AuthenticationOrigin> authenticationOrigins = updateParams.getAuthenticationOrigins();
                for (int i = 0; i < authenticationOrigins.size(); i++) {
                    AuthenticationOrigin authenticationOrigin = authenticationOrigins.get(i);
                    ParamUtils.checkParameter(authenticationOrigin.getId(), "AuthenticationOrigin id");
                    ParamUtils.checkObj(authenticationOrigin.getType(), "AuthenticationOrigin type");
                    if (updateAuthOriginIds.contains(authenticationOrigin.getId())) {
                        throw new CatalogParameterException("Found duplicated authentication origin id '" + authenticationOrigin.getId()
                                + "'.");
                    }
                    // Check authOrigin OPENCGA-OPENCGA
                    if ((authenticationOrigin.getType().equals(AuthenticationOrigin.AuthenticationType.OPENCGA)
                            && !CatalogAuthenticationManager.OPENCGA.equals(authenticationOrigin.getId()))
                            || (!authenticationOrigin.getType().equals(AuthenticationOrigin.AuthenticationType.OPENCGA)
                            && CatalogAuthenticationManager.OPENCGA.equals(authenticationOrigin.getId()))) {
                        throw new CatalogParameterException("AuthenticationOrigin type '" + AuthenticationOrigin.AuthenticationType.OPENCGA
                                + "' must go together with id '" + CatalogAuthenticationManager.OPENCGA + "'.");
                    }
                    updateAuthOriginIds.add(authenticationOrigin.getId());
                    if (i > 0) {
                        authOriginUpdateBuilder.append(", ");
                    }
                    authOriginUpdateBuilder.append(authenticationOrigin.getType()).append(": ").append(authenticationOrigin.getId());

                    if (authOriginsAction != ParamUtils.UpdateAction.REMOVE) {
                        // Validate configuration is correct and can be used
                        authenticationFactory.validateAuthenticationOrigin(authenticationOrigin);
                    }
                }

                switch (authOriginsAction) {
                    case ADD:
                        for (AuthenticationOrigin authenticationOrigin : updateParams.getAuthenticationOrigins()) {
                            if (currentAuthOriginIds.contains(authenticationOrigin.getId())) {
                                throw new CatalogException("Authentication origin '" + authenticationOrigin.getId() + "' already exists. "
                                        + "Please, set the authOriginsAction to 'REPLACE' to replace the current configuration.");
                            }
                        }
                        logger.debug("Adding new list of Authentication Origins: {}.", authOriginUpdateBuilder);
                        break;
                    case SET:
                        boolean userAuthOriginFound = false;
                        for (AuthenticationOrigin authenticationOrigin : updateParams.getAuthenticationOrigins()) {
                            if (tokenPayload.getAuthOrigin().equals(authenticationOrigin.getType())) {
                                userAuthOriginFound = true;
                            }
                        }
                        if (!userAuthOriginFound) {
                            throw new CatalogException("User authentication origin not found in the list of authentication origins. "
                                    + "Please, add an AuthenticationOrigin of type '" + tokenPayload.getAuthOrigin() + "' to the list.");
                        }
                        logger.debug("Set new list of Authentication Origins: {}.", authOriginUpdateBuilder);
                        break;
                    case REMOVE:
                        for (AuthenticationOrigin authenticationOrigin : updateParams.getAuthenticationOrigins()) {
                            if (!currentAuthOriginIds.contains(authenticationOrigin.getId())) {
                                throw new CatalogException("Authentication origin '" + authenticationOrigin.getId() + "' does not exist. "
                                        + "The current available authentication origin ids are: '"
                                        + StringUtils.join(currentAuthOriginIds, ", ") + "'.");
                            }
                            if (tokenPayload.getAuthOrigin().equals(authenticationOrigin.getType())) {
                                throw new CatalogException("Removing the authentication origin '" + tokenPayload.getAuthOrigin() + "' "
                                        + "not allowed. Your user account uses that AuthenticationOrigin.");
                            }
                        }
                        logger.debug("Removing list of Authentication Origins: {}.", authOriginUpdateBuilder);
                        break;
                    case REPLACE:
                        for (AuthenticationOrigin authenticationOrigin : updateParams.getAuthenticationOrigins()) {
                            if (!currentAuthOriginIds.contains(authenticationOrigin.getId())) {
                                throw new CatalogException("Authentication origin '" + authenticationOrigin.getId() + "' not found."
                                        + "Please, set the authOriginsAction to 'ADD' to add the new AuthenticationOrigin.");
                            }
                        }
                        logger.debug("Replace list of Authentication Origins: {}.", authOriginUpdateBuilder);
                        break;
                    default:
                        throw new CatalogParameterException("Unknown authentication origins action " + authOriginsAction);
                }
            }

            if (updateParams.getToken() != null) {
                try {
                    JwtUtils.validateJWTKey(updateParams.getToken().getAlgorithm(), updateParams.getToken().getSecretKey());
                } catch (InvalidKeyException e) {
                    throw new CatalogParameterException("Invalid secret key - algorithm for JWT token: " + e.getMessage(), e);
                }
                if (updateParams.getToken().getExpiration() <= 0) {
                    throw new CatalogParameterException("Invalid expiration for JWT token. It must be a positive number.");
                }
            }

            ObjectMap updateMap = new ObjectMap(OrganizationDBAdaptor.QueryParams.CONFIGURATION.key(), updateConfigurationMap);
            OpenCGAResult<?> update = getOrganizationDBAdaptor(organizationId).update(organizationId, updateMap, queryOptions);
            result.append(update);
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.ORGANIZATION, organization.getId(), organization.getUuid(), "",
                    "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            organization = null;
            if (queryOptions.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated organization
                organization = getOrganizationDBAdaptor(organizationId).get(INCLUDE_ORGANIZATION_CONFIGURATION).first();
                result.setResults(Collections.singletonList(organization.getConfiguration()));
            }

            if (CollectionUtils.isNotEmpty(updateParams.getAuthenticationOrigins()) || updateParams.getToken() != null) {
                if (organization == null) {
                    organization = getOrganizationDBAdaptor(organizationId).get(INCLUDE_ORGANIZATION_CONFIGURATION).first();
                }
                authenticationFactory.configureOrganizationAuthenticationManager(organization);
            }

        } catch (Exception e) {
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.ORGANIZATION, organizationId, organizationId, "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Update organization",
                            e.getMessage())));
            throw e;
        }
        return result;
    }

    private void validateOrganizationForCreation(Organization organization, String userId) throws CatalogParameterException {
        ParamUtils.checkParameter(organization.getId(), OrganizationDBAdaptor.QueryParams.ID.key());

        organization.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.ORGANIZATION));
        organization.setName(ParamUtils.defaultString(organization.getName(), organization.getId()));
        organization.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(organization.getCreationDate(),
                OrganizationDBAdaptor.QueryParams.CREATION_DATE.key()));
        organization.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(organization.getModificationDate(),
                OrganizationDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
        organization.setInternal(new OrganizationInternal(new InternalStatus(), TimeUtils.getTime(), TimeUtils.getTime(),
                GitRepositoryState.getInstance().getBuildVersion(), Collections.emptyList()));
        organization.setOwner(userId);
        organization.setAdmins(Collections.emptyList());
        organization.setProjects(Collections.emptyList());
        organization.setFederation(new Federation(Collections.emptyList(), Collections.emptyList()));

        if (organization.getConfiguration() == null) {
            organization.setConfiguration(new OrganizationConfiguration());
        }
        validateOrganizationConfiguration(organization);

        organization.setAttributes(ParamUtils.defaultObject(organization.getAttributes(), HashMap::new));
    }

    private void validateOrganizationConfiguration(Organization organization) throws CatalogParameterException {
        if (CollectionUtils.isNotEmpty(organization.getConfiguration().getAuthenticationOrigins())) {
            for (AuthenticationOrigin authenticationOrigin : organization.getConfiguration().getAuthenticationOrigins()) {
                ParamUtils.checkParameter(authenticationOrigin.getId(), "AuthenticationOrigin id");
            }
        } else {
            organization.getConfiguration()
                    .setAuthenticationOrigins(Collections.singletonList(
                            CatalogAuthenticationManager.createOpencgaAuthenticationOrigin()));
        }
        if (organization.getConfiguration().getToken() == null
                || StringUtils.isEmpty(organization.getConfiguration().getToken().getSecretKey())) {
            organization.getConfiguration().setToken(TokenConfiguration.init());
        }
        organization.getConfiguration().setDefaultUserExpirationDate(ParamUtils.defaultString(
                organization.getConfiguration().getDefaultUserExpirationDate(), Constants.DEFAULT_USER_EXPIRATION_DATE));
        if (organization.getConfiguration().getOptimizations() == null) {
            organization.getConfiguration().setOptimizations(new Optimizations(false));
        }
    }

    Set<String> getOrganizationOwnerAndAdmins(String organizationId) throws CatalogException {
        OpenCGAResult<Organization> result = getOrganizationDBAdaptor(organizationId).get(INCLUDE_ORGANIZATION_ADMINS);
        if (result.getNumResults() == 0) {
            throw new CatalogException("Could not get owner and admins of organization '" + organizationId + "'. Organization not found.");
        }
        Organization organization = result.first();
        Set<String> users = new HashSet<>();
        if (StringUtils.isNotEmpty(organization.getOwner())) {
            users.add(organization.getOwner());
        }
        if (CollectionUtils.isNotEmpty(organization.getAdmins())) {
            users.addAll(organization.getAdmins());
        }
        return users;
    }

    public List<String> getOrganizationIds(String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        authorizationManager.checkIsOpencgaAdministrator(tokenPayload, "get all organization ids");
        return getCatalogDBAdaptorFactory().getOrganizationIds();
    }

    private void privatizeResults(OpenCGAResult<Organization> result) {
        if (CollectionUtils.isNotEmpty(result.getResults())) {
            for (Organization organization : result.getResults()) {
                if (organization.getConfiguration() != null) {
                    organization.getConfiguration().setToken(null);
                    if (CollectionUtils.isNotEmpty(organization.getConfiguration().getAuthenticationOrigins())) {
                        for (AuthenticationOrigin authenticationOrigin : organization.getConfiguration().getAuthenticationOrigins()) {
                            authenticationOrigin.setOptions(null);
                        }
                    }
                }
            }
        }
    }

    @Override
    Enums.Resource getResource() {
        return Enums.Resource.ORGANIZATION;
    }
}
