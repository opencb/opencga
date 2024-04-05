package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.organizations.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
            QueryOptions queryOptions = ParamUtils.defaultObject(options, QueryOptions::new);
            boolean isOrgAdmin = authorizationManager.isAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
            queryOptions.put(OrganizationDBAdaptor.IS_ORGANIZATION_ADMIN_OPTION, isOrgAdmin);
            queryResult = getOrganizationDBAdaptor(organizationId).get(queryOptions);
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

            queryResult = catalogDBAdaptorFactory.createOrganization(organization, options, userId);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                OpenCGAResult<Organization> result = getOrganizationDBAdaptor(organization.getId()).get(options);
                organization = result.first();
                // Fetch created organization
                queryResult.setResults(result.getResults());
            }

            // Add required authentication manager for the new organization
            authenticationFactory.configureOrganizationAuthenticationManager(organization);
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
                catalogDBAdaptorFactory.deleteOrganization(organization);
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

            // Avoid duplicated authentication origin ids.
            // Validate all mandatory authentication origin fields are filled in.
            if (updateParams.getConfiguration() != null && updateParams.getConfiguration().getAuthenticationOrigins() != null) {
                String authOriginsPrefixKey = OrganizationDBAdaptor.QueryParams.CONFIGURATION_AUTHENTICATION_ORIGINS.key();
                boolean internal = false;
                Set<String> authenticationOriginIds = new HashSet<>();
                for (AuthenticationOrigin authenticationOrigin : updateParams.getConfiguration().getAuthenticationOrigins()) {
                    if (authenticationOrigin.getType().equals(AuthenticationOrigin.AuthenticationType.OPENCGA)) {
                        if (internal) {
                            throw new CatalogException("Found duplicated authentication origin of type OPENCGA.");
                        }
                        internal = true;
                        // Set id to INTERNAL
                        authenticationOrigin.setId(CatalogAuthenticationManager.INTERNAL);
                    }
                    ParamUtils.checkIdentifier(authenticationOrigin.getId(), authOriginsPrefixKey + ".id");
                    ParamUtils.checkObj(authenticationOrigin.getType(), authOriginsPrefixKey + ".type");
                    ParamUtils.checkParameter(authenticationOrigin.getSecretKey(), authOriginsPrefixKey + ".secretKey");
                    ParamUtils.checkParameter(authenticationOrigin.getAlgorithm(), authOriginsPrefixKey + ".algorithm");
                    if (authenticationOriginIds.contains(authenticationOrigin.getId())) {
                        throw new CatalogException("Found duplicated authentication origin id '" + authenticationOrigin.getId() + "'.");
                    }
                    authenticationOriginIds.add(authenticationOrigin.getId());
                }
                if (!internal) {
                    throw new CatalogException("Missing mandatory AuthenticationOrigin of type OPENCGA.");
                }
            }

            result = getOrganizationDBAdaptor(organizationId).update(organizationId, updateMap, options);

            auditManager.auditUpdate(organizationId, userId, Enums.Resource.ORGANIZATION, organization.getId(), organization.getUuid(), "",
                    "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated organization
                OpenCGAResult<Organization> queryResult = getOrganizationDBAdaptor(organizationId).get(options);
                result.setResults(queryResult.getResults());
            }
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

        if (organization.getConfiguration() == null) {
            organization.setConfiguration(new OrganizationConfiguration());
        }
        if (CollectionUtils.isNotEmpty(organization.getConfiguration().getAuthenticationOrigins())) {
            for (AuthenticationOrigin authenticationOrigin : organization.getConfiguration().getAuthenticationOrigins()) {
                ParamUtils.checkParameter(authenticationOrigin.getId(), "AuthenticationOrigin id");
            }
        } else {
            organization.getConfiguration()
                    .setAuthenticationOrigins(Collections.singletonList(
                            CatalogAuthenticationManager.createRandomInternalAuthenticationOrigin()));
        }
        organization.setAttributes(ParamUtils.defaultObject(organization.getAttributes(), HashMap::new));
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
        return catalogDBAdaptorFactory.getOrganizationIds();
    }
}
