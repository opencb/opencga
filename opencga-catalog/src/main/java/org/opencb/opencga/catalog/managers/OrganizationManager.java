package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.organizations.OrganizationConfiguration;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class OrganizationManager extends AbstractManager {

    public static final QueryOptions INCLUDE_ORGANIZATION_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            OrganizationDBAdaptor.QueryParams.ID.key(), OrganizationDBAdaptor.QueryParams.UID.key(),
            OrganizationDBAdaptor.QueryParams.UUID.key()));
    public static final QueryOptions INCLUDE_ORGANIZATION_ADMINS = keepFieldsInQueryOptions(INCLUDE_ORGANIZATION_IDS,
            Arrays.asList(OrganizationDBAdaptor.QueryParams.OWNER.key(), OrganizationDBAdaptor.QueryParams.ADMINS.key()));
    protected static Logger logger = LoggerFactory.getLogger(OrganizationManager.class);
    private final CatalogIOManager catalogIOManager;

    OrganizationManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                   DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManager catalogIOManager, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.catalogIOManager = catalogIOManager;
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
            queryResult = getOrganizationDBAdaptor(organizationId).get(options);
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
        String userId = this.catalogManager.getUserManager().getUserId(ParamConstants.ADMIN_ORGANIZATION, token);

        ObjectMap auditParams = new ObjectMap()
                .append("organizationCreateParams", organizationCreateParams)
                .append("options", options)
                .append("token", token);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        OpenCGAResult<Organization> queryResult;
        Organization organization;
        try {
            // The first time we create the ADMIN_ORGANIZATION as there are no users yet, we should not check anything
            if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationCreateParams.getId())) {
                //Only the OpenCGA administrator can create an organization
                authorizationManager.checkIsInstallationAdministrator(ParamConstants.ADMIN_ORGANIZATION, userId);
            }

            ParamUtils.checkObj(organizationCreateParams, "organizationCreateParams");

            organization = organizationCreateParams.toOrganization();
            validateOrganizationForCreation(organization);

            queryResult = catalogDBAdaptorFactory.createOrganization(organization, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                OpenCGAResult<Organization> result = getOrganizationDBAdaptor(organization.getId()).get(options);
                organization = result.first();
                // Fetch created organization
                queryResult.setResults(result.getResults());
            }
        } catch (CatalogException e) {
            auditManager.auditCreate(ParamConstants.ADMIN_ORGANIZATION, userId, Enums.Resource.ORGANIZATION,
                    organizationCreateParams.getId(), "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        try {
            catalogIOManager.createOrganization(organization.getId().toLowerCase());
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

        OpenCGAResult<Organization> result = OpenCGAResult.empty(Organization.class);
        try {
            OpenCGAResult<Organization> internalResult = get(organizationId, INCLUDE_ORGANIZATION_ADMINS, token);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Organization '" + organizationId + "' not found");
            }
            Organization organization = internalResult.first();

            // We set the proper values for the audit
            organizationId = organization.getId();

            OpenCGAResult<Organization> updateResult = getOrganizationDBAdaptor(organizationId)
                    .update(organizationId, updateMap, options);
            result.append(updateResult);

            auditManager.auditUpdate(organizationId, userId, Enums.Resource.ORGANIZATION, organization.getId(), organization.getUuid(), "",
                    "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, organizationId, e.getMessage());
            result.getEvents().add(event);
            result.setNumErrors(result.getNumErrors() + 1);

            logger.error("Cannot update organization {}: {}", organizationId, e.getMessage());
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.ORGANIZATION, organizationId, organizationId, "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
        return result;
    }

    private void validateOrganizationForCreation(Organization organization) throws CatalogParameterException {
        ParamUtils.checkParameter(organization.getId(), OrganizationDBAdaptor.QueryParams.ID.key());

        organization.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.ORGANIZATION));
        organization.setName(ParamUtils.defaultString(organization.getName(), organization.getId()));
        organization.setDomain(ParamUtils.defaultString(organization.getDomain(), ""));
        organization.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(organization.getCreationDate(),
                OrganizationDBAdaptor.QueryParams.CREATION_DATE.key()));
        organization.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(organization.getModificationDate(),
                OrganizationDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
        organization.setAdmins(Collections.emptyList());
        organization.setProjects(Collections.emptyList());

        if (organization.getConfiguration() != null && organization.getConfiguration().getAuthentication() != null
                && CollectionUtils.isNotEmpty(organization.getConfiguration().getAuthentication().getAuthenticationOrigins())) {
            for (AuthenticationOrigin authenticationOrigin
                    : organization.getConfiguration().getAuthentication().getAuthenticationOrigins()) {
                ParamUtils.checkParameter(authenticationOrigin.getId(), "AuthenticationOrigin id");
            }
        } else {
            organization.setConfiguration(new OrganizationConfiguration());
        }
        organization.setAttributes(ParamUtils.defaultObject(organization.getAttributes(), HashMap::new));
    }


}
