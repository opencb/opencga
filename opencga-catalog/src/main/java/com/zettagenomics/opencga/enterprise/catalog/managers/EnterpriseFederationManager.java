package com.zettagenomics.opencga.enterprise.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.zettagenomics.opencga.enterprise.catalog.utils.FederationUtils;
import com.zettagenomics.opencga.enterprise.catalog.utils.SecureKeyUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.federation.FederationClientUpdateParams;
import org.opencb.opencga.core.models.federation.FederationServerCreateParams;
import org.opencb.opencga.core.models.federation.FederationServerUpdateParams;
import org.opencb.opencga.core.models.federation.FederationUserParams;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.*;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.client.GenericClient;
import org.opencb.opencga.core.client.ParentClient;
import org.opencb.opencga.core.common.MailUtils;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.exceptions.MailException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.federation.FederationClientParams;
import org.opencb.opencga.core.models.federation.FederationClientParamsRef;
import org.opencb.opencga.core.models.federation.FederationServerParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserInternal;
import org.opencb.opencga.core.models.user.UserStatus;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnterpriseFederationManager extends EnterpriseAbstractManager {

    protected static Logger logger = LoggerFactory.getLogger(EnterpriseFederationManager.class);

    private final static Pattern URL_REDIRECT = Pattern.compile(".*/webservices/rest/v\\d+/([^/]+)/(.*)");
    private final static String SEPARATOR = "______";
    private final static int PASSWORD_LENGTH = 16;

    private final QueryOptions ORGANIZATION_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            OrganizationDBAdaptor.QueryParams.ID.key(), OrganizationDBAdaptor.QueryParams.OWNER.key(),
            OrganizationDBAdaptor.QueryParams.ADMINS.key(), OrganizationDBAdaptor.QueryParams.FEDERATION.key(),
            OrganizationDBAdaptor.QueryParams.CONFIGURATION.key()));
    private final QueryOptions PROJECT_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ProjectDBAdaptor.QueryParams.ID.key(), ProjectDBAdaptor.QueryParams.FQN.key(), ProjectDBAdaptor.QueryParams.UID.key(),
            ProjectDBAdaptor.QueryParams.FEDERATION.key(),
            ProjectDBAdaptor.QueryParams.STUDIES.key() + "." + StudyDBAdaptor.QueryParams.ID.key(),
            ProjectDBAdaptor.QueryParams.STUDIES.key() + "." + StudyDBAdaptor.QueryParams.FQN.key(),
            ProjectDBAdaptor.QueryParams.STUDIES.key() + "." + StudyDBAdaptor.QueryParams.UID.key()));

    public EnterpriseFederationManager(CatalogManager catalogManager, Configuration configuration) {
        super(catalogManager, configuration);
    }

    // ************* FOR SERVERS **************** //
    public OpenCGAResult<FederationServerParams> createFederation(String url, FederationServerCreateParams federationServerCreateParams,
                                                                  String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("url", url)
                .append("server", federationServerCreateParams)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            FederationServerParams federationServer = generateFederationServer(federationServerCreateParams);
            QueryOptions orgOptions = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.FEDERATION.key());

            DBAdaptorFactory catalogDBAdaptorFactory = EnterpriseFactory.getCatalogDBAdaptorFactory();
            Organization organization = catalogDBAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(orgOptions).first();

            // Validate that the organization does not already have any federated client using the same id
            if (organization.getFederation() != null
                    && organization.getFederation().getClients().stream().anyMatch(c -> c.getId().equals(federationServer.getId()))) {
                throw new CatalogParameterException("The organization already has a federation server with the same id.");
            }

            // Validate that it doesn't exist any user id with the same id
            if (catalogDBAdaptorFactory.getCatalogUserDBAdaptor(organizationId).exists(federationServer.getUserId())) {
                throw new CatalogParameterException("Please, use a different user id. The one provided already exists.");
            }

            // Write the FederationClient in the database
            ObjectMap parameters = new ObjectMap()
                    .append(OrganizationDBAdaptor.QueryParams.FEDERATION_SERVERS.key(), Collections.singletonList(federationServer));
            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(OrganizationDBAdaptor.QueryParams.FEDERATION_SERVERS.key(), ParamUtils.AddRemoveAction.ADD);
            orgOptions = new QueryOptions(Constants.ACTIONS, actionMap);

            OpenCGAResult<Organization> result = catalogDBAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId)
                    .update(organizationId, parameters, orgOptions);
            if (result.getNumUpdated() == 0) {
                throw new CatalogException("Internal error: Federation server could not be created.");
            }

            // Create new user
            Account.AuthenticationOrigin authenticationOrigin = new Account.AuthenticationOrigin(
                    AuthenticationOrigin.AuthenticationType.OPENCGA.name(), true, false);
            // Create 24h expiration time
            String expTime = TimeUtils.getTime(TimeUtils.add24HtoDate(TimeUtils.getDate()));
            User user = new User()
                    .setId(federationServer.getUserId())
                    .setInternal(new UserInternal().setAccount(new Account(expTime, authenticationOrigin)));
            String password = PasswordUtils.getStrongRandomPassword(PASSWORD_LENGTH);
            catalogManager.getUserManager().create(user, password, token);

            FederationClientParams federationClient = new FederationClientParams()
                    .setUrl(url)
                    .setOrganizationId(organizationId)
                    .setUserId(federationServer.getUserId())
                    .setPassword(password)
                    .setSecurityKey(federationServer.getSecurityKey());

            List<Event> eventList = new ArrayList<>();
            try {
                MailUtils mailUtils = MailUtils.configure(catalogManager.getConfiguration().getEmail());
                mailUtils.sendMail(federationServer.getEmail(), "XetaBase: Federation client credentials",
                        getCredentialsMailContent(federationServer.getEmail(), federationClient));
            } catch (MailException e) {
                eventList.add(new Event(Event.Type.WARNING, "Federation was successfully created. However, we could not send email with "
                        + "credentials to the federation client. Please, check with your administrator your email configuration in "
                        + "the 'configuration.yml' file and then call to '/federations/server/{federationServerId}/reset' again."));
            }

            auditManager.audit(organizationId, userId, Enums.Action.CREATE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(0, eventList, 1, Collections.singletonList(federationServer), 1);
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.CREATE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    public OpenCGAResult<FederationServerParams> reset(String url, String federationServerId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("url", url)
                .append("federationId", federationServerId)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            // Look for the FederationServerParams object
            DBAdaptorFactory catalogDBAdaptorFactory = EnterpriseFactory.getCatalogDBAdaptorFactory();
            Organization organization = catalogDBAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(ORGANIZATION_OPTIONS)
                    .first();
            FederationServerParams serverParams = FederationUtils.findFederationServer(organization, federationServerId);

            // Generate new client params
            FederationClientParams clientParams = new FederationClientParams()
                    .setUrl(url)
                    .setOrganizationId(organizationId)
                    .setUserId(serverParams.getUserId())
                    .setPassword(PasswordUtils.getStrongRandomPassword(PASSWORD_LENGTH))
                    .setSecurityKey(SecureKeyUtils.generateNewSecurityKey());

            // Update security key
            FederationServerUpdateParams updateParams = new FederationServerUpdateParams()
                    .setSecurityKey(clientParams.getSecurityKey());
            OpenCGAResult<Organization> result = updateFederationServer(organizationId, federationServerId, updateParams);
            if (result.getNumUpdated() == 0) {
                throw new CatalogException("Internal error. Could not update security key.");
            }

            // Extend user account expiration date and change password
            ObjectMap params = new ObjectMap()
                    .append(UserDBAdaptor.QueryParams.INTERNAL_ACCOUNT_EXPIRATION_DATE.key(),
                            TimeUtils.getTime(TimeUtils.add24HtoDate(TimeUtils.getDate())));
            catalogDBAdaptorFactory.getCatalogUserDBAdaptor(organizationId).update(serverParams.getUserId(), params);
            catalogDBAdaptorFactory.getCatalogUserDBAdaptor(organizationId).changePassword(serverParams.getUserId(), null,
                    clientParams.getPassword());

            List<Event> eventList = new ArrayList<>();
            try {
                MailUtils mailUtils = MailUtils.configure(catalogManager.getConfiguration().getEmail());
                mailUtils.sendMail(serverParams.getEmail(), "XetaBase: Federation client credentials",
                        getCredentialsMailContent(serverParams.getEmail(), clientParams));
            } catch (MailException e) {
                eventList.add(new Event(Event.Type.WARNING, "Could not send email with credentials to the federation client. Please, "
                        + "check with your administrator your email configuration in the 'configuration.yml' file and then call "
                        + "to '/federations/server/{federationServerId}/reset' again."));
            }

            auditManager.audit(organizationId, userId, Enums.Action.RESET_FEDERATION_CLIENT_CREDENTIALS, Enums.Resource.ORGANIZATION,
                    organizationId, "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(0, eventList, 0, Collections.singletonList(serverParams), 0);
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.RESET_FEDERATION_CLIENT_CREDENTIALS, Enums.Resource.ORGANIZATION,
                    organizationId, "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    private String getCredentialsMailContent(String user, FederationClientParams clientParams) {
        return new StringBuilder()
                .append("Hi ").append(user).append(",\n\n")
                .append("We have invited you to access our XetaBase installation under '").append(clientParams.getUrl()).append("'.\n\n")
                .append("In order to get access, you will need to call to REST endpoint '/federations/client/connect' using the following"
                        + " credentials:\n\n")
                .append("URL: ").append(clientParams.getUrl()).append("\n")
                .append("Organization ID: ").append(clientParams.getOrganizationId()).append("\n")
                .append("User ID: ").append(clientParams.getUserId()).append("\n")
                .append("Temporary password: ").append(clientParams.getPassword()).append("\n")
                .append("Security key: ").append(clientParams.getSecurityKey()).append("\n\n")
                .append("The password and the secret key will be automatically renewed upon first login.\n\n")
                .append("You now have 24 hours to connect to the federation server. After that, the temporary password will expire.\n\n")
                .append("Best regards,\n\n")
                .toString();
    }

    /**
     * Reset the security key of the federation server.
     * This method should only be called the very first time the server B connects to the federation server A.
     * The federation server A will then:
     * 1. Extend the expiration time of the federated server B.
     * 2. Update the security key for the communication between the two servers.
     * 3. Update the user password
     * 4. Send the new security key and user password in the response so that the server B can also update it.
     *
     * @param token token corresponding to the user given to the server B.
     * @return the new updated security key and user password to be used in further communications.
     * @throws CatalogException if there is any error.
     */
    public OpenCGAResult<String> resetSecurityKey(String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("token", token);
        try {
            // Check the user is the one created to communicate between federated servers
            User user = dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId).get(userId, UserManager.INCLUDE_INTERNAL).first();
            if (!user.getInternal().getAccount().getAuthentication().isFederation()) {
                throw new CatalogAuthorizationException("User '" + userId + "' is not a federated user.");
            }
            Date expirationDate = TimeUtils.toDate(user.getInternal().getAccount().getExpirationDate());
            // If there are less than 24h until the expiration date
            Date date = TimeUtils.add24HtoDate(TimeUtils.getDate());
            if (expirationDate.after(date)) {
                throw new CatalogAuthorizationException("Operation already executed. This method should only be called once.");
            }

            OpenCGAResult<Organization> orgResult = dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(ORGANIZATION_OPTIONS);
            if (orgResult.getNumResults() == 0) {
                throw new CatalogException("Organization '" + organizationId + "' not found.");
            }

            Organization organization = orgResult.first();
            if (organization.getFederation() == null || CollectionUtils.isEmpty(organization.getFederation().getServers())) {
                throw new CatalogException("The organization does not have any federation server configured.");
            }
            String federationId = null;
            for (FederationServerParams server : organization.getFederation().getServers()) {
                if (server.getUserId().equals(userId)) {
                    federationId = server.getId();
                    break;
                }
            }
            if (federationId == null) {
                throw new CatalogException("Federation server not found.");
            }

            // Extend user account expiration date
            ObjectMap userUpdateParams = new ObjectMap(UserDBAdaptor.QueryParams.INTERNAL_ACCOUNT_EXPIRATION_DATE.key(),
                    organization.getConfiguration().getDefaultUserExpirationDate());
            dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId).update(userId, userUpdateParams);

            // Generate new security key and user password
            String newSecurityKey = SecureKeyUtils.generateNewSecurityKey();
            String newPassword = PasswordUtils.getStrongRandomPassword(PASSWORD_LENGTH);
            FederationServerUpdateParams updateParams = new FederationServerUpdateParams()
                    .setSecurityKey(newSecurityKey);
            OpenCGAResult<Organization> result = updateFederationServer(organizationId, federationId, updateParams);
            if (result.getNumUpdated() == 0) {
                // Restore old expiration time
                userUpdateParams = new ObjectMap(UserDBAdaptor.QueryParams.INTERNAL_ACCOUNT_EXPIRATION_DATE.key(),
                        user.getInternal().getAccount().getExpirationDate());
                dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId).update(userId, userUpdateParams);

                throw new CatalogException("Could not update security key.");
            }
            // Change password
            dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId).changePassword(userId, null, newPassword);

            auditManager.audit(organizationId, userId, Enums.Action.RESET_FEDERATION_SECURITY_KEY, Enums.Resource.ORGANIZATION,
                    organizationId, "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(result.getTime(), Collections.singletonList(newSecurityKey + SEPARATOR + newPassword));
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.RESET_FEDERATION_SECURITY_KEY, Enums.Resource.ORGANIZATION,
                    organizationId, "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw new CatalogException(e);
        }
    }

    public OpenCGAResult<Organization> update(String clientId, FederationServerUpdateParams updateParams, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("updateParams", updateParams)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
            OpenCGAResult<Organization> result = updateFederationServer(organizationId, clientId, updateParams);
            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            if (e instanceof CatalogException) {
                throw (CatalogException) e;
            } else {
                throw new CatalogException(e);
            }
        }
    }

    public OpenCGAResult<Organization> deleteFederationServer(String federationServerId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("federationServerId", federationServerId)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            DBAdaptorFactory catalogDBAdaptorFactory = EnterpriseFactory.getCatalogDBAdaptorFactory();
            Organization organization = catalogDBAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(ORGANIZATION_OPTIONS)
                    .first();
            FederationServerParams serverParams = FederationUtils.findFederationServer(organization, federationServerId);

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(OrganizationDBAdaptor.QueryParams.FEDERATION_SERVERS.key(), ParamUtils.AddRemoveAction.REMOVE);
            QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

            ObjectMap params = new ObjectMap(OrganizationDBAdaptor.QueryParams.FEDERATION_SERVERS.key(),
                    Collections.singletonList(serverParams));

            OpenCGAResult<Organization> result = dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId)
                    .update(organizationId, params, options);

            // Remove user id from all study groups
            dbAdaptorFactory.getCatalogStudyDBAdaptor(organizationId)
                    .removeUsersFromAllGroups(Collections.singletonList(serverParams.getUserId()));
            // And suspend account
            catalogManager.getUserManager().changeStatus(organizationId, serverParams.getUserId(), UserStatus.SUSPENDED,
                    QueryOptions.empty(), token);


            auditManager.audit(organizationId, userId, Enums.Action.DELETE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.DELETE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            if (e instanceof CatalogException) {
                throw (CatalogException) e;
            } else {
                throw new CatalogException(e);
            }
        }
    }

    public OpenCGAResult<Organization> deleteFederationClient(String federationClientId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("federationClientId", federationClientId)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            DBAdaptorFactory catalogDBAdaptorFactory = EnterpriseFactory.getCatalogDBAdaptorFactory();
            Organization organization = catalogDBAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(ORGANIZATION_OPTIONS)
                    .first();
            FederationClientParams clientParams = FederationUtils.findFederationClient(organization, federationClientId);

            // 1. Remove federationClient
            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(OrganizationDBAdaptor.QueryParams.FEDERATION_CLIENTS.key(), ParamUtils.AddRemoveAction.REMOVE);
            QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
            ObjectMap params = new ObjectMap(OrganizationDBAdaptor.QueryParams.FEDERATION_CLIENTS.key(),
                    Collections.singletonList(clientParams));
            OpenCGAResult<Organization> result = dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId)
                    .update(organizationId, params, options);

            // 2. Remove federated projects and studies
            List<Project> projects = catalogManager.getProjectManager().search(organizationId,
                    new Query(ProjectDBAdaptor.QueryParams.INTERNAL_FEDERATED.key(), true), PROJECT_OPTIONS, token).getResults();

            // 2.1. Filter out projects and studies that are not from federated this federated server
            projects.removeIf(project -> !project.getFederation().getId().equals(clientParams.getId()));

            Set<Long> studiesToRemove = new HashSet<>();
            Set<Long> projectsToRemove = new HashSet<>();
            for (Project project : projects) {
                for (Study study : project.getStudies()) {
                    studiesToRemove.add(study.getUid());
                }
                projectsToRemove.add(project.getUid());
            }
            // 2.2. Remove federated projects/studies
            removeFederatedStudies(organizationId, studiesToRemove);
            removeFederatedProjects(organizationId, projectsToRemove);

            auditManager.audit(organizationId, userId, Enums.Action.DELETE_FEDERATION_CLIENT, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.DELETE_FEDERATION_CLIENT, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            if (e instanceof CatalogException) {
                throw (CatalogException) e;
            } else {
                throw new CatalogException(e);
            }
        }
    }


    // ************* FOR CLIENTS **************** //
    public OpenCGAResult<FederationClientParams> connect(FederationClientParams federationClient, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("client", federationClient)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
            validateFederationClientParams(federationClient);

            // Validate duplicated federation client id
            Organization organization = EnterpriseFactory.getCatalogDBAdaptorFactory().getCatalogOrganizationDBAdaptor(organizationId)
                    .get(new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.FEDERATION_CLIENTS.key())).first();
            if (organization.getFederation().getClients().stream().anyMatch(c -> c.getId().equalsIgnoreCase(federationClient.getId()))) {
                throw new CatalogParameterException("The organization already has a federation client with the same id.");
            }

            // Obtain the projects and studies from the federation server
            GenericClient client = FederationUtils.getClientInstance(federationClient);
            List<Project> projectList = obtainRemoteProjectsAndStudies(client);
            if (projectList.isEmpty()) {
                throw new CatalogException("The federation server did not share any project. Please, contact them and try again.");
            }

            // Store the projects and studies in the database
            importFederatedProjects(federationClient.getId(), organizationId, projectList);

            // Check and store OpenCGA version from remote server
            ObjectMap aboutMap = client.about().firstResult();
            String version = aboutMap.getString("Version");
            federationClient.setVersion(version);

            // Store the federation client object in the database
            ObjectMap parameters = new ObjectMap()
                    .append(OrganizationDBAdaptor.QueryParams.FEDERATION_CLIENTS.key(), Collections.singletonList(federationClient));
            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(OrganizationDBAdaptor.QueryParams.FEDERATION_CLIENTS.key(), ParamUtils.AddRemoveAction.ADD);
            QueryOptions orgOptions = new QueryOptions(Constants.ACTIONS, actionMap);

            OpenCGAResult<Organization> result = EnterpriseFactory.getCatalogDBAdaptorFactory()
                    .getCatalogOrganizationDBAdaptor(organizationId).update(organizationId, parameters, orgOptions);
            if (result.getNumUpdated() == 0) {
                throw new CatalogException("Internal error: Federation client could not be created.");
            }

            // Query and update new security key in the database
            String updatedCredentials = getUpdatedCredentials(client);
            String[] split = StringUtils.split(updatedCredentials, SEPARATOR);
            String updatedSecurityKey = split[0];
            String updatedPassword = split[1];
            FederationClientUpdateParams updateParams = new FederationClientUpdateParams()
                    .setSecurityKey(updatedSecurityKey)
                    .setPassword(updatedPassword)
                    .setToken("");
            result = updateFederationClient(organizationId, federationClient.getId(), updateParams);
            if (result.getNumUpdated() == 0) {
                throw new CatalogException("Could not update security key. Please, talk to the federation server administrator as the " +
                        "communication with the federation may be broken.");
            }

            auditManager.audit(organizationId, userId, Enums.Action.CREATE_FEDERATION_CLIENT, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return new OpenCGAResult<>(0, Collections.singletonList(federationClient));
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.CREATE_FEDERATION_CLIENT, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            if (e instanceof CatalogException) {
                throw (CatalogException) e;
            } else {
                throw new CatalogException(e);
            }
        }
    }

    public OpenCGAResult<FederationClientParams> sync(String federationId, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("federationId", federationId)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            Organization organization = EnterpriseFactory.getCatalogDBAdaptorFactory().getCatalogOrganizationDBAdaptor(organizationId)
                    .get(ORGANIZATION_OPTIONS).first();
            FederationClientParams federationClient = FederationUtils.findFederationClient(organization, federationId);
            GenericClient client = FederationUtils.getClientInstance(federationClient);
            List<Project> federatedProjects = obtainRemoteProjectsAndStudies(client);

            // Obtain shared projects/studies locally
            List<Project> projects = catalogManager.getProjectManager().search(organizationId,
                    new Query(ProjectDBAdaptor.QueryParams.INTERNAL_FEDERATED.key(), true), PROJECT_OPTIONS, token).getResults();

            // Filter out projects and studies that are not from federated this federated server
            projects.removeIf(project -> !project.getFederation().getId().equals(federationId));

            Set<Long> projectsToRemove = new HashSet<>();
            Set<Long> studiesToRemove = new HashSet<>();
            List<Project> newProjects = new LinkedList<>();
            Map<Project, List<Study>> newStudies = new HashMap<>();

            // Look for projects and studies that are no longer present in the federated server
            for (Project project : projects) {
                Optional<Project> fedProjectOpt = federatedProjects.stream().filter(p -> p.getFqn().equals(project.getFqn())).findFirst();
                if (!fedProjectOpt.isPresent()) {
                    projectsToRemove.add(project.getUid());
                } else {
                    Project federatedProject = fedProjectOpt.get();
                    // Check studies
                    for (Study study : project.getStudies()) {
                        if (federatedProject.getStudies().stream().noneMatch(s -> s.getFqn().equals(study.getFqn()))) {
                            studiesToRemove.add(study.getUid());
                        }
                    }
                }
            }

            // Look for new projects and studies in the federated server
            for (Project project : federatedProjects) {
                if (projects.stream().noneMatch(p -> p.getFqn().equals(project.getFqn()))) {
                    newProjects.add(project);
                } else {
                    // Get matching project
                    Project localProject = projects.stream().filter(p -> p.getFqn().equals(project.getFqn())).findFirst().get();
                    List<Study> studyList = new ArrayList<>(project.getStudies().size());
                    for (Study study : project.getStudies()) {
                        if (localProject.getStudies().stream().noneMatch(s -> s.getFqn().equals(study.getFqn()))) {
                            studyList.add(study);
                        }
                    }
                    if (CollectionUtils.isNotEmpty(studyList)) {
                        newStudies.put(localProject, studyList);
                    }
                }
            }

            // Remove old federated projects/studies
            removeFederatedStudies(organizationId, studiesToRemove);
            removeFederatedProjects(organizationId, projectsToRemove);

            // Import new federated projects/studies
            importFederatedProjects(federationId, organizationId, newProjects);
            for (Map.Entry<Project, List<Study>> entry : newStudies.entrySet()) {
                Project project = entry.getKey();
                List<Study> studyList = entry.getValue();
                importFederatedStudies(federationId, organization, project, studyList);
            }

            auditManager.audit(organizationId, userId, Enums.Action.SYNCHRONIZE_FEDERATION_CLIENT, Enums.Resource.ORGANIZATION,
                    organizationId, "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return new OpenCGAResult<>(0, Collections.emptyList());
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.SYNCHRONIZE_FEDERATION_CLIENT, Enums.Resource.ORGANIZATION,
                    organizationId, "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            if (e instanceof CatalogException) {
                throw (CatalogException) e;
            } else {
                throw new CatalogException(e);
            }
        }
    }

    public OpenCGAResult<Organization> update(String clientId, FederationClientUpdateParams updateParams, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("updateParams", updateParams)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            if (StringUtils.isNotEmpty(updateParams.getSecurityKey()) || StringUtils.isNotEmpty(updateParams.getPassword())) {
                Organization organization = EnterpriseFactory.getCatalogDBAdaptorFactory().getCatalogOrganizationDBAdaptor(organizationId)
                        .get(ORGANIZATION_OPTIONS).first();
                FederationClientParams federationClient = FederationUtils.findFederationClient(organization, clientId);

                // Override values with the new ones
                federationClient.setSecurityKey(StringUtils.isNotEmpty(updateParams.getSecurityKey())
                        ? updateParams.getSecurityKey() : federationClient.getSecurityKey());
                federationClient.setPassword(StringUtils.isNotEmpty(updateParams.getPassword())
                        ? updateParams.getPassword() : federationClient.getPassword());

                // Remove token so it tries to generate a new one
                federationClient.setToken(null);

                // Query and update new security key in the database
                GenericClient client = FederationUtils.getClientInstance(federationClient);
                String updatedCredentials = getUpdatedCredentials(client);
                String[] split = StringUtils.split(updatedCredentials, SEPARATOR);
                String updatedSecurityKey = split[0];
                String updatedPassword = split[1];
                updateParams
                        .setSecurityKey(updatedSecurityKey)
                        .setPassword(updatedPassword)
                        .setToken("");
            }

            OpenCGAResult<Organization> result = updateFederationClient(organizationId, clientId, updateParams);
            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_FEDERATION_CLIENT, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_FEDERATION_CLIENT, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            if (e instanceof CatalogException) {
                throw (CatalogException) e;
            } else {
                throw new CatalogException(e);
            }
        }
    }

    public RestResponse<Object> redirect(String url, Map<String, Object> queryParams, Object body, String method, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("url", url)
                .append("queryParams", queryParams)
                .append("body", body)
                .append("method", method)
                .append("token", token);
        try {
            String project = FederationUtils.extractProject(url, queryParams);
            String study = FederationUtils.extractStudy(url, queryParams);
            String federationId = FederationUtils.findFederationServerIdInPayload(project, study, tokenPayload);

            // Obtain the federation server credentials
            Organization organization = EnterpriseFactory.getCatalogDBAdaptorFactory().getCatalogOrganizationDBAdaptor(organizationId)
                    .get(ORGANIZATION_OPTIONS).first();
            FederationClientParams federationClient = FederationUtils.findFederationClient(organization, federationId);

            // Call to project/study info to check if the user still has access to the project/study before redirecting
            if (StringUtils.isNotEmpty(study)) {
                catalogManager.getStudyManager().get(study, StudyManager.INCLUDE_STUDY_IDS, token);
            } else {
                catalogManager.getProjectManager().get(study, ProjectManager.INCLUDE_PROJECT_IDS, token);
            }

            String federationToken = federationClient.getToken();
            // The call to getClientInstance will update the token if it has expired
            GenericClient client = FederationUtils.getClientInstance(federationClient);
            if (!federationClient.getToken().equals(federationToken)) {
                // Token has been updated.
                FederationClientUpdateParams updateParams = new FederationClientUpdateParams()
                        .setToken(federationClient.getToken());

                // Call to /about to check if the version has changed only once per token update
                ObjectMap aboutMap = client.about().firstResult();
                String version = aboutMap.getString("Version");
                if (StringUtils.isNotEmpty(version) && !version.equals(federationClient.getVersion())) {
                    logger.warn("Calling to federation server '{}'. The version of the federation server has changed from '{}' to '{}'.",
                            federationClient.getUrl(), federationClient.getVersion(), version);
                    // Update new version in database
                    updateParams.setVersion(version);
                }

                // Store changes in database
                OpenCGAResult<Organization> result = updateFederationClient(organizationId, federationClient.getId(), updateParams);
                if (result.getNumUpdated() == 0) {
                    throw new CatalogException("Could not update token to communicate with the federation server.");
                }
                logger.debug("Token has been updated.");
            }

            Matcher matcher = URL_REDIRECT.matcher(url);
            if (!matcher.find()) {
                throw new CatalogException("The url '" + url + "' does not seem to follow the OpenCGA URL pattern.");
            }
            String group1 = matcher.group(1);
            String group2 = matcher.group(2);

            boolean isAclQuery = isAclQuery(url, queryParams, userId, federationClient);
            RestResponse<Object> execute = client.execute(group1, group2, queryParams, body, method, Object.class);
            if (isAclQuery) {
                changeAclResponse(userId, federationClient, execute);
            }

            auditManager.audit(organizationId, userId, Enums.Action.FEDERATION_REDIRECT, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return execute;
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.FEDERATION_REDIRECT, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw new CatalogException(e);
        }
    }

    /**
     * If the user is requesting the ACLs of its own user, we need to replace the user id for the one used in the federation.
     *
     * @param url                 Url requested to validate if it is an ACL query.
     * @param queryParams         Query parameters of the request.
     * @param userId              User id of the user requesting the information.
     * @param federationClient    Federation client object containing the user id used in the federation.
     * @return true if the query is an ACL query and the member is the user requesting the information.
     */
    private boolean isAclQuery(String url, Map<String, Object> queryParams, String userId, FederationClientParams federationClient) {
        if (url.endsWith("/acl") || url.endsWith("/acl/")) {
            Object memberObj = queryParams.get("member");
            if (memberObj != null) {
                String member = String.valueOf(memberObj);
                if (userId.equals(member)) {
                    // The user wants to know the permissions for himself. We need to change for the user being used in the federation
                    logger.info("Changing member from '{}' to '{}' for federated /acl query", member, federationClient.getUserId());
                    queryParams.put("member", federationClient.getUserId());
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Assuming the RestResponse object is of type {@link org.opencb.opencga.core.models.AclEntryList}, change the user id from the
     * federation user to the user id of the user requesting the information.
     *
     * @param userId            User id of the user requesting the information.
     * @param federationClient  Federation client object containing the user id used in the federation.
     * @param response          RestResponse object containing the ACLs.
     */
    private void changeAclResponse(String userId, FederationClientParams federationClient, RestResponse<Object> response) {
        if (CollectionUtils.isNotEmpty(response.getResponses())) {
            if (!federationClient.getUserId().equals(response.getParams().get("member"))) {
                // The member is not the user requesting the information. Nothing to do
                return;
            }
            logger.info("Changing member from '{}' to '{}' for federated /acl query response", federationClient.getUserId(), userId);
            response.getParams().put("member", userId);

            if (CollectionUtils.isNotEmpty(response.getResponses().get(0).getResults())) {
                for (Object data : response.getResponses().get(0).getResults()) {
                    if (data instanceof Map) {
                        List<Map<String, Object>> list = (List<Map<String, Object>>) ((Map<String, Object>) data).get("acl");
                        for (Map<String, Object> aclEntry : list) {
                            String member = String.valueOf(aclEntry.get("member"));
                            if (federationClient.getUserId().equals(member)) {
                                aclEntry.put("member", userId);
                            }
                        }
                    }
                }
            }
        }
    }

    public OpenCGAResult<?> shareStudy(String studyStr, String action, FederationUserParams params, String token)
            throws CatalogException {
        long startTime = System.currentTimeMillis();
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("action", action)
                .append("params", params)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(tokenPayload.getOrganization(), userId);

            QueryOptions queryOptions = new QueryOptions(StudyDBAdaptor.QueryParams.INTERNAL.key(), true);
            Study study = catalogManager.getStudyManager().get(studyStr, queryOptions, token).first();
            if (!study.getInternal().isFederated()) {
                throw new CatalogException("Study '" + studyStr + "' is not federated.");
            }

            ParamUtils.checkObj(params, "body");
            ParamUtils.checkNotEmptyArray(params.getUserIds(), "userIds");

            if (StringUtils.isEmpty(action)) {
                action = ParamUtils.AddRemoveAction.ADD.name();
            }
            ParamUtils.BasicUpdateAction updateAction = ParamUtils.BasicUpdateAction.valueOf(action);
            GroupUpdateParams groupUpdateParams = new GroupUpdateParams(params.getUserIds());

            catalogManager.getStudyManager().updateGroup(studyStr, StudyManager.MEMBERS, updateAction, groupUpdateParams, token);
            auditManager.audit(organizationId, userId, Enums.Action.FEDERATION_SHARE, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>((int) (System.currentTimeMillis() - startTime), Collections.emptyList(), 0, 0, 1, 0);
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.FEDERATION_SHARE, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw new CatalogException(e);
        }
    }

    public OpenCGAResult<FederationUserParams> showUsers(String studyStr, String token) throws CatalogException {
        long startTime = System.currentTimeMillis();
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(tokenPayload.getOrganization(), userId);

            QueryOptions queryOptions = new QueryOptions(StudyDBAdaptor.QueryParams.INTERNAL.key(), true);
            Study study = catalogManager.getStudyManager().get(studyStr, queryOptions, token).first();
            if (!study.getInternal().isFederated()) {
                throw new CatalogException("Study '" + studyStr + "' is not federated.");
            }

            Group group = catalogManager.getStudyManager().getGroup(studyStr, StudyManager.MEMBERS, token).first();
            auditManager.audit(organizationId, userId, Enums.Action.FEDERATION_SHOW_USERS, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            FederationUserParams userParams = new FederationUserParams(group.getUserIds());
            return new OpenCGAResult<>((int) (System.currentTimeMillis() - startTime), Collections.singletonList(userParams));
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.FEDERATION_SHOW_USERS, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw new CatalogException(e);
        }
    }

    private OpenCGAResult<Organization> updateFederationClient(String organizationId, String federationId,
                                                               FederationClientUpdateParams updateParams) throws CatalogException {
        if (StringUtils.isNotEmpty(updateParams.getSecurityKey())) {
            // Encode security key before storing in database
            updateParams.setSecurityKey(SecureKeyUtils.encodeSecureString(updateParams.getSecurityKey()));
        }
        if (StringUtils.isNotEmpty(updateParams.getPassword())) {
            // Encode password before storing in database
            updateParams.setPassword(SecureKeyUtils.encodeSecureString(updateParams.getPassword()));
        }

        ObjectMap parameters;
        try {
            parameters = updateParams.getUpdateMap();
        } catch (JsonProcessingException e) {
            throw new CatalogException(e);
        }
        return dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).updateFederationClientParams(federationId, parameters);
    }

    private OpenCGAResult<Organization> updateFederationServer(String organizationId, String federationId,
                                                               FederationServerUpdateParams updateParams) throws CatalogException {
        ObjectMap parameters;
        try {
            parameters = updateParams.getUpdateMap();
        } catch (JsonProcessingException e) {
            throw new CatalogException(e);
        }
        return dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).updateFederationServerParams(federationId, parameters);
    }

    private List<Project> obtainRemoteProjectsAndStudies(GenericClient client) throws ClientException {
        return client.execute("projects", null, null, null, "search", Collections.emptyMap(), ParentClient.GET, Project.class).allResults();
    }

    private String getUpdatedCredentials(GenericClient client) throws ClientException {
        RestResponse<String> execute = client.execute("federations", null, null, null, "firstConnection", Collections.emptyMap(),
                ParentClient.POST, String.class);
        if (CollectionUtils.isNotEmpty(execute.getEvents())) {
            for (Event event : execute.getEvents()) {
                if (event.getType().equals(Event.Type.ERROR)) {
                    throw new ClientException("Federation server error: " + event.getMessage());
                }
            }
        }
        return execute.firstResult();
    }

    private void importFederatedProjects(String federationId, String organizationId, List<Project> projectList) throws CatalogException {
        // Validate there are projects and studies
        if (CollectionUtils.isEmpty(projectList)) {
            return;
        }
        for (Project project : projectList) {
            List<Study> studyList = project.getStudies();
            if (CollectionUtils.isEmpty(studyList)) {
                throw new CatalogParameterException("Missing list of studies for project '" + project.getFqn() + "'.");
            }
            // Validate there's no study with the same fqn
            for (Study study : studyList) {
                Query query = new Query(StudyDBAdaptor.QueryParams.FQN.key(), study.getFqn());
                if (dbAdaptorFactory.getCatalogStudyDBAdaptor(organizationId).count(query).getNumMatches() > 0) {
                    throw new CatalogException("Study '" + study.getFqn() + "' already exists.");
                }
            }
        }

        // Fetch organization to get the owner and admins
        Organization organization = dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId)
                .get(OrganizationManager.INCLUDE_ORGANIZATION_ADMINS).first();

        // Insert projects in the database
        for (Project project : projectList) {
            List<Study> studyList = project.getStudies();
            project.setStudies(null);
            project.setFederation(new FederationClientParamsRef(federationId));
            project.getInternal().setFederated(true);
            dbAdaptorFactory.getCatalogProjectDbAdaptor(organizationId).insert(project, null);
            importFederatedStudies(federationId, organization, project, studyList);
        }
    }

    private void importFederatedStudies(String federationId, Organization organization, Project project, List<Study> studyList)
            throws CatalogException {
        if (CollectionUtils.isEmpty(studyList)) {
            throw new CatalogParameterException("Missing list of studies for project '" + project.getFqn() + "'.");
        }
        // Validate there's no study with the same fqn
        for (Study study : studyList) {
            Query query = new Query(StudyDBAdaptor.QueryParams.FQN.key(), study.getFqn());
            if (dbAdaptorFactory.getCatalogStudyDBAdaptor(organization.getId()).count(query).getNumMatches() > 0) {
                throw new CatalogException("Study '" + study.getFqn() + "' already exists.");
            }
        }

        ParamUtils.checkParameter(organization.getOwner(), "Organization owner");
        ParamUtils.checkObj(organization.getAdmins(), "Organization admins");

        // Insert studies in the database
        for (Study study : studyList) {
            FederationUtils.removeStudyFieldsForStorage(organization, study);
            study.setFederation(new FederationClientParamsRef(federationId));
            study.getInternal().setFederated(true);
            dbAdaptorFactory.getCatalogStudyDBAdaptor(organization.getId()).insert(project, study, QueryOptions.empty());
        }
    }

    private void removeFederatedProjects(String organizationId, Set<Long> projectList) throws CatalogException {
        if (CollectionUtils.isEmpty(projectList)) {
            return;
        }
        Query query = new Query(ProjectDBAdaptor.QueryParams.UID.key(), new ArrayList<>(projectList));
        dbAdaptorFactory.getCatalogProjectDbAdaptor(organizationId).delete(query);
    }

    private void removeFederatedStudies(String organizationId, Set<Long> studyList) throws CatalogException {
        if (CollectionUtils.isEmpty(studyList)) {
            return;
        }
        Query query = new Query(StudyDBAdaptor.QueryParams.UID.key(), new ArrayList<>(studyList));
        dbAdaptorFactory.getCatalogStudyDBAdaptor(organizationId).delete(query);
    }

    private void validateFederationClientParams(FederationClientParams federationClient) throws CatalogParameterException {
        // Validate mandatory fields
        ParamUtils.checkIdentifier(federationClient.getId(), "id");
        ParamUtils.checkEmail(federationClient.getEmail());
        ParamUtils.checkParameter(federationClient.getUrl(), "url");
        ParamUtils.checkParameter(federationClient.getOrganizationId(), "organizationId");
        ParamUtils.checkParameter(federationClient.getUserId(), "userId");
        ParamUtils.checkParameter(federationClient.getPassword(), "password");
        ParamUtils.checkParameter(federationClient.getSecurityKey(), "securityKey");

        federationClient.setDescription(StringUtils.isNotEmpty(federationClient.getDescription()) ? federationClient.getDescription() : "");
    }

    private FederationServerParams generateFederationServer(FederationServerCreateParams federationServerCreateParams)
            throws CatalogException {
        // Validate mandatory fields
        ParamUtils.checkIdentifier(federationServerCreateParams.getId(), "id");
        ParamUtils.checkEmail(federationServerCreateParams.getEmail());

        String userId = StringUtils.isNotEmpty(federationServerCreateParams.getUserId())
                ? federationServerCreateParams.getUserId()
                : federationServerCreateParams.getId() + ".user1";
        // Create a random security key
        String securityKey = SecureKeyUtils.generateNewSecurityKey();

        // Create the client
        return new FederationServerParams(federationServerCreateParams.getId(),
                StringUtils.isNotEmpty(federationServerCreateParams.getDescription()) ? federationServerCreateParams.getDescription() : "",
                federationServerCreateParams.getEmail(), userId, true, securityKey);
    }

}
