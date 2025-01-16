package com.zettagenomics.opencga.enterprise.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.zettagenomics.opencga.enterprise.catalog.utils.FederationUtils;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authentication.JwtManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.catalog.managers.UserManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.client.GenericClient;
import org.opencb.opencga.core.client.ParentClient;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.federation.FederationClientParams;
import org.opencb.opencga.core.models.federation.FederationClientParamsRef;
import org.opencb.opencga.core.models.federation.FederationServerCreateParams;
import org.opencb.opencga.core.models.federation.FederationServerParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.LoginParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserInternal;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FederationManager extends AbstractManager {

    protected static Logger logger = LoggerFactory.getLogger(FederationManager.class);

    private final static Pattern URL_REDIRECT = Pattern.compile(".*/webservices/rest/v\\d+/([^/]+)/(.*)");

    public FederationManager(CatalogManager catalogManager, EnterpriseConfiguration enterpriseConfiguration) {
        super(catalogManager, enterpriseConfiguration);
    }

    // ************* FOR SERVERS **************** //
    public OpenCGAResult<FederationClientParams> createFederation(FederationServerCreateParams federationServerCreateParams,
                                                                  String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
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
            String password = PasswordUtils.getStrongRandomPassword();
            catalogManager.getUserManager().create(user, password, token);

            // Send email with all the information (url, user id, password, secret key) to the email provided
            // TODO: Send mail


            auditManager.audit(organizationId, userId, Enums.Action.EXPOSE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            FederationClientParams federationClient = new FederationClientParams()
                    .setOrganizationId(organizationId)
                    .setUserId(federationServer.getUserId())
                    .setPassword(password)
                    .setSecretKey(federationServer.getSecretKey());

            return new OpenCGAResult<>(0, Collections.singletonList(federationClient));
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.EXPOSE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    /**
     * Reset the secret key of the federation server.
     * This method should only be called the very first time the server B connects to the federation server A.
     * The federation server A will then:
     * 1. Extend the expiration time of the federated server B.
     * 2. Update the secret key for the communication between the two servers.
     * 3. Send the new secret key in the response so that the server B can also update it.
     *
     * @param token token corresponding to the user given to the server B.
     * @return the new updated secret key to be used in further communications.
     * @throws CatalogException if there is any error.
     */
    public OpenCGAResult<String> resetSecretKey(String token) throws CatalogException {
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

            QueryOptions orgOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    OrganizationDBAdaptor.QueryParams.FEDERATION.key(), OrganizationDBAdaptor.QueryParams.CONFIGURATION.key()));
            OpenCGAResult<Organization> orgResult = dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(orgOptions);
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

            // Update secret key
            String newSecretKey = PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH);
            FederationServerParams serverParams = new FederationServerParams()
                    .setSecretKey(newSecretKey);
            ObjectMap parameters = new ObjectMap(JacksonUtils.getUpdateObjectMapper().writeValueAsString(serverParams));
            // Remove active as it will be set to active = false;
            parameters.remove("active");

            OpenCGAResult<Organization> result = dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId)
                    .updateFederationServerParams(federationId, parameters);
            if (result.getNumUpdated() == 0) {
                // Restore old expiration time
                userUpdateParams = new ObjectMap(UserDBAdaptor.QueryParams.INTERNAL_ACCOUNT_EXPIRATION_DATE.key(),
                        user.getInternal().getAccount().getExpirationDate());
                dbAdaptorFactory.getCatalogUserDBAdaptor(organizationId).update(userId, userUpdateParams);

                throw new CatalogException("Could not update secret key.");
            }

            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_FEDERATION_SECRET_KEY, Enums.Resource.ORGANIZATION,
                    organizationId, "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(result.getTime(), Collections.singletonList(newSecretKey));
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_FEDERATION_SECRET_KEY, Enums.Resource.ORGANIZATION,
                    organizationId, "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw new CatalogException(e);
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
            validateFederationServerParams(federationClient);

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

            // Query and update new secret key in the database
            String updatedSecretKey = getUpdatedSecretKey(client);
            FederationClientParams clientParams = new FederationClientParams()
                    .setSecretKey(updatedSecretKey)
                    .setToken("");
            result = updateFederationClient(organizationId, federationClient.getId(), clientParams);
            if (result.getNumUpdated() == 0) {
                throw new CatalogException("Could not update secret key. Please, talk to the federation server administrator as the " +
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

            // Obtain the federation server credentials
            QueryOptions orgOptions = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.FEDERATION.key());
            Organization organization = EnterpriseFactory.getCatalogDBAdaptorFactory().getCatalogOrganizationDBAdaptor(organizationId)
                    .get(orgOptions).first();

            if (organization.getFederation() == null || CollectionUtils.isEmpty(organization.getFederation().getClients())) {
                throw new CatalogException("The organization does not have any federation clients configured.");
            }
            FederationClientParams federationClient = null;
            for (FederationClientParams client : organization.getFederation().getClients()) {
                if (client.getId().equals(federationId)) {
                    federationClient = client;
                    break;
                }
            }
            if (federationClient == null) {
                throw new CatalogException("Federation client id '" + federationId + "' not found in the organization.");
            }

            GenericClient client = FederationUtils.getClientInstance(federationClient);
            List<Project> federatedProjects = obtainRemoteProjectsAndStudies(client);

            // Obtain shared projects/studies locally
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    ProjectDBAdaptor.QueryParams.ID.key(), ProjectDBAdaptor.QueryParams.FQN.key(), ProjectDBAdaptor.QueryParams.UID.key(),
                    ProjectDBAdaptor.QueryParams.STUDIES.key() + "." + StudyDBAdaptor.QueryParams.ID.key(),
                    ProjectDBAdaptor.QueryParams.STUDIES.key() + "." + StudyDBAdaptor.QueryParams.FQN.key(),
                    ProjectDBAdaptor.QueryParams.STUDIES.key() + "." + StudyDBAdaptor.QueryParams.UID.key()));
            List<Project> projects = catalogManager.getProjectManager().search(organizationId, new Query(), options, token).getResults();

            // Filter out projects and studies that are not from federated servers (that are local)
            projects.removeIf(project -> project.getFqn().startsWith(organizationId + "@"));

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
                    newStudies.put(localProject, studyList);
                }
            }

            // Remove old federated projects/studies
            removeFederatedProjects(organizationId, projectsToRemove);
            removeFederatedStudies(organizationId, studiesToRemove);

            // Import new federated projects/studies
            importFederatedProjects(federationId, organizationId, newProjects);
            for (Map.Entry<Project, List<Study>> entry : newStudies.entrySet()) {
                Project project = entry.getKey();
                List<Study> studyList = entry.getValue();
                importFederatedStudies(federationId, organization, project, studyList);
            }

            auditManager.audit(organizationId, userId, Enums.Action.EXPOSE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return new OpenCGAResult<>(0, Collections.emptyList());
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.EXPOSE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
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
            String federationId = FederationUtils.getFederationServerId(project, study, tokenPayload);

            // Obtain the federation server credentials
            QueryOptions orgOptions = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.FEDERATION.key());
            Organization organization = EnterpriseFactory.getCatalogDBAdaptorFactory().getCatalogOrganizationDBAdaptor(organizationId)
                    .get(orgOptions).first();
            if (organization.getFederation() == null || CollectionUtils.isEmpty(organization.getFederation().getClients())) {
                throw new CatalogException("Organization does not have any federation server configured.");
            }
            FederationClientParams federationClient = null;
            for (FederationClientParams client : organization.getFederation().getClients()) {
                if (client.getId().equals(federationId)) {
                    federationClient = client;
                    break;
                }
            }
            if (federationClient == null) {
                throw new CatalogException("Federation server id '" + federationId + "' not found in the organization.");
            }

            String federationToken = federationClient.getToken();
            // The call to getClientInstance will update the token if it has expired
            GenericClient client = FederationUtils.getClientInstance(federationClient);
            if (federationClient.getToken().equals(federationToken)) {
                LoginParams loginParams = new LoginParams(federationClient.getOrganizationId(), federationClient.getUserId(),
                        federationClient.getPassword());
                federationToken = client.login(loginParams).firstResult().getToken();
                client.setToken(federationToken);

                // Store token in database
                FederationClientParams clientParams = new FederationClientParams()
                        .setToken(federationToken);
                OpenCGAResult<Organization> result = updateFederationClient(organizationId, federationClient.getId(), clientParams);
                if (result.getNumUpdated() == 0) {
                    throw new CatalogException("Could not update token to communicate with the federation server.");
                }

                logger.debug("Token has been updated.");
            }

            ObjectMap aboutMap = client.about().firstResult();
            String version = aboutMap.getString("Version");
            if (StringUtils.isNotEmpty(version) && !version.equals(federationClient.getVersion())) {
                logger.warn("Calling to federation server '{}'. The version of the federation server has changed from '{}' to '{}'.",
                        federationClient.getUrl(), federationClient.getVersion(), version);

                // Store new version in database
                FederationClientParams clientParams = new FederationClientParams()
                        .setVersion(version);
                OpenCGAResult<Organization> result = updateFederationClient(organizationId, federationClient.getId(), clientParams);
                if (result.getNumUpdated() == 0) {
                    throw new CatalogException("Could not update new OpenCGA version from the federation server.");
                }
            }

            Matcher matcher = URL_REDIRECT.matcher(url);
            if (!matcher.find()) {
                throw new CatalogException("The url '" + url + "' does not seem to follow the OpenCGA URL pattern.");
            }
            String group1 = matcher.group(1);
            String group2 = matcher.group(2);
            return client.execute(group1, group2, queryParams, body, method, Object.class);


//            auditManager.audit(organizationId, userId, Enums.Action.EXPOSE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
//                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (Exception e) {
//            auditManager.audit(organizationId, userId, Enums.Action.EXPOSE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
//                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw new CatalogException(e);
        }
    }

    private OpenCGAResult<Organization> updateFederationClient(String organizationId, String federationId,
                                                               FederationClientParams federationClientParams) throws CatalogException {
        ObjectMap parameters;
        try {
            parameters = new ObjectMap(JacksonUtils.getUpdateObjectMapper().writeValueAsString(federationClientParams));
        } catch (JsonProcessingException e) {
            throw new CatalogException(e);
        }
        return dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).updateFederationClientParams(federationId, parameters);
    }

    private List<Project> obtainRemoteProjectsAndStudies(GenericClient client) throws ClientException {
        return client.execute("projects", null, null, null, "search", Collections.emptyMap(), ParentClient.GET, Project.class).allResults();
    }

    private String getUpdatedSecretKey(GenericClient client) throws ClientException {
        return client.execute("federations", null, null, null, "firstConnection", Collections.emptyMap(), ParentClient.POST, String.class)
                .firstResult();
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
            dbAdaptorFactory.getCatalogStudyDBAdaptor(organization.getId()).insert(project, study, Collections.emptyList(),
                    QueryOptions.empty());
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

    private void validateFederationServerParams(FederationClientParams federationClient) throws CatalogParameterException {
        // Validate mandatory fields
        ParamUtils.checkIdentifier(federationClient.getId(), "id");
        ParamUtils.checkEmail(federationClient.getEmail());
        ParamUtils.checkParameter(federationClient.getUrl(), "url");
        ParamUtils.checkParameter(federationClient.getOrganizationId(), "organizationId");
        ParamUtils.checkParameter(federationClient.getUserId(), "userId");
        ParamUtils.checkParameter(federationClient.getPassword(), "password");
        ParamUtils.checkParameter(federationClient.getSecretKey(), "secretKey");

        federationClient.setDescription(StringUtils.isNotEmpty(federationClient.getDescription()) ? federationClient.getDescription() : "");
    }

    private FederationServerParams generateFederationServer(FederationServerCreateParams federationServerCreateParams)
            throws CatalogParameterException {
        // Validate mandatory fields
        ParamUtils.checkIdentifier(federationServerCreateParams.getId(), "id");
        ParamUtils.checkEmail(federationServerCreateParams.getEmail());

        String userId = StringUtils.isNotEmpty(federationServerCreateParams.getUserId())
                ? federationServerCreateParams.getUserId()
                : federationServerCreateParams.getId() + ".user1";
        // Create 24h expiration time and a random secret key
        String expTime = TimeUtils.getTime(TimeUtils.add24HtoDate(TimeUtils.getDate()));
        String secretKey = PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH);

        // Create the client
        return new FederationServerParams(federationServerCreateParams.getId(),
                StringUtils.isNotEmpty(federationServerCreateParams.getDescription()) ? federationServerCreateParams.getDescription() : "",
                federationServerCreateParams.getEmail(), userId, true, expTime, secretKey);
    }
}
