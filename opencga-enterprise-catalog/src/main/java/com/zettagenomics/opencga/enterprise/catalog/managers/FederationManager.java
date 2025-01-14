package com.zettagenomics.opencga.enterprise.catalog.managers;

import com.zettagenomics.opencga.enterprise.catalog.utils.FederationUtils;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.client.GenericClient;
import org.opencb.opencga.core.client.ParentClient;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.federation.FederationClient;
import org.opencb.opencga.core.models.federation.FederationClientRef;
import org.opencb.opencga.core.models.federation.FederationServer;
import org.opencb.opencga.core.models.federation.FederationServerCreateParams;
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

import javax.ws.rs.core.Response;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FederationManager extends AbstractManager {

    protected static Logger logger = LoggerFactory.getLogger(FederationManager.class);

    private final static Pattern URL_REDIRECT = Pattern.compile(".*/webservices/rest/v\\d+/([^/]+)/(.*)");

    public FederationManager(CatalogManager catalogManager, EnterpriseConfiguration enterpriseConfiguration) {
        super(catalogManager, enterpriseConfiguration);
    }

    public OpenCGAResult<FederationClient> createFederation(FederationServerCreateParams federationServerCreateParams,
                                                            String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String userId = tokenPayload.getUserId();

        ObjectMap auditParams = new ObjectMap()
                .append("server", federationServerCreateParams)
                .append("token", token);
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);

            FederationServer federationServer = generateFederationServer(federationServerCreateParams);
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
            User user = new User()
                    .setId(federationServer.getUserId())
                    .setInternal(new UserInternal().setAccount(new Account().setAuthentication(authenticationOrigin)));
            String password = PasswordUtils.getStrongRandomPassword();
            catalogManager.getUserManager().create(user, password, token);

            // Send email with all the information (url, user id, password, secret key) to the email provided
            // TODO: Send mail


            auditManager.audit(organizationId, userId, Enums.Action.EXPOSE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            FederationClient federationClient = new FederationClient()
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

    public void connect(FederationClient federationClient, String token) throws CatalogException {
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
            List<Project> projectList = obtainRemoteProjectsAndStudies(federationClient);
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

            auditManager.audit(organizationId, userId, Enums.Action.CREATE_FEDERATION_CLIENT, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.CREATE_FEDERATION_CLIENT, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
        }
    }

    public void sync(String federationId, String token) throws CatalogException {
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
            FederationClient federationClient = null;
            for (FederationClient client : organization.getFederation().getClients()) {
                if (client.getId().equals(federationId)) {
                    federationClient = client;
                    break;
                }
            }

            List<Project> federatedProjects = obtainRemoteProjectsAndStudies(federationClient);

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
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.EXPOSE_FEDERATION_SERVER, Enums.Resource.ORGANIZATION, organizationId,
                    "", "", "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e));
            throw e;
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
            FederationClient federationClient = null;
            for (FederationClient client : organization.getFederation().getClients()) {
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
                logger.debug("Token has been updated.");
                // TODO If the token has been updated, update the token stored in the database
            }

            ObjectMap aboutMap = client.about().firstResult();
            String version = aboutMap.getString("Version");
            if (StringUtils.isNotEmpty(version) && !version.equals(federationClient.getVersion())) {
                logger.warn("Calling to federation server '{}'. The version of the federation server has changed from '{}' to '{}'.",
                        federationClient.getUrl(), federationClient.getVersion(), version);
                // TODO: Update version in the database
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

    private List<Project> obtainRemoteProjectsAndStudies(FederationClient federationClient) throws CatalogException {
        try {
            GenericClient client = FederationUtils.getClientInstance(federationClient);
            return client.execute("projects", null, null, null, "search", Collections.emptyMap(), ParentClient.GET, Project.class)
                    .allResults();
        } catch (ClientException e) {
            throw new CatalogException("Could not connect to the federation server. Please, check the provided information.", e);
        }
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
            project.setFederation(new FederationClientRef(federationId));
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
            study.setFederation(new FederationClientRef(federationId));
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



    private void validateFederationServerParams(FederationClient federationClient) throws CatalogParameterException {
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

    private FederationServer generateFederationServer(FederationServerCreateParams federationServerCreateParams)
            throws CatalogParameterException {
        // Validate mandatory fields
        ParamUtils.checkIdentifier(federationServerCreateParams.getId(), "id");
        ParamUtils.checkEmail(federationServerCreateParams.getEmail());

        String userId = StringUtils.isNotEmpty(federationServerCreateParams.getUserId())
                ? federationServerCreateParams.getUserId()
                : federationServerCreateParams.getId() + ".user1";
        // Create 24h expiration time and a random secret key
        String expTime = TimeUtils.getTime(TimeUtils.add24HtoDate(TimeUtils.getDate()));
        String secretKey = PasswordUtils.getStrongRandomPassword(15);

        // Create the client
        return new FederationServer(federationServerCreateParams.getId(),
                StringUtils.isNotEmpty(federationServerCreateParams.getDescription()) ? federationServerCreateParams.getDescription() : "",
                federationServerCreateParams.getEmail(), userId, true, expTime, secretKey);
    }
}
