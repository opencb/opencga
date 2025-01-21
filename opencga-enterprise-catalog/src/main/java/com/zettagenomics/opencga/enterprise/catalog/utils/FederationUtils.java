package com.zettagenomics.opencga.enterprise.catalog.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.core.client.GenericClient;
import org.opencb.opencga.core.common.JwtUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.federation.FederationClientParams;
import org.opencb.opencga.core.models.federation.FederationServerParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyInternal;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.LoginParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;

import javax.crypto.SecretKey;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FederationUtils {

    private static final Pattern STUDY_PATTERN = Pattern.compile(".*/studies/([^/]+)/info$");
    private static final Pattern PROJECT_PATTERN = Pattern.compile(".*/projects/([^/]+)/info$");

    public FederationUtils() {
    }

    public static String extractProject(String url, Map<String, Object> queryParams) {
        String project = (String) queryParams.get("project");
        if (StringUtils.isNotEmpty(project)) {
            return project;
        }
        Matcher matcher = PROJECT_PATTERN.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    public static String extractStudy(String url, Map<String, Object> queryParams) {
        String study = (String) queryParams.get("study");
        if (StringUtils.isNotEmpty(study)) {
            return study;
        }
        Matcher matcher = STUDY_PATTERN.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * Given a project or study id, it will look for the corresponding federation server id in the token payload.
     *
     * @param project Project FQN or UUID.
     * @param study   Study FQN or UUID.
     * @param tokenPayload Token payload.
     * @return Federation server id.
     * @throws CatalogException If the user does not belong to any federation that contains the project or study provided.
     */
    public static String findFederationServerIdInPayload(String project, String study, JwtPayload tokenPayload)
            throws CatalogException {
        if (StringUtils.isEmpty(study) && StringUtils.isEmpty(project)) {
            throw new CatalogParameterException("Missing project or study from the query parameters");
        } else if (StringUtils.isNotEmpty(study)) {
            for (JwtPayload.FederationJwtPayload federation : tokenPayload.getFederations()) {
                if (federation.getStudyIds().contains(study)) {
                    return federation.getId();
                }
            }
        } else if (StringUtils.isNotEmpty(project)) {
            for (JwtPayload.FederationJwtPayload federation : tokenPayload.getFederations()) {
                if (federation.getProjectIds().contains(project)) {
                    return federation.getId();
                }
            }
        }
        throw new CatalogException("User does not belong to any federation that contains the project or study provided.");
    }

    /**
     * Given an organization and a user, it will return the FederationServerParams object that contains the user.
     * @param organizationId Organization id
     * @param userId User id
     * @param dbAdaptorFactory DBAdaptorFactory
     * @return FederationServerParams object
     * @throws CatalogException If the organization is not federated, the user is not federated in the organization or the organization
     * does not exist.
     */
    public static FederationServerParams findFederationServerForUser(String organizationId, String userId,
                                                                     DBAdaptorFactory dbAdaptorFactory) throws CatalogException {
        QueryOptions orgOptions = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.FEDERATION.key());
        OpenCGAResult<Organization> result = dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(orgOptions);
        if (result.getNumResults() == 0) {
            throw new CatalogException("Could not find Organization '" + organizationId + "'");
        }
        Organization organization = result.first();
        if (organization.getFederation() == null || CollectionUtils.isEmpty(organization.getFederation().getServers())) {
            throw new CatalogException("Organization '" + organizationId + "' is not federated.");
        }
        for (FederationServerParams server : organization.getFederation().getServers()) {
            if (server.getUserId().equals(userId)) {
                return server;
            }
        }
        throw new CatalogException("User '" + userId + "' is not federated in organization '" + organizationId + "'");
    }

    /**
     * Given a FederationClientParams object, it will return a GenericClient object with the token set.
     * @param federationClient FederationClientParams object
     * @return GenericClient object
     * @throws ClientException If there is an error logging in.
     */
    public static GenericClient getClientInstance(FederationClientParams federationClient) throws ClientException {
        ClientConfiguration clientConfiguration = new ClientConfiguration(federationClient.getUrl());

        if (StringUtils.isNotEmpty(federationClient.getToken())) {
            // Check the expiration date is still valid
            JwtPayload jwtPayload = new JwtPayload(federationClient.getToken());
            Date expirationTime = jwtPayload.getExpirationTime();
            if (expirationTime.before(TimeUtils.getDate())) {
                // Clear token
                federationClient.setToken(null);
            }
        }
        GenericClient client = new GenericClient(federationClient.getToken(), clientConfiguration);
        if (StringUtils.isEmpty(federationClient.getToken())) {
            LoginParams loginParams = new LoginParams(federationClient.getOrganizationId(), federationClient.getUserId(),
                    federationClient.getPassword());
            RestResponse<AuthenticationResponse> login = client.login(loginParams);

            if (CollectionUtils.isNotEmpty(login.getEvents())) {
                throw new ClientException("Error logging in: " + login.getEvents().get(0).getMessage());
            }

            // Cypher token using the security key provided by the server
            JwtUtils.Token token = JwtUtils.getToken(login.firstResult().getToken());
            SecretKey secretKey = CryptoUtils.stringToSecretKey(federationClient.getSecurityKey());
            String encryptedSignature;
            try {
                encryptedSignature = CryptoUtils.encrypt(token.getVerifySignature(), secretKey);
            } catch (Exception e) {
                throw new ClientException("Failed to encrypt the token for subsequent queries", e);
            }
            String finalToken = JwtUtils.generateToken(token.getHeader(), token.getPayload(), encryptedSignature);

            // Set token in the client object to be used in next call
            client.setToken(finalToken);
            // Set token in the federationClient object
            federationClient.setToken(finalToken);
        }
        return client;
    }

    public static void removeStudyFieldsForStorage(Organization organization, Study study) {
        study.setPermissionRules(Collections.emptyMap());
        study.setNotes(Collections.emptyList());
        study.setVariableSets(Collections.emptyList());
        study.setInternal(StudyInternal.init());
        study.setAttributes(Collections.emptyMap());
        study.setUri(null);

        // Set groups
        Set<String> users = new HashSet<>();
        users.add(organization.getOwner());
        users.addAll(organization.getAdmins());
        List<Group> groups = Arrays.asList(
                new Group(StudyManager.MEMBERS, new ArrayList<>(users)),
                new Group(StudyManager.ADMINS, new ArrayList<>(users))
        );
        study.setGroups(groups);
    }


}
