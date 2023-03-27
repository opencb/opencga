package com.zettagenomics.opencga.enterprise.app.cli.session;

import com.zettagenomics.opencga.enterprise.client.rest.EnterpriseOpenCGAClient;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EnterpriseSessionManager extends SessionManager {

    private Logger logger;
    private final String host;

    public EnterpriseSessionManager(ClientConfiguration clientConfiguration) throws ClientException {
        this(clientConfiguration, clientConfiguration.getCurrentHost().getName());
    }

    public EnterpriseSessionManager(ClientConfiguration clientConfiguration, String host) {
        super(clientConfiguration, host);
        this.host = host;
        this.logger = LoggerFactory.getLogger(EnterpriseSessionManager.class);
    }

    public RestResponse<AuthenticationResponse> saveSession(String user, AuthenticationResponse response,
                                                            EnterpriseOpenCGAClient openCGAClient,
                                                            Map<String, Object> attributes)
            throws ClientException, IOException {
        RestResponse<AuthenticationResponse> res = new RestResponse<>();
        if (response != null) {
            List<String> studies = new ArrayList<>();
            logger.debug(response.toString());
            RestResponse<Project> projects = openCGAClient.getProjectClient().search(
                    new ObjectMap(ProjectDBAdaptor.QueryParams.OWNER.key(), user));

            if (projects.getResponses().get(0).getNumResults() == 0) {
                // We try to fetch shared projects and studies instead when the user does not own any project or study
                projects = openCGAClient.getProjectClient().search(new ObjectMap());
            }

            for (Project project : projects.getResponses().get(0).getResults()) {
                for (Study study : project.getStudies()) {
                    studies.add(study.getFqn());
                }
            }
            this.saveSession(user, response.getToken(), response.getRefreshToken(), studies, this.host, attributes);
            res.setType(QueryType.VOID);
        }
        return res;
    }
}
