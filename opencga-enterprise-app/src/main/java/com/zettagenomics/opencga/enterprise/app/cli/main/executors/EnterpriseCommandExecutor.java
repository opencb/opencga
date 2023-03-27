package com.zettagenomics.opencga.enterprise.app.cli.main.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class EnterpriseCommandExecutor extends OpencgaCommandExecutor {

    public EnterpriseCommandExecutor(GeneralCliOptions.CommonCommandOptions options) throws CatalogAuthenticationException {
        super(options);
    }

    @Override
    public RestResponse<AuthenticationResponse> saveSession(String user, AuthenticationResponse response) throws ClientException, IOException {
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
            this.sessionManager.saveSession(user, response.getToken(), response.getRefreshToken(), studies, this.host);
            res.setType(QueryType.VOID);
        }
        return res;
    }
}
