/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencb.opencga.app.cli.main;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.UsersCommandOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 02/03/15.
 */
public abstract class ParentUsersCommandExecutor extends OpencgaCommandExecutor {
    // TODO: Add include/exclude/skip/... (queryOptions) to the client calls !!!!

    private UsersCommandOptions usersCommandOptions;

    public ParentUsersCommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean command,
                                      UsersCommandOptions usersCommandOptions) {

        super(options, command);
        this.usersCommandOptions = usersCommandOptions;
    }

    protected RestResponse<AuthenticationResponse> login() throws Exception {
        logger.debug("Login");

        String user = usersCommandOptions.loginCommandOptions.user;
        String password = usersCommandOptions.loginCommandOptions.password;

        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            AuthenticationResponse response = openCGAClient.login(user, password);
            if (response != null) {
                List<String> studies = new ArrayList<>();

                RestResponse<Project> projects = openCGAClient.getProjectClient().search(
                        new ObjectMap(ProjectDBAdaptor.QueryParams.OWNER.key(), user));

                if (projects.getResponses().get(0).getNumResults() == 0) {
                    // We try to fetch shared projects and studies instead when the user does not owe any project or study
                    projects = openCGAClient.getProjectClient().search(new ObjectMap());
                }
                for (Project project : projects.getResponses().get(0).getResults()) {
                    for (Study study : project.getStudies()) {
                        studies.add(study.getFqn());
                    }
                }
                // write CLI session file
                saveCliSessionFile(user, response.getToken(), response.getRefreshToken(), studies);
                System.out.println("You have been logged in correctly.");
            }
        } else {
            String sessionId = usersCommandOptions.commonCommandOptions.token;
            String errorMsg = "Missing password. ";
            if (StringUtils.isNotEmpty(sessionId)) {
                errorMsg += "Active token detected ";
            }
            System.err.println(errorMsg);
        }
        RestResponse<AuthenticationResponse> res = new RestResponse();
        return res;
    }

    protected void logout() throws IOException {
        logger.debug("Logout");
        openCGAClient.logout();
        logoutCliSessionFile();
    }
}
