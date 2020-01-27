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

package org.opencb.opencga.app.demo;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.demo.config.DemoConfiguration;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyCreateParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserCreateParams;

import java.util.ArrayList;
import java.util.List;

public class DemoManager {

    private DemoConfiguration demoConfiguration;
    private OpenCGAClient openCGAClient;

    public DemoManager(DemoConfiguration demoConfiguration, ClientConfiguration clientConfiguration) {
        this.demoConfiguration = demoConfiguration;
        this.openCGAClient = new OpenCGAClient(clientConfiguration);
    }

    public void load() throws ClientException {
        List<String> projects = new ArrayList<>();
        List<String> studies = new ArrayList<>();
        List<String> users = new ArrayList<>();
        for (User user : demoConfiguration.getUsers()) {
            for (Project project : user.getProjects()) {
                if(!projects.contains(project.getId())) {
                    projects.add(project.getId());
                    openCGAClient.getProjectClient().create(ProjectCreateParams.of(project));
                }
                for (Study study : project.getStudies()) {
                    if(!studies.contains(study.getId())) {
                        studies.add(study.getId());
                        openCGAClient.getStudyClient().create(
                                StudyCreateParams.of(study),
                                (ObjectMap) new ObjectMap().put("project", project.getId())
                        );
                    }
                }
            }
        }
        for (User user : demoConfiguration.getUsers()) {
            if (!users.contains(user.getId())) {
                users.add(user.getId());
                openCGAClient.getUserClient().create(UserCreateParams.of(user));
            }
        }
    }



    public void execute() throws ClientException {
        this.load();
    }

}
