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

import org.apache.commons.collections.CollectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.demo.config.DemoConfiguration;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualCreateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleCreateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyCreateParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserCreateParams;


public class DemoManager {

    private DemoConfiguration demoConfiguration;
    private OpenCGAClient openCGAClient;

    public DemoManager() {
    }

    public DemoManager(DemoConfiguration demoConfiguration, ClientConfiguration clientConfiguration) {
        this.demoConfiguration = demoConfiguration;
        this.openCGAClient = new OpenCGAClient(clientConfiguration);
    }

    public void execute() throws ClientException {
        User mainUser = null;
        String password = demoConfiguration.getConfiguration().getPassword();
        openCGAClient.login("opencga", password);
        for (User user : demoConfiguration.getUsers()) {
            if (CollectionUtils.isNotEmpty(user.getProjects())) {
                mainUser = user;
            }
            openCGAClient.getUserClient().create(UserCreateParams.of(user));
        }
        openCGAClient.logout();

        openCGAClient.login(mainUser.getId(), mainUser.getPassword());
        for (Project project : mainUser.getProjects()) {
            openCGAClient.getProjectClient().create(ProjectCreateParams.of(project));
            for (Study study : project.getStudies()) {
                ObjectMap params = new ObjectMap("project", project.getId());
                openCGAClient.getStudyClient().create(StudyCreateParams.of(study), params);
                if (CollectionUtils.isNotEmpty(study.getIndividuals())) {
                    this.createIndividuals(study);
                }
                if (CollectionUtils.isNotEmpty(study.getSamples())) {
                    this.createSamples(study);
                }
                if (CollectionUtils.isNotEmpty(study.getCohorts())) {
                    this.createCohorts(study);
                }
            }
        }
    }

    private void createIndividuals(Study study) throws ClientException {
        ObjectMap params = new ObjectMap("study", study.getId());
        for (Individual individual : study.getIndividuals()) {
            openCGAClient.getIndividualClient().create(IndividualCreateParams.of(individual), params);
        }
    }

    private void createSamples(Study study) throws ClientException {
        ObjectMap params = new ObjectMap("study", study.getId());
        for (Sample sample : study.getSamples()) {
            openCGAClient.getSampleClient().create(SampleCreateParams.of(sample), params);
        }
    }

    private void createCohorts(Study study) throws ClientException {
        ObjectMap params = new ObjectMap("study", study.getId());
        for (Cohort cohort : study.getCohorts()) {
            openCGAClient.getCohortClient().create(CohortCreateParams.of(cohort), params);
        }
    }

    private void fetchFiles(Study study) throws ClientException {
        ObjectMap params = new ObjectMap("study", study.getId());
        for (File file : study.getFiles()) {
            params.put("path", file.getPath());
            params.put("url", file.getUri());
            openCGAClient.getFileClient().fetch(params);
        }
    }

}
