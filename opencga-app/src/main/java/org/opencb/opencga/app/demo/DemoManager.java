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
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.demo.config.DemoConfiguration;
import org.opencb.opencga.app.demo.config.StudyConfiguration;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyCreateParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualCreateParams;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleCreateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyCreateParams;

import java.util.*;


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
        for (Project project : demoConfiguration.getProjects()) {
            openCGAClient.getProjectClient().create(ProjectCreateParams.of(project));
            for (Study study : project.getStudies()) {
                ObjectMap params = new ObjectMap("project", project.getId());
                openCGAClient.getStudyClient().create(StudyCreateParams.of(study), params);
                if (CollectionUtils.isNotEmpty(study.getIndividuals())) {
                    createIndividuals(study);
                }
                if (CollectionUtils.isNotEmpty(study.getSamples())) {
                    createSamples(study);
                }
                if (CollectionUtils.isNotEmpty(study.getCohorts())) {
                    createCohorts(study);
                }
                if (CollectionUtils.isNotEmpty(study.getFamilies())) {
                    createFamilies(study);
                }
                if (CollectionUtils.isNotEmpty(study.getFiles())) {
                    List<String> fetchJobIds = fetchFiles(study);
                    List<String> indexJobIds = index(study, fetchJobIds);
                }
            }
        }
    }

    private void createIndividuals(Study study) throws ClientException {
        ObjectMap params = new ObjectMap("study", study.getId());
        Map<String, Map<String, Object>> relatives = new HashMap<>();

        // Create individuals without parents and siblings
        for (Individual individual : study.getIndividuals()) {
            relatives.put(individual.getId(), new HashMap<String, Object>(){{
                put("father", individual.getFather());
                put("mother", individual.getMother());
                put("multiples", individual.getMultiples());
            }});
            individual.setFather(null);
            individual.setMother(null);
            individual.setMultiples(null);
            openCGAClient.getIndividualClient().create(IndividualCreateParams.of(individual), params);
        }

        // Update parents and siblings for each individual
        for (Individual individual : study.getIndividuals()) {
            IndividualUpdateParams updateParams = new IndividualUpdateParams();
            Individual father = (Individual) relatives.get(individual.getId()).get("father");
            updateParams.setFather(father.getId());
            Individual mother = (Individual) relatives.get(individual.getId()).get("mother");
            updateParams.setMother(mother.getId());
            updateParams.setMultiples((Multiples) relatives.get(individual.getId()).get("multiples"));
            openCGAClient.getIndividualClient().update(individual.getId(), updateParams, params);
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

    private void createFamilies(Study study) throws ClientException {
        ObjectMap params = new ObjectMap("study", study.getId());
        for (Family family : study.getFamilies()) {
            openCGAClient.getFamilyClient().create(FamilyCreateParams.of(family), params);
        }
    }

    private List<String> fetchFiles(Study study) throws ClientException {
        ObjectMap params = new ObjectMap("study", study.getId());
        String urlBase = null;
        for (StudyConfiguration studyConfiguration : demoConfiguration.getConfiguration().getStudies()) {
            if (studyConfiguration.getId().equals(study.getId())) {
                urlBase = studyConfiguration.getUrlBase();
            }
        }

        if (urlBase == null) {
            throw new java.lang.NullPointerException();
        } else if (!urlBase.endsWith("/")) {
            urlBase = urlBase + "/";
        }

        List<String> fetchJobIds = new ArrayList<>();
        for (File file : study.getFiles()) {
            params.put("url", urlBase + "/" + file.getId());
            fetchJobIds.add(openCGAClient.getFileClient().fetch(params).getResponses().get(0).getResults().get(0).getId());
        }
        return fetchJobIds;
    }

    private List<String> index(Study study, List<String> fetchJobIds) throws ClientException {
        ObjectMap params = new ObjectMap("study", study.getId());
        List<String> indexJobIds = new ArrayList<>();
        for (String jobId : fetchJobIds) {
            params.put("jobDependsOn", jobId);
            indexJobIds.add(openCGAClient.getVariantClient().index(null, params).getResponses().get(0).getResults().get(0).getId());
        }
        return indexJobIds;
    }

}
