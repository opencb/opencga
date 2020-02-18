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

package org.opencb.opencga.client.template;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.client.template.config.TemplateConfiguration;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyCreateParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileFetch;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualCreateParams;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationIndexParams;
import org.opencb.opencga.core.models.operations.variant.VariantSecondaryIndexParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleCreateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyCreateParams;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.opencb.opencga.core.models.variant.VariantStatsAnalysisParams;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class TemplateManager {

    private TemplateConfiguration templateConfiguration;
    private OpenCGAClient openCGAClient;

    private Logger logger;

    public TemplateManager() {
    }

    public TemplateManager(TemplateConfiguration templateConfiguration, ClientConfiguration clientConfiguration, String token) {
        this.templateConfiguration = templateConfiguration;
        this.openCGAClient = new OpenCGAClient(token, clientConfiguration);

        this.logger = LoggerFactory.getLogger(TemplateManager.class);
    }

    public void execute() throws ClientException {
        // TODO Check version

        // Check if any study exists before we start, if a study exists we should fail. Projects are allowed to exist.
        for (Project project : templateConfiguration.getProjects()) {
            for (Study study : project.getStudies()) {
                RestResponse<Study> infoResponse = openCGAClient.getStudyClient()
                        .info(project.getId() + ":" + study.getId(), new ObjectMap());
                if (infoResponse.getResponses().size() > 0) {
                    logger.error("Study already exists");
                    return;
                }
            }
        }

        // Create and load data
        for (Project project : templateConfiguration.getProjects()) {
            if (openCGAClient.getProjectClient().info(project.getId(), new ObjectMap()).first().getNumResults() == 0) {
                logger.info("Creating project '{}'", project.getId());
                openCGAClient.getProjectClient().create(ProjectCreateParams.of(project));
            } else {
                logger.warn("Project '{}' already exists.", project.getId());
            }

            List<String> indexVcfJobIds = null;
            for (Study study : project.getStudies()) {
                ObjectMap params = new ObjectMap(ParamConstants.PROJECT_PARAM, project.getId());
                openCGAClient.getStudyClient().create(StudyCreateParams.of(study), params);
                // NOTE: Do not change the order of the following resource creation.
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
                    indexVcfJobIds = fetchFiles(study);
                }
            }
            if (CollectionUtils.isNotEmpty(indexVcfJobIds)) {
                postIndex(project, indexVcfJobIds);
            }
        }
    }

    private void createIndividuals(Study study) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
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
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
        for (Sample sample : study.getSamples()) {
            openCGAClient.getSampleClient().create(SampleCreateParams.of(sample), params);
        }
    }

    private void createCohorts(Study study) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
        for (Cohort cohort : study.getCohorts()) {
            openCGAClient.getCohortClient().create(CohortCreateParams.of(cohort), params);
        }
    }

    private void createFamilies(Study study) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
        for (Family family : study.getFamilies()) {
            openCGAClient.getFamilyClient().create(FamilyCreateParams.of(family), params);
        }
    }

    private List<String> fetchFiles(Study study) throws ClientException {
        String baseUrl = this.templateConfiguration.getBaseUrl();
        baseUrl = baseUrl.replaceAll("STUDY_ID", study.getId());
        if (!baseUrl.endsWith("/")) {
            baseUrl = baseUrl + "/";
        }

        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
        List<String> indexVcfJobIds = new ArrayList<>();
        for (File file : study.getFiles()) {
            FileFetch fileFetch = new FileFetch(baseUrl + file.getName(), file.getPath());
            String fetchJobId = openCGAClient.getFileClient().fetch(fileFetch, params).getResponses().get(0).getResults().get(0).getId();
            if (templateConfiguration.isIndex()) {
                if (isVcf(file)) {
                    indexVcfJobIds.add(indexVcf(study, file.getId(), Collections.singletonList(fetchJobId)));
                }
            }
        }
        return indexVcfJobIds;
    }

    private boolean isVcf(File file) {
        return file.getId().endsWith(".vcf.gz") || file.getId().endsWith(".vcf");
    }

    private String indexVcf(Study study, String file, List<String> jobDependsOn) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
        params.put("jobDependsOn", jobDependsOn);
        return openCGAClient.getVariantClient()
                .index(new VariantIndexParams().setFile(file), params)
                .getResponses().get(0).getResults().get(0).getId();
    }

    private void postIndex(Project project, List<String> indexVcfJobIds) throws ClientException {
        List<String> jobs = new ArrayList<>();
        jobs.add(variantAnnot(project, indexVcfJobIds));
        for (Study study : project.getStudies()) {
            if (study.getFiles().stream().anyMatch(this::isVcf)) {
                jobs.add(variantStats(study, indexVcfJobIds));
            }
        }
        variantSecondaryIndex(project, jobs);
    }

    private String variantAnnot(Project project, List<String> indexVcfJobIds) throws ClientException {
        List<String> studies = project.getStudies().stream()
                .filter(study -> study.getFiles()
                        .stream()
                        .anyMatch(this::isVcf))
                .map(Study::getFqn)
                .collect(Collectors.toList());

        ObjectMap params = new ObjectMap()
                .append(ParamConstants.PROJECT_PARAM, project.getId())
                .append(ParamConstants.STUDY_PARAM, studies)
                .append("jobDependsOn", indexVcfJobIds);
        return openCGAClient.getVariantOperationClient()
                .indexVariantAnnotation(new VariantAnnotationIndexParams(), params)
                .getResponses().get(0).getResults().get(0).getId();
    }

    private String variantStats(Study study, List<String> indexVcfJobIds) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId())
                .append("jobDependsOn", indexVcfJobIds);
        
        VariantStatsAnalysisParams data = new VariantStatsAnalysisParams();
        data.setAggregated(Aggregation.NONE);
        for (File file : study.getFiles()) {
            if (file.getId().endsWith(".properties")
                    && file.getAttributes() != null
                    && Boolean.parseBoolean(String.valueOf(file.getAttributes().get("aggregationMappingFile")))) {
                data.setAggregationMappingFile(file.getId());
                data.setAggregated(Aggregation.BASIC);
            }
        }

        if (study.getAttributes() != null) {
            Object aggregationObj = study.getAttributes().get("aggregation");
            if (aggregationObj != null) {
                data.setAggregated(AggregationUtils.valueOf(aggregationObj.toString()));
            }
        }

        return openCGAClient.getVariantClient()
                .runStats(data, params)
                .getResponses().get(0).getResults().get(0).getId();
    }

    private void variantSecondaryIndex(Project project, List<String> jobs) throws ClientException {
        List<String> studies = project.getStudies().stream()
                .filter(study -> study.getFiles()
                        .stream()
                        .anyMatch(this::isVcf))
                .map(Study::getFqn)
                .collect(Collectors.toList());

        ObjectMap params = new ObjectMap()
                .append(ParamConstants.PROJECT_PARAM, project.getId())
                .append(ParamConstants.STUDY_PARAM, studies)
                .append("jobDependsOn", jobs);
        openCGAClient.getVariantOperationClient()
                .secondaryIndexVariant(new VariantSecondaryIndexParams().setOverwrite(true), params);
    }

}
