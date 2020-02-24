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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.biodata.models.variant.StudyEntry;
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
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyCreateParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.core.models.file.FileFetch;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualCreateParams;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.job.Job;
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
        this.openCGAClient.setThrowExceptionOnError(true);

        this.logger = LoggerFactory.getLogger(TemplateManager.class);
    }

    public void execute() throws ClientException {
        // TODO Check version

        // Check if any study exists before we start, if a study exists we should fail. Projects are allowed to exist.
        for (Project project : templateConfiguration.getProjects()) {
            if (projectExists(project.getId())) {
                // If project exists, check that studies does not exist
                for (Study study : project.getStudies()) {
                    if (studyExists(project.getId(), study.getId())) {
                        throw new ClientException("Study '" + study.getId() + "' already exists");
                    }
                }
            }
        }

        // Create and load data
        for (Project project : templateConfiguration.getProjects()) {
            if (projectExists(project.getId())) {
                logger.warn("Project '{}' already exists.", project.getId());
            } else {
                logger.info("Creating project '{}'", project.getId());
                openCGAClient.getProjectClient().create(ProjectCreateParams.of(project));
            }

            List<String> projectIndexVcfJobIds = new ArrayList<>();
            List<String> statsJobIds = new ArrayList<>();
            for (Study study : project.getStudies()) {
                // NOTE: Do not change the order of the following resource creation.
                createStudy(project, study);
                if (CollectionUtils.isNotEmpty(study.getVariableSets())) {
                    // TODO
                    //  createVariableSets(study);
                    logger.warn("Variable sets not created!");
                }
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
                if (CollectionUtils.isNotEmpty(study.getPanels())) {
                    // TODO
                    //  createPanels(study);
                    logger.warn("Panels sets not created!");
                }
// TODO
//                if (CollectionUtils.isNotEmpty(study.getClinicalAnalysis())) {
//                    createClinicalAnalysis(study);
//                }
                if (CollectionUtils.isNotEmpty(study.getFiles())) {
                    List<String> studyIndexVcfJobIds = fetchFiles(study);
                    statsJobIds.add(variantStats(study, studyIndexVcfJobIds));
                    projectIndexVcfJobIds.addAll(studyIndexVcfJobIds);
                }
            }
            if (CollectionUtils.isNotEmpty(projectIndexVcfJobIds)) {
                postIndex(project, projectIndexVcfJobIds, statsJobIds);
            }
        }
    }

    public boolean projectExists(String projectId) throws ClientException {
        try {
            return openCGAClient.getProjectClient()
                    .info(openCGAClient.getUserId() + "@" + projectId, new ObjectMap()).first().getNumResults() > 0;
        } catch (ClientException e) {
            // TODO: Check error code
            if (e.getMessage().toLowerCase().contains("not found")) {
                return false;
            } else {
                throw e;
            }
        }
    }

    public boolean studyExists(String projectId, String studyId) throws ClientException {
        try {
            return openCGAClient.getStudyClient()
                    .info(openCGAClient.getUserId() + "@" + projectId + ":" + studyId, new ObjectMap()).first().getNumResults() > 0;
        } catch (ClientException e) {
            // TODO: Check error code
            if (e.getMessage().toLowerCase().contains("no study found")) {
                return false;
            } else {
                throw e;
            }
        }
    }

    public void createStudy(Project project, Study study) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.PROJECT_PARAM, project.getId());
        study.setFqn(openCGAClient.getUserId() + "@" + project.getId() + ":" + study.getId());
        logger.info("Creating Study '{}'", study.getFqn());
        if (study.getType() == null) {
            study.setType(Study.Type.COLLECTION);
        }
        openCGAClient.getStudyClient().create(StudyCreateParams.of(study), params);
    }

    private void createIndividuals(Study study) throws ClientException {
        if (CollectionUtils.isEmpty(study.getIndividuals())) {
            return;
        }
        logger.info("Creating {} individuals from study {}", study.getIndividuals().size(), study.getId());

        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
        Map<String, Map<String, Object>> relatives = new HashMap<>();

        // Create individuals without parents and siblings
        for (Individual individual : study.getIndividuals()) {
            relatives.put(individual.getId(), new ObjectMap()
                    .append("father", individual.getFather())
                    .append("mother", individual.getMother())
                    .append("multiples", individual.getMultiples()));
            individual.setFather(null);
            individual.setMother(null);
            individual.setMultiples(null);
            openCGAClient.getIndividualClient().create(IndividualCreateParams.of(individual), params);
        }

        // Update parents and siblings for each individual
        for (Individual individual : study.getIndividuals()) {
            IndividualUpdateParams updateParams = new IndividualUpdateParams();
            boolean empty = true;
            Individual father = (Individual) relatives.get(individual.getId()).get("father");
            if (father != null) {
                updateParams.setFather(father.getId());
                empty = false;
            }
            Individual mother = (Individual) relatives.get(individual.getId()).get("mother");
            if (mother != null) {
                updateParams.setMother(mother.getId());
                empty = false;
            }
            Multiples multiples = (Multiples) relatives.get(individual.getId()).get("multiples");
            if (multiples != null) {
                updateParams.setMultiples(multiples);
                empty = false;
            }
            if (!empty) {
                openCGAClient.getIndividualClient().update(individual.getId(), updateParams, params);
            }
        }
    }

    private void createSamples(Study study) throws ClientException {
        logger.info("Creating {} samples from study {}", study.getSamples().size(), study.getId());
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
        for (Sample sample : study.getSamples()) {
            openCGAClient.getSampleClient().create(SampleCreateParams.of(sample), params);
        }
    }

    private void createCohorts(Study study) throws ClientException {
        logger.info("Creating {} cohorts from study {}", study.getCohorts().size(), study.getId());
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
        for (Cohort cohort : study.getCohorts()) {
            openCGAClient.getCohortClient().create(CohortCreateParams.of(cohort), params);
        }
    }

    private void createFamilies(Study study) throws ClientException {
        logger.info("Creating {} families from study {}", study.getFamilies().size(), study.getId());
        for (Family family : study.getFamilies()) {
            ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
            params.put("members", family.getMembers().stream().map(Individual::getId).collect(Collectors.toList()));
            family.setMembers(null);
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
            List<String> jobDependsOn;
            logger.info("Process file " + file.getName());
            if (file.getUri() == null || !file.getUri().getScheme().equals("file")) {
                String fetchUrl;
                if (file.getUri() == null) {
                    fetchUrl = baseUrl + file.getName();
                } else {
                    fetchUrl = file.getUri().toString();
                }
                FileFetch fileFetch = new FileFetch(fetchUrl, file.getPath());
                String fetchJobId = checkJob(openCGAClient.getFileClient().fetch(fileFetch, params));
                jobDependsOn = Collections.singletonList(fetchJobId);
            } else {
                if (StringUtils.isNotEmpty(file.getPath())) {
                    FileCreateParams createFolder = new FileCreateParams(file.getPath(), null, null, true, true);
                    openCGAClient.getFileClient().create(createFolder, params);
                }
                FileLinkParams data = new FileLinkParams(file.getUri().toString(), file.getPath(), null, Collections.emptyList());
                openCGAClient.getFileClient().link(data, params);
                jobDependsOn = Collections.emptyList();
            }
            if (templateConfiguration.isIndex()) {
                if (isVcf(file)) {
                    indexVcfJobIds.add(indexVcf(study, file.getName(), jobDependsOn));
                }
            }
        }
        return indexVcfJobIds;
    }

    private boolean isVcf(File file) {
        return file.getName().endsWith(".vcf.gz") || file.getName().endsWith(".vcf");
    }

    private String indexVcf(Study study, String file, List<String> jobDependsOn) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId());
        params.put("jobDependsOn", jobDependsOn);
        return checkJob(openCGAClient.getVariantClient()
                .index(new VariantIndexParams().setFile(file), params));
    }

    private void postIndex(Project project, List<String> indexVcfJobIds, List<String> statsJobIds) throws ClientException {
        if (CollectionUtils.isEmpty(indexVcfJobIds)) {
            return;
        }
        List<String> jobs = new ArrayList<>(statsJobIds);
        jobs.add(variantAnnot(project, indexVcfJobIds));
        variantSecondaryIndex(project, jobs);
    }

    private String variantStats(Study study, List<String> indexVcfJobIds) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getId())
                .append("jobDependsOn", indexVcfJobIds);
        VariantStatsAnalysisParams data = new VariantStatsAnalysisParams();
        data.setAggregated(Aggregation.NONE);
        for (File file : study.getFiles()) {
            if (file.getName().endsWith(".properties")
                    && file.getAttributes() != null
                    && Boolean.parseBoolean(String.valueOf(file.getAttributes().get("aggregationMappingFile")))) {
                data.setAggregationMappingFile(file.getName());
                data.setAggregated(Aggregation.BASIC);
            }
        }

        if (study.getAttributes() != null) {
            Object aggregationObj = study.getAttributes().get("aggregation");
            if (aggregationObj != null) {
                data.setAggregated(AggregationUtils.valueOf(aggregationObj.toString()));
            }
        }

        if (data.getAggregated().equals(Aggregation.NONE)) {
            data.setCohort(Collections.singletonList(StudyEntry.DEFAULT_COHORT));
        }

        return checkJob(openCGAClient.getVariantClient()
                .runStats(data, params));
    }

    private String variantAnnot(Project project, List<String> indexVcfJobIds) throws ClientException {
        List<String> studies = project.getStudies().stream()
                .filter(study -> study.getFiles()
                        .stream()
                        .anyMatch(this::isVcf))
                .map(Study::getFqn)
                .collect(Collectors.toList());

        if (!studies.isEmpty()) {
            ObjectMap params = new ObjectMap()
                    // TODO: Allow project based annotation
//                    .append(ParamConstants.PROJECT_PARAM, project.getId())
                    .append(ParamConstants.STUDY_PARAM, studies)
                    .append("jobDependsOn", indexVcfJobIds);
            return checkJob(openCGAClient.getVariantOperationClient()
                    .indexVariantAnnotation(new VariantAnnotationIndexParams(), params));
        } else {
            return null;
        }
    }

    private void variantSecondaryIndex(Project project, List<String> jobs) throws ClientException {
        List<String> studies = project.getStudies().stream()
                .filter(study -> study.getFiles()
                        .stream()
                        .anyMatch(this::isVcf))
                .map(Study::getFqn)
                .collect(Collectors.toList());

        if (!studies.isEmpty()) {
            ObjectMap params = new ObjectMap()
                    // TODO: Allow project based secondary index
//                    .append(ParamConstants.PROJECT_PARAM, project.getId())
                    .append(ParamConstants.STUDY_PARAM, studies)
                    .append("jobDependsOn", jobs);
            checkJob(openCGAClient.getVariantOperationClient()
                    .secondaryIndexVariant(new VariantSecondaryIndexParams().setOverwrite(true), params));
        }
    }

    private String checkJob(RestResponse<Job> jobRestResponse) throws ClientException {
        Job job = jobRestResponse.firstResult();
        String status = job.getInternal().getStatus().getName();
        if (status.equals(Enums.ExecutionStatus.ABORTED) || status.equals(Enums.ExecutionStatus.ERROR)) {
            throw new ClientException("Error submitting job " + job.getTool().getId() + " " + job.getId());
        }
        return job.getId();
    }

}
