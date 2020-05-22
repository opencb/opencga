/*
 * Copyright 2015-2020 OpenCB
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
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.client.template.config.TemplateConfiguration;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
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
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.models.study.VariableSetCreateParams;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.opencb.opencga.core.models.variant.VariantStatsAnalysisParams;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class TemplateManager {

    private final OpenCGAClient openCGAClient;

    private final Logger logger;
    private final boolean resume;

    public TemplateManager(ClientConfiguration clientConfiguration, boolean resume, String token) {
        this.openCGAClient = new OpenCGAClient(new AuthenticationResponse(token), clientConfiguration);
        this.openCGAClient.setThrowExceptionOnError(true);
        this.resume = resume;

        this.logger = LoggerFactory.getLogger(TemplateManager.class);
    }

    public void execute(TemplateConfiguration template, Set<String> studiesSubSet) throws ClientException {
        filterOutTemplate(template, studiesSubSet);
        execute(template);
    }

    public void execute(TemplateConfiguration template) throws ClientException {
        validate(template);

        // Create and load data
        for (Project project : template.getProjects()) {
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
                    createVariableSets(study);
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
                    List<String> studyIndexVcfJobIds = fetchFiles(template, study);
                    projectIndexVcfJobIds.addAll(studyIndexVcfJobIds);
                    String statsJob = variantStats(study, studyIndexVcfJobIds);
                    if (statsJob != null) {
                        statsJobIds.add(statsJob);
                    }
                }
            }
            postIndex(project, projectIndexVcfJobIds, statsJobIds);
        }
    }

    private void filterOutTemplate(TemplateConfiguration template, Set<String> studiesSubSet) {
        if (studiesSubSet != null && !studiesSubSet.isEmpty()) {
            int studies = 0;
            for (Project project : template.getProjects()) {
                project.getStudies().removeIf(s -> !studiesSubSet.contains(s.getId()));
                studies += project.getStudies().size();
            }
            if (studies == 0) {
                throw new IllegalArgumentException("Studies " + studiesSubSet + " not found in template");
            }
        }
    }


    public void validate(TemplateConfiguration template, Set<String> studiesSubSet) throws ClientException {
        filterOutTemplate(template, studiesSubSet);
        validate(template);
    }

    public void validate(TemplateConfiguration template) throws ClientException {
        // Check version
        String versionReal = openCGAClient.getMetaClient().about().firstResult().getString("Version");
        String version;
        if (versionReal.contains("-")) {
            logger.warn("Using development OpenCGA version: " + versionReal);
            version = versionReal.split("-")[0];
        } else {
            version = versionReal;
        }
        String templateVersion = template.getVersion();
        if (!version.equals(templateVersion)) {
            throw new IllegalArgumentException("Version mismatch! Expected " + templateVersion + " but found " + version);
        }

        // Check if any study exists before we start, if a study exists we should fail. Projects are allowed to exist.
        if (!resume) {
            for (Project project : template.getProjects()) {
                if (projectExists(project.getId())) {
                    // If project exists, check that studies does not exist
                    for (Study study : project.getStudies()) {
                        if (studyExists(project.getId(), study.getId())) {
                            throw new ClientException("Study '" + study.getId() + "' already exists");
                        }
                    }
                }
            }
        }
    }

    public boolean projectExists(String projectId) throws ClientException {
        return openCGAClient.getProjectClient().search(
                new ObjectMap()
                        .append("id", projectId)
                        .append(QueryOptions.INCLUDE, "id")).first().getNumResults() > 0;
    }

    public boolean studyExists(String projectId, String studyId) throws ClientException {
        return openCGAClient.getStudyClient().search(
                openCGAClient.getUserId() + "@" + projectId,
                new ObjectMap()
                        .append("id", studyId)
                        .append(QueryOptions.INCLUDE, "id")).first().getNumResults() > 0;
    }

    public void createStudy(Project project, Study study) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.PROJECT_PARAM, project.getId());
        study.setFqn(openCGAClient.getUserId() + "@" + project.getId() + ":" + study.getId());
        if (StringUtils.isEmpty(study.getName())) {
            study.setName(study.getId());
        }
        if (study.getFiles() == null) {
            study.setFiles(Collections.emptyList());
        }
        logger.info("Creating Study '{}'", study.getFqn());
        if (resume && studyExists(project.getId(), study.getId())) {
            logger.info("Study {} already exists", study.getFqn());
        } else {
            openCGAClient.getStudyClient().create(StudyCreateParams.of(study), params);
        }
    }
    private void createVariableSets(Study study) throws ClientException {
        logger.info("Creating {} variable sets from study {}", study.getVariableSets().size(), study.getId());
        Set<String> existing = Collections.emptySet();
        if (resume) {
            existing = openCGAClient.getStudyClient()
                    .variableSets(study.getFqn(), new ObjectMap()
                            .append(QueryOptions.INCLUDE, "name,id")
                            .append(QueryOptions.LIMIT, study.getVariableSets().size()))
                    .allResults()
                    .stream()
                    .map(VariableSet::getId)
                    .collect(Collectors.toSet());
        }
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getFqn());
        for (VariableSet variableSet : study.getVariableSets()) {
            if (!existing.contains(variableSet.getId())) {
                VariableSetCreateParams data = VariableSetCreateParams.of(variableSet);
                openCGAClient.getStudyClient().updateVariableSets(study.getId(), data, params);
            }
        }
    }

    private void createIndividuals(Study study) throws ClientException {
        if (CollectionUtils.isEmpty(study.getIndividuals())) {
            return;
        }
        logger.info("Creating {} individuals from study {}", study.getIndividuals().size(), study.getId());
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getFqn());
        Map<String, Individual> existing = Collections.emptyMap();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getIndividualClient()
                    .info(study.getIndividuals().stream().map(Individual::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id,father.id,mother.id"))
                    .allResults()
                    .stream()
                    .collect(Collectors.toMap(Individual::getId, i -> i));
            openCGAClient.setThrowExceptionOnError(true);
        }


        // Create individuals without parents and siblings
        for (Individual individual : study.getIndividuals()) {
            if (!existing.containsKey(individual.getId())) {
                IndividualCreateParams createParams = IndividualCreateParams.of(individual);
                createParams.setFather(null);
                createParams.setMother(null);
                openCGAClient.getIndividualClient().create(createParams, params);
            }
        }

        // Update parents and siblings for each individual
        for (Individual individual : study.getIndividuals()) {
            IndividualUpdateParams updateParams = new IndividualUpdateParams();
            boolean empty = true;
            Individual existingIndividual = existing.get(individual.getId());
            Individual father = individual.getFather();
            if (father != null) {
                if (existingIndividual == null || existingIndividual.getFather() == null) {
                    // Only if father does not exist already for this individual
                    updateParams.setFather(father.getId());
                    empty = false;
                }
            }
            Individual mother = individual.getMother();
            if (mother != null) {
                if (existingIndividual == null || existingIndividual.getMother() == null) {
                    // Only if mother does not exist already for this individual
                    updateParams.setMother(mother.getId());
                    empty = false;
                }
            }
            if (!empty) {
                openCGAClient.getIndividualClient().update(individual.getId(), updateParams, params);
            }
        }
    }

    private void createSamples(Study study) throws ClientException {
        logger.info("Creating {} samples from study {}", study.getSamples().size(), study.getId());
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getFqn());
        Set<String> existing = Collections.emptySet();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getSampleClient()
                    .info(study.getSamples().stream().map(Sample::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id"))
                    .allResults()
                    .stream()
                    .map(Sample::getId)
                    .collect(Collectors.toSet());
            openCGAClient.setThrowExceptionOnError(true);
        }
        for (Sample sample : study.getSamples()) {
            if (!existing.contains(sample.getId())) {
                openCGAClient.getSampleClient().create(SampleCreateParams.of(sample), params);
            }
        }
    }

    private void createCohorts(Study study) throws ClientException {
        logger.info("Creating {} cohorts from study {}", study.getCohorts().size(), study.getId());
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getFqn());
        Set<String> existing = Collections.emptySet();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getCohortClient()
                    .info(study.getCohorts().stream().map(Cohort::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id"))
                    .allResults()
                    .stream()
                    .map(Cohort::getId)
                    .collect(Collectors.toSet());
            openCGAClient.setThrowExceptionOnError(true);
        }
        for (Cohort cohort : study.getCohorts()) {
            if (!existing.contains(cohort.getId())) {
                openCGAClient.getCohortClient().create(CohortCreateParams.of(cohort), params);
            }
        }
    }

    private void createFamilies(Study study) throws ClientException {
        logger.info("Creating {} families from study {}", study.getFamilies().size(), study.getId());
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getFqn());
        Set<String> existing = Collections.emptySet();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getFamilyClient()
                    .info(study.getFamilies().stream().map(Family::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id"))
                    .allResults()
                    .stream()
                    .map(Family::getId)
                    .collect(Collectors.toSet());
            openCGAClient.setThrowExceptionOnError(true);
        }
        for (Family family : study.getFamilies()) {
            params.put("members", family.getMembers().stream().map(Individual::getId).collect(Collectors.toList()));
            family.setMembers(null);
            if (!existing.contains(family.getId())) {
                openCGAClient.getFamilyClient().create(FamilyCreateParams.of(family), params);
            }
        }
    }

    private List<String> fetchFiles(TemplateConfiguration template, Study study) throws ClientException {
        URI baseUrl = getBaseUrl(template, study);

        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getFqn());
        Set<String> existing = new HashSet<>();
        Set<String> existingAndIndexed = new HashSet<>();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            openCGAClient.getFileClient()
                    .info(study.getFiles().stream().map(file -> getFilePath(file).replace('/', ':')).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "path,name,id,internal"))
                    .allResults()
                    .forEach(file -> {
                        existing.add(file.getPath());
                        if (file.getInternal() != null) {
                            if (file.getInternal().getIndex() != null) {
                                if (file.getInternal().getIndex().getStatus() != null) {
                                    if (Status.READY.equals(file.getInternal().getIndex().getStatus().getName())) {
                                        existingAndIndexed.add(file.getPath());
                                    }
                                }
                            }
                        }
                    });
            openCGAClient.setThrowExceptionOnError(true);
        }
        List<String> indexVcfJobIds = new ArrayList<>();
        for (File file : study.getFiles()) {
            file.setPath(getFilePath(file));
            file.setName(Paths.get(file.getPath()).getFileName().toString());
            file.setId(file.getPath().replace("/", ":"));

            List<String> jobs = Collections.emptyList();
            if (!existing.contains(getFilePath(file))) {
                jobs = fetchFile(baseUrl, params, file);
            }
            if (template.isIndex()) {
                if (isVcf(file) && !existingAndIndexed.contains(getFilePath(file))) {
                    indexVcfJobIds.add(indexVcf(study, getFilePath(file), jobs));
                }
            }
        }
        return indexVcfJobIds;
    }

    private String getFilePath(File file) {
        String path = file.getPath() == null ? "" : file.getPath();
        String name = file.getName() == null ? "" : file.getName();
        if (StringUtils.isEmpty(name) || path.endsWith(name)) {
            return path;
        }
        return path + (path.endsWith("/") ? "" : "/") + name;
    }

    private List<String> fetchFile(URI baseUrl, ObjectMap params, File file) throws ClientException {
        List<String> jobDependsOn;

        String parentPath;
        if (file.getPath().contains("/")) {
            parentPath = Paths.get(file.getPath()).getParent().toString();
        } else {
            parentPath = "";
        }
        logger.info("Process file " + file.getName());

        URI fileUri;
        if (file.getUri() == null) {
            fileUri = baseUrl.resolve(file.getName());
        } else {
            fileUri = file.getUri();
        }

        if (!fileUri.getScheme().equals("file")) {
            // Fetch file
            String fetchUrl;
            if (file.getUri() == null) {
                fetchUrl = baseUrl + file.getName();
            } else {
                fetchUrl = file.getUri().toString();
            }
            FileFetch fileFetch = new FileFetch(fetchUrl, parentPath);
            String fetchJobId = checkJob(openCGAClient.getFileClient().fetch(fileFetch, params));
            jobDependsOn = Collections.singletonList(fetchJobId);
        } else {
            // Link file
            if (StringUtils.isNotEmpty(parentPath)) {
                FileCreateParams createFolder = new FileCreateParams(parentPath, null, null, true, true);
                openCGAClient.getFileClient().create(createFolder, params);
            }
            FileLinkParams data = new FileLinkParams(fileUri.toString(), parentPath, null, Collections.emptyList(), null, null);
            openCGAClient.getFileClient().link(data, params);
            jobDependsOn = Collections.emptyList();
        }
        return jobDependsOn;
    }

    private URI getBaseUrl(TemplateConfiguration template, Study study) {
        String baseUrl = template.getBaseUrl();
        baseUrl = baseUrl.replaceAll("STUDY_ID", study.getId());
        if (!baseUrl.endsWith("/")) {
            baseUrl = baseUrl + "/";
        }
        return URI.create(baseUrl);
    }

    private boolean isVcf(File file) {
        String path = getFilePath(file);
        return path.endsWith(".vcf.gz") || path.endsWith(".vcf");
    }

    private String indexVcf(Study study, String file, List<String> jobDependsOn) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getFqn());
        params.put(ParamConstants.JOB_DEPENDS_ON, jobDependsOn);
        VariantIndexParams variantIndexParams = new VariantIndexParams().setFile(file);
        if (study.getAttributes() != null) {
            variantIndexParams.updateParams(new HashMap<>(study.getAttributes()));
        }
        return checkJob(openCGAClient.getVariantClient()
                .runIndex(variantIndexParams, params));
    }

    private void postIndex(Project project, List<String> indexVcfJobIds, List<String> statsJobIds) throws ClientException {
        List<String> studies = getVariantStudies(project);
        if (!studies.isEmpty()) {
            List<String> jobs = new ArrayList<>(statsJobIds);
            jobs.add(variantAnnot(project, indexVcfJobIds));
            variantSecondaryIndex(project, jobs);
        }
    }

    private String variantStats(Study study, List<String> indexVcfJobIds) throws ClientException {
        if (resume) {
            Cohort cohort = openCGAClient.getCohortClient()
                    .info(StudyEntry.DEFAULT_COHORT, new ObjectMap(ParamConstants.STUDY_PARAM, study.getFqn()))
                    .firstResult();
            if (cohort != null) {
                if (cohort.getInternal() != null && cohort.getInternal().getStatus() != null) {
                    if (Status.READY.equals(cohort.getInternal().getStatus().getName())) {
                        logger.info("Variant stats already calculated. Skip");
                        return null;
                    }
                }
            }
        }

        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study.getFqn())
                .append(ParamConstants.JOB_DEPENDS_ON, indexVcfJobIds);
        VariantStatsAnalysisParams data = new VariantStatsAnalysisParams();
        data.setIndex(true);
        data.setAggregated(Aggregation.NONE);
        for (File file : study.getFiles()) {
            if (file.getName().endsWith(".properties")
                    && file.getAttributes() != null
                    && Boolean.parseBoolean(String.valueOf(file.getAttributes().get("aggregationMappingFile")))) {
                data.setAggregationMappingFile(getFilePath(file));
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
        ObjectMap params = new ObjectMap()
                .append(ParamConstants.PROJECT_PARAM, project.getId())
                .append(ParamConstants.JOB_DEPENDS_ON, indexVcfJobIds);
        return checkJob(openCGAClient.getVariantOperationClient()
                .indexVariantAnnotation(new VariantAnnotationIndexParams(), params));
    }

    private void variantSecondaryIndex(Project project, List<String> jobs) throws ClientException {
        ObjectMap params = new ObjectMap()
                .append(ParamConstants.PROJECT_PARAM, project.getId())
                .append(ParamConstants.JOB_DEPENDS_ON, jobs);
        checkJob(openCGAClient.getVariantOperationClient()
                .secondaryIndexVariant(new VariantSecondaryIndexParams().setOverwrite(true), params));
    }

    private List<String> getVariantStudies(Project project) {
        return project.getStudies()
                .stream()
                .filter(study -> study.getFiles() != null
                        && study.getFiles().stream().anyMatch(this::isVcf))
                .map(Study::getFqn)
                .collect(Collectors.toList());
    }

    private String checkJob(RestResponse<Job> jobRestResponse) throws ClientException {
        Job job = jobRestResponse.firstResult();
        String status = job.getInternal().getStatus().getName();
        if (status.equals(Enums.ExecutionStatus.ABORTED) || status.equals(Enums.ExecutionStatus.ERROR)) {
            throw new ClientException("Error submitting job " + job.getTool().getId() + " " + job.getId());
        }
        return job.getUuid();
    }

}
