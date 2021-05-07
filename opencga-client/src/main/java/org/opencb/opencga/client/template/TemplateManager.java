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

import org.apache.commons.collections4.CollectionUtils;
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
import org.opencb.opencga.client.template.config.TemplateFile;
import org.opencb.opencga.client.template.config.TemplateProject;
import org.opencb.opencga.client.template.config.TemplateStudy;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisCreateParams;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyCreateParams;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.core.models.file.FileFetch;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualCreateParams;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationIndexParams;
import org.opencb.opencga.core.models.operations.variant.VariantSecondaryIndexParams;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelCreateParams;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleCreateParams;
import org.opencb.opencga.core.models.study.StudyCreateParams;
import org.opencb.opencga.core.models.study.StudyVariantEngineConfiguration;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.models.study.VariableSetCreateParams;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.opencb.opencga.core.models.variant.VariantStatsIndexParams;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


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
        for (TemplateProject project : template.getProjects()) {
            if (projectExists(project.getId())) {
                logger.warn("Project '{}' already exists.", project.getId());
            } else {
                logger.info("Creating project '{}'", project.getId());
                openCGAClient.getProjectClient().create(new ProjectCreateParams(
                        project.getId(),
                        project.getName(),
                        project.getDescription(),
                        project.getOrganism()));
            }

            List<String> projectIndexVcfJobIds = new ArrayList<>();
            List<String> statsJobIds = new ArrayList<>();
            for (TemplateStudy study : project.getStudies()) {
                // NOTE: Do not change the order of the following resource creation.
                String studyFqn = createStudy(project, study);
                if (CollectionUtils.isNotEmpty(study.getVariableSets())) {
                    createVariableSets(studyFqn, study);
                }
                if (CollectionUtils.isNotEmpty(study.getIndividuals())) {
                    createIndividuals(studyFqn, study);
                }
                if (CollectionUtils.isNotEmpty(study.getSamples())) {
                    createSamples(studyFqn, study);
                }
                if (CollectionUtils.isNotEmpty(study.getCohorts())) {
                    createCohorts(studyFqn, study);
                }
                if (CollectionUtils.isNotEmpty(study.getFamilies())) {
                    createFamilies(studyFqn, study);
                }
                if (CollectionUtils.isNotEmpty(study.getPanels())) {
                    createPanels(studyFqn, study);
                }
                if (CollectionUtils.isNotEmpty(study.getClinicalAnalyses())) {
                    createClinicalAnalyses(studyFqn, study);
                }
                if (study.getVariantEngineConfiguration() != null) {
                    configureVariantEngine(studyFqn, study);
                }
                if (CollectionUtils.isNotEmpty(study.getFiles())) {
                    List<String> studyIndexVcfJobIds = fetchFiles(template, studyFqn, study);
                    projectIndexVcfJobIds.addAll(studyIndexVcfJobIds);
                    if (isVariantStudy(study)) {
                        String statsJob = variantStats(studyFqn, study, studyIndexVcfJobIds);
                        if (statsJob != null) {
                            statsJobIds.add(statsJob);
                        }
                    }
                }
            }
            postIndex(project, projectIndexVcfJobIds, statsJobIds);
        }
    }

    private void filterOutTemplate(TemplateConfiguration template, Set<String> studiesSubSet) {
        if (studiesSubSet != null && !studiesSubSet.isEmpty()) {
            int studies = 0;
            for (TemplateProject project : template.getProjects()) {
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
        String version = openCGAClient.getMetaClient().about().firstResult().getString("Version");
        String versionShort;
        if (version.contains("-")) {
            logger.warn("Using development OpenCGA version: " + version);
            versionShort = version.split("-")[0];
        } else {
            versionShort = version;
        }
        String templateVersion = template.getVersion();
        if (!version.equals(templateVersion) && !versionShort.equals(templateVersion)) {
            throw new IllegalArgumentException("Version mismatch! Expected " + templateVersion + " but found " + versionShort);
        }

        // Check if any study exists before we start, if a study exists we should fail. Projects are allowed to exist.
        if (!resume) {
            for (TemplateProject project : template.getProjects()) {
                if (projectExists(project.getId())) {
                    // If project exists, check that studies does not exist
                    for (TemplateStudy study : project.getStudies()) {
                        if (studyExists(project.getId(), study.getId())) {
                            throw new ClientException("Study '" + study.getId() + "' already exists");
                        }
                    }
                }
            }
        }
        for (TemplateProject project : template.getProjects()) {
            for (TemplateStudy study : project.getStudies()) {
                URI baseUrl = getBaseUrl(template, study);
                if (baseUrl == null) {
                    for (TemplateFile file : study.getFiles()) {
                        if (file.getUri() == null) {
                            throw new IllegalArgumentException("File uri missing. Either file.uri or baseUrl should be defined.");
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

    public String createStudy(TemplateProject project, TemplateStudy study) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.PROJECT_PARAM, project.getId());
        String fqn = buildFqn(project, study);
        if (StringUtils.isEmpty(study.getName())) {
            study.setName(study.getId());
        }
        if (study.getFiles() == null) {
            study.setFiles(Collections.emptyList());
        }
        logger.info("Creating Study '{}'", fqn);
        if (resume && studyExists(project.getId(), study.getId())) {
            logger.info("Study {} already exists", fqn);
        } else {
            openCGAClient.getStudyClient().create(new StudyCreateParams(
                    study.getId(),
                    study.getName(),
                    study.getAlias(),
                    study.getDescription(),
                    study.getNotification(),
                    study.getAttributes(),
                    study.getStatus()), params);
        }
        return fqn;
    }

    protected String buildFqn(TemplateProject project, TemplateStudy study) {
        return openCGAClient.getUserId() + "@" + project.getId() + ":" + study.getId();
    }

    private void createVariableSets(String studyFqn, TemplateStudy study) throws ClientException {
        logger.info("Creating {} variable sets from study {}", study.getVariableSets().size(), study.getId());
        Set<String> existing = Collections.emptySet();
        if (resume) {
            existing = openCGAClient.getStudyClient()
                    .variableSets(studyFqn, new ObjectMap()
                            .append(QueryOptions.INCLUDE, "name,id")
                            .append(QueryOptions.LIMIT, study.getVariableSets().size()))
                    .allResults()
                    .stream()
                    .map(VariableSet::getId)
                    .collect(Collectors.toSet());
        }
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn);
        for (VariableSetCreateParams variableSet : study.getVariableSets()) {
            if (!existing.contains(variableSet.getId())) {
                openCGAClient.getStudyClient().updateVariableSets(study.getId(), variableSet, params);
            }
        }
    }

    private void createIndividuals(String studyFqn, TemplateStudy study) throws ClientException {
        List<IndividualCreateParams> individuals = study.getIndividuals();
        if (CollectionUtils.isEmpty(individuals)) {
            return;
        }
        logger.info("Creating {} individuals from study {}", individuals.size(), study.getId());
        for (List<IndividualCreateParams> batch : batches(individuals)) {
            createIndividualsWithoutParents(studyFqn, batch);
        }
        List<IndividualCreateParams> individualsWithParents = individuals.stream()
                .filter(i -> StringUtils.isNotEmpty(i.getFather()) || StringUtils.isNotEmpty(i.getMother()))
                .collect(Collectors.toList());
        for (List<IndividualCreateParams> batch : batches(individualsWithParents)) {
            updateIndividualsParents(studyFqn, batch);
        }
    }

    private void createIndividualsWithoutParents(String studyFqn, List<IndividualCreateParams> individuals) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn);
        Map<String, Individual> existing = Collections.emptyMap();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getIndividualClient()
                    .info(individuals.stream().map(IndividualCreateParams::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id"))
                    .allResults()
                    .stream()
                    .collect(Collectors.toMap(Individual::getId, i -> i));
            openCGAClient.setThrowExceptionOnError(true);
        }


        // Create individuals without parents and siblings
        for (IndividualCreateParams individual : individuals) {
            if (!existing.containsKey(individual.getId())) {
                // Take parents
                String father = individual.getFather();
                String mother = individual.getMother();
                individual.setFather(null);
                individual.setMother(null);

                openCGAClient.getIndividualClient().create(individual, params);

                // Restore parents
                individual.setFather(father);
                individual.setMother(mother);
            }
        }
    }

    private void updateIndividualsParents(String studyFqn, List<IndividualCreateParams> individuals) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn);
        Map<String, Individual> existing = Collections.emptyMap();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getIndividualClient()
                    .info(individuals.stream().map(IndividualCreateParams::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id,father.id,mother.id"))
                    .allResults()
                    .stream()
                    .collect(Collectors.toMap(Individual::getId, i -> i));
            openCGAClient.setThrowExceptionOnError(true);
        }

        // Update parents and siblings for each individual
        for (IndividualCreateParams individual : individuals) {
            IndividualUpdateParams updateParams = new IndividualUpdateParams();
            boolean empty = true;
            Individual existingIndividual = existing.get(individual.getId());
            String father = individual.getFather();
            if (father != null) {
                if (existingIndividual == null || existingIndividual.getFather() == null) {
                    // Only if father does not exist already for this individual
                    updateParams.setFather(father);
                    empty = false;
                }
            }
            String mother = individual.getMother();
            if (mother != null) {
                if (existingIndividual == null || existingIndividual.getMother() == null) {
                    // Only if mother does not exist already for this individual
                    updateParams.setMother(mother);
                    empty = false;
                }
            }
            if (!empty) {
                openCGAClient.getIndividualClient().update(individual.getId(), updateParams, params);
            }
        }
    }

    private void createSamples(String fqn, TemplateStudy study) throws ClientException {
        logger.info("Creating {} samples from study {}", study.getSamples().size(), study.getId());
        for (List<SampleCreateParams> batch : batches(study.getSamples())) {
            createSamples(fqn, batch);
        }
    }

    private void createSamples(String fqn, List<SampleCreateParams> samples) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, fqn);
        Set<String> existing = Collections.emptySet();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getSampleClient()
                    .info(samples.stream().map(SampleCreateParams::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id"))
                    .allResults()
                    .stream()
                    .map(Sample::getId)
                    .collect(Collectors.toSet());
            openCGAClient.setThrowExceptionOnError(true);
        }
        for (SampleCreateParams sample : samples) {
            if (!existing.contains(sample.getId())) {
                openCGAClient.getSampleClient().create(sample, params);
            }
        }
    }

    private void createCohorts(String fqn, TemplateStudy study) throws ClientException {
        logger.info("Creating {} cohorts from study {}", study.getCohorts().size(), study.getId());
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, fqn);
        Set<String> existing = Collections.emptySet();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getCohortClient()
                    .info(study.getCohorts().stream().map(CohortCreateParams::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id"))
                    .allResults()
                    .stream()
                    .map(Cohort::getId)
                    .collect(Collectors.toSet());
            openCGAClient.setThrowExceptionOnError(true);
        }
        for (CohortCreateParams cohort : study.getCohorts()) {
            if (!existing.contains(cohort.getId())) {
                openCGAClient.getCohortClient().create(cohort, params);
            }
        }
    }

    private void createFamilies(String fqn, TemplateStudy study) throws ClientException {
        List<FamilyCreateParams> families = study.getFamilies();
        if (CollectionUtils.isEmpty(families)) {
            return;
        }
        logger.info("Creating {} families from study {}", families.size(), study.getId());
        for (List<FamilyCreateParams> batch : batches(families)) {
            createFamilies(fqn, batch);
        }
    }

    private void createFamilies(String fqn, List<FamilyCreateParams> families) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, fqn);
        Set<String> existing = Collections.emptySet();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getFamilyClient()
                    .info(families.stream().map(FamilyCreateParams::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id"))
                    .allResults()
                    .stream()
                    .map(Family::getId)
                    .collect(Collectors.toSet());
            openCGAClient.setThrowExceptionOnError(true);
        }
        for (FamilyCreateParams family : families) {
            if (!existing.contains(family.getId())) {
                List<org.opencb.opencga.core.models.family.IndividualCreateParams> members = family.getMembers();
                params.put("members", members.stream().map(i -> i.getId()).collect(Collectors.toList()));
                family.setMembers(null);
                openCGAClient.getFamilyClient().create(family, params);
                family.setMembers(members);
            }
        }
    }

    private void createPanels(String fqn, TemplateStudy study) throws ClientException {
        List<PanelCreateParams> panels = study.getPanels();
        if (CollectionUtils.isEmpty(panels)) {
            return;
        }
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, fqn);
        Set<String> existing = Collections.emptySet();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getDiseasePanelClient()
                    .info(panels.stream().map(PanelCreateParams::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id"))
                    .allResults()
                    .stream()
                    .map(Panel::getId)
                    .collect(Collectors.toSet());
            openCGAClient.setThrowExceptionOnError(true);
        }
        for (PanelCreateParams panel : panels) {
            if (!existing.contains(panel.getId())) {
                openCGAClient.getDiseasePanelClient().create(panel, params);
            }
        }
    }

    private void createClinicalAnalyses(String studyFqn, TemplateStudy study) throws ClientException {
        List<ClinicalAnalysisCreateParams> clinicalAnalyses = study.getClinicalAnalyses();
        if (CollectionUtils.isEmpty(clinicalAnalyses)) {
            return;
        }
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn);
        Set<String> existing = Collections.emptySet();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            existing = openCGAClient.getClinicalAnalysisClient()
                    .info(clinicalAnalyses.stream().map(ClinicalAnalysisCreateParams::getId).collect(Collectors.joining(",")),
                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id"))
                    .allResults()
                    .stream()
                    .map(ClinicalAnalysis::getId)
                    .collect(Collectors.toSet());
            openCGAClient.setThrowExceptionOnError(true);
        }
        for (ClinicalAnalysisCreateParams createParams : clinicalAnalyses) {
            if (!existing.contains(createParams.getId())) {
                openCGAClient.getClinicalAnalysisClient().create(createParams, params);
            }
        }
    }

    private void configureVariantEngine(String studyFqn, TemplateStudy study) throws ClientException {
        StudyVariantEngineConfiguration configuration = study.getVariantEngineConfiguration();
        if (configuration == null) {
            return;
        }
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn);
        if (configuration.getOptions() != null && !configuration.getOptions().isEmpty()) {
            openCGAClient.getVariantOperationClient().configureVariant(configuration.getOptions(), params);
        }
        if (configuration.getSampleIndex() != null) {
            openCGAClient.getVariantOperationClient().configureSampleIndex(configuration.getSampleIndex(), params);
        }

    }

    private List<String> fetchFiles(TemplateConfiguration template, String studyFqn, TemplateStudy study)
            throws ClientException {
        List<TemplateFile> files = study.getFiles();
        if (CollectionUtils.isEmpty(files)) {
            return Collections.emptyList();
        }
        List<String> indexVcfJobIds = new ArrayList<>(files.size());
        for (List<TemplateFile> batch : batches(files)) {
            indexVcfJobIds.addAll(fetchFiles(template, studyFqn, study, batch));
        }
        return indexVcfJobIds;
    }

    private List<String> fetchFiles(TemplateConfiguration template, String studyFqn, TemplateStudy study, List<TemplateFile> files)
            throws ClientException {
        URI baseUrl = getBaseUrl(template, study);
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn);
        Set<String> existing = new HashSet<>();
        Set<String> existingAndIndexed = new HashSet<>();
        if (resume) {
            openCGAClient.setThrowExceptionOnError(false);
            openCGAClient.getFileClient()
                    .info(files.stream().map(file -> getFilePath(file).replace('/', ':')).collect(Collectors.joining(",")),
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
        for (TemplateFile file : files) {
            file.setPath(getFilePath(file));
            file.setName(Paths.get(file.getPath()).getFileName().toString());
            String fileId = file.getPath().replace("/", ":");

            List<String> jobs = Collections.emptyList();
            if (!existing.contains(getFilePath(file))) {
                jobs = fetchFile(baseUrl, params, file);
            } else {
                updateFileIfChanged(fileId, file, params);
            }
            if (template.isIndex()) {
                if (isVcf(file) && !existingAndIndexed.contains(getFilePath(file))) {
                    indexVcfJobIds.add(indexVcf(studyFqn, study, getFilePath(file), jobs));
                }
            }
        }
        return indexVcfJobIds;
    }

    private String getFilePath(TemplateFile file) {
        String path = file.getPath() == null ? "" : file.getPath();
        String name = file.getName() == null ? "" : file.getName();
        if (StringUtils.isEmpty(name) || path.endsWith(name)) {
            return path;
        }
        return path + (path.endsWith("/") ? "" : "/") + name;
    }

    private List<String> fetchFile(URI baseUrl, ObjectMap params, TemplateFile file) throws ClientException {
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
            fileUri = URI.create(file.getUri());
        }

        if (!fileUri.getScheme().equals("file")) {
            // Fetch file
            String fetchUrl;
            if (file.getUri() == null) {
                fetchUrl = baseUrl + file.getName();
            } else {
                fetchUrl = file.getUri();
            }
            FileFetch fileFetch = new FileFetch(fetchUrl, parentPath);
            String fetchJobId = checkJob(openCGAClient.getFileClient().fetch(fileFetch, params));
            jobDependsOn = Collections.singletonList(fetchJobId);
            // TODO: Other fields are not being updated!
        } else {
            // Link file
            if (StringUtils.isNotEmpty(parentPath)) {
                FileCreateParams createFolder = new FileCreateParams(parentPath, null, null, true, true);
                openCGAClient.getFileClient().create(createFolder, params);
            }
            FileLinkParams data = new FileLinkParams(fileUri.toString(), parentPath,
                    file.getDescription(), file.getRelatedFiles(), file.getStatus(), null);
            String fileId = openCGAClient.getFileClient().link(data, params).firstResult().getId();
            updateFile(fileId, file, params);
            jobDependsOn = Collections.emptyList();
        }
        return jobDependsOn;
    }

    private void updateFileIfChanged(String fileId, TemplateFile file, ObjectMap params) throws ClientException {
        // TODO: Check if update is needed!
        updateFile(fileId, file, params);
    }

    private void updateFile(String fileId, TemplateFile file, ObjectMap params) throws ClientException {
        openCGAClient.getFileClient().update(fileId, new FileUpdateParams(
                file.getName(),
                file.getDescription(),
                file.getSampleIds(),
                file.getChecksum(),
                file.getFormat(),
                file.getBioformat(),
                file.getSoftware(),
                file.getExperiment(),
                file.getTags(),
                file.getInternal(),
                file.getSize(),
                file.getRelatedFiles(),
                file.getStatus(),
                file.getAnnotationSets(),
                file.getQualityControl(),
                file.getStats(),
                file.getAttributes()
        ), params);
    }

    private URI getBaseUrl(TemplateConfiguration template, TemplateStudy study) {
        String baseUrl = template.getBaseUrl();
        if (StringUtils.isEmpty(baseUrl)) {
            return null;
        }
        baseUrl = baseUrl.replaceAll("STUDY_ID", study.getId());
        if (!baseUrl.endsWith("/")) {
            baseUrl = baseUrl + "/";
        }
        return URI.create(baseUrl);
    }

    private boolean isVcf(TemplateFile file) {
        String path = getFilePath(file);
        return path.endsWith(".vcf.gz") || path.endsWith(".vcf")
                || path.endsWith(".gvcf.gz") || path.endsWith(".gvcf");
    }

    private String indexVcf(String fqn, TemplateStudy study, String file, List<String> jobDependsOn) throws ClientException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, fqn);
        params.put(ParamConstants.JOB_DEPENDS_ON, jobDependsOn);
        VariantIndexParams variantIndexParams = new VariantIndexParams().setFile(file);
        if (study.getAttributes() != null) {
            Object o = study.getAttributes().get("variant-index-run");
            if (o instanceof Map) {
                variantIndexParams.updateParams(new HashMap<>(((Map) o)));
            }
        }
        return checkJob(openCGAClient.getVariantClient()
                .runIndex(variantIndexParams, params));
    }

    private void postIndex(TemplateProject project, List<String> indexVcfJobIds, List<String> statsJobIds) throws ClientException {
        List<String> studies = getVariantStudies(project);
        if (!studies.isEmpty()) {
            List<String> jobs = new ArrayList<>(statsJobIds);
            jobs.add(variantAnnot(project, indexVcfJobIds));
            variantSecondaryIndex(project, jobs);
        }
    }

    private String variantStats(String fqn, TemplateStudy study, List<String> indexVcfJobIds) throws ClientException {
        if (resume) {
            Cohort cohort = openCGAClient.getCohortClient()
                    .info(StudyEntry.DEFAULT_COHORT, new ObjectMap(ParamConstants.STUDY_PARAM, fqn))
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

        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, fqn)
                .append(ParamConstants.JOB_DEPENDS_ON, indexVcfJobIds);
        VariantStatsIndexParams data = new VariantStatsIndexParams();

        data.setAggregated(Aggregation.NONE);
        for (TemplateFile file : study.getFiles()) {
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

        return checkJob(openCGAClient.getVariantOperationClient()
                .indexVariantStats(data, params));
    }

    private String variantAnnot(TemplateProject project, List<String> indexVcfJobIds) throws ClientException {
        ObjectMap params = new ObjectMap()
                .append(ParamConstants.PROJECT_PARAM, project.getId())
                .append(ParamConstants.JOB_DEPENDS_ON, indexVcfJobIds);
        return checkJob(openCGAClient.getVariantOperationClient()
                .indexVariantAnnotation(new VariantAnnotationIndexParams(), params));
    }

    private void variantSecondaryIndex(TemplateProject project, List<String> jobs) throws ClientException {
        ObjectMap params = new ObjectMap()
                .append(ParamConstants.PROJECT_PARAM, project.getId())
                .append(ParamConstants.JOB_DEPENDS_ON, jobs);
        checkJob(openCGAClient.getVariantOperationClient()
                .secondaryIndexVariant(new VariantSecondaryIndexParams().setOverwrite(true), params));
    }

    private List<String> getVariantStudies(TemplateProject project) {
        return project.getStudies()
                .stream()
                .filter(this::isVariantStudy)
                .map(s -> buildFqn(project, s))
                .collect(Collectors.toList());
    }

    private boolean isVariantStudy(TemplateStudy study) {
        return study.getFiles() != null
                && study.getFiles().stream().anyMatch(this::isVcf);
    }

    private String checkJob(RestResponse<Job> jobRestResponse) throws ClientException {
        Job job = jobRestResponse.firstResult();
        String status = job.getInternal().getStatus().getName();
        if (status.equals(Enums.ExecutionStatus.ABORTED) || status.equals(Enums.ExecutionStatus.ERROR)) {
            throw new ClientException("Error submitting job " + job.getTool().getId() + " " + job.getId());
        }
        return job.getUuid();
    }

    private <T> Iterable<List<T>> batches(List<T> elements) {
        return batches(elements, 50);
    }

    private <T> Iterable<List<T>> batches(List<T> elements, int size) {
        if (elements == null || elements.isEmpty()) {
            return Collections.emptyList();
        }
        return () -> {
            int numBatches = (int) Math.ceil(elements.size() / ((float) size));
            return IntStream.range(0, numBatches)
                    .mapToObj(i -> elements.subList(i * size, Math.min((i + 1) * size, elements.size())))
                    .iterator();
        };
    }

}
