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

package org.opencb.opencga.catalog.templates;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.GitRepositoryState;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.templates.config.TemplateFile;
import org.opencb.opencga.catalog.templates.config.TemplateManifest;
import org.opencb.opencga.catalog.templates.config.TemplateStudy;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisUpdateParams;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.panel.PanelUpdateParams;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class TemplateManager {

    private final CatalogManager catalogManager;

    private final Logger logger;
    private final boolean resume;
    private final boolean overwrite;

    private final String token;

    public TemplateManager(CatalogManager catalogManager, boolean resume, boolean overwrite, String token) {
        this.catalogManager = catalogManager;
        this.overwrite = overwrite;
        this.resume = resume || overwrite;
        this.token = token;

        this.logger = LoggerFactory.getLogger(TemplateManager.class);
    }

    public void execute(TemplateManifest manifest, Path path) throws CatalogException {
//        filterOutTemplate(template, studiesSubSet);

        validate(manifest);

        List<String> projectIndexVcfJobIds = new ArrayList<>();
        List<String> statsJobIds = new ArrayList<>();

        TemplateStudy study = manifest.getStudy();

        // NOTE: Do not change the order of the following resource creation.
        String studyFqn = addStudyMetadata(manifest.getConfiguration().getProjectId(), study);

        createIndividuals(studyFqn, path);
        createSamples(studyFqn, path);
        createCohorts(studyFqn, path);
        createFamilies(studyFqn, path);
        createPanels(studyFqn, path);
        createClinicalAnalyses(studyFqn, path);

        // TODO: What is this?
//        if (study.getVariantEngineConfiguration() != null) {
//            configureVariantEngine(studyFqn, study);
//        }
        createFiles(studyFqn, path);
//            if (CollectionUtils.isNotEmpty(study.getFiles())) {
//                List<String> studyIndexVcfJobIds = fetchFiles(template, studyFqn, study);
//                projectIndexVcfJobIds.addAll(studyIndexVcfJobIds);
//                if (isVariantStudy(study)) {
//                    String statsJob = variantStats(studyFqn, study, studyIndexVcfJobIds);
//                    if (statsJob != null) {
//                        statsJobIds.add(statsJob);
//                    }
//                }
//            }
        // TODO: Post index?
//        postIndex(manifest.getConfiguration().getProjectId(), projectIndexVcfJobIds, statsJobIds);
    }

    public void validate(TemplateManifest manifest) throws CatalogException {
        if (manifest.getConfiguration() == null) {
            throw new IllegalStateException("Missing 'configuration' section from the Manifest file");
        }
        if (StringUtils.isEmpty(manifest.getConfiguration().getProjectId())) {
            throw new IllegalStateException("Missing 'configuration.projectId' from the Manifest file");
        }
        if (manifest.getStudy() == null) {
            throw new IllegalStateException("Missing 'study' section from the Manifest file");
        }
        if (StringUtils.isEmpty(manifest.getStudy().getId())) {
            throw new IllegalStateException("Missing 'study.id' from the Manifest file");
        }

        // Check version
        GitRepositoryState gitRepositoryState = GitRepositoryState.get();
        String version = gitRepositoryState.getBuildVersion();
        String versionShort;
        if (version.contains("-")) {
            logger.warn("Using development OpenCGA version: " + version);
            versionShort = version.split("-")[0];
        } else {
            versionShort = version;
        }
        String templateVersion = manifest.getConfiguration().getVersion();
        if (!version.equals(templateVersion) && !versionShort.equals(templateVersion)) {
            throw new IllegalArgumentException("Version mismatch! Expected " + templateVersion + " but found " + versionShort);
        }

        // Study should already exist
        Study study = getStudy(manifest.getConfiguration().getProjectId(), manifest.getStudy().getId());
        String userId = catalogManager.getUserManager().getUserId(token);
        catalogManager.getAuthorizationManager().checkIsOwnerOrAdmin(study.getUid(), userId);

//        // Check if any study exists before we start, if a study exists we should fail. Projects are allowed to exist.
//        if (!resume && !overwrite) {
//            if (studyExists(manifest.getConfiguration().getProjectId(), manifest.getStudy().getId())) {
//                throw new CatalogException("Study '" + manifest.getStudy().getId() + "' already exists. Do you want to resume?");
//            }
//        }
    }

    private Study getStudy(String projectId, String studyId) throws CatalogException {
        OpenCGAResult<Study> studyOpenCGAResult =
                catalogManager.getStudyManager().get(projectId + ":" + studyId, QueryOptions.empty(), token);
        return studyOpenCGAResult.first();
    }

    private String addStudyMetadata(String projectId, TemplateStudy tmplStudy) throws CatalogException {
        Study origStudy = getStudy(projectId, tmplStudy.getId());
        String fqn;
//        if (origStudy == null) {
//            Study study = new Study()
//                    .setId(tmplStudy.getId())
//                    .setName(tmplStudy.getName())
//                    .setAlias(tmplStudy.getAlias())
//                    .setDescription(tmplStudy.getDescription())
//                    .setNotification(tmplStudy.getNotification())
//                    .setAttributes(tmplStudy.getAttributes())
//                    .setStatus(tmplStudy.getStatus() != null ? tmplStudy.getStatus().toCustomStatus() : null);
//
//            logger.info("Creating Study '{}'", tmplStudy.getId());
//            OpenCGAResult<Study> result = catalogManager.getStudyManager().create(projectId, study, QueryOptions.empty(), token);
//            fqn = result.first().getFqn();
//
//        } else {
        fqn = origStudy.getFqn();
        if (overwrite) {
            // Updating values
            StudyUpdateParams studyUpdateParams = new StudyUpdateParams(tmplStudy.getName(), tmplStudy.getAlias(), tmplStudy.getType(),
                    tmplStudy.getSources(), tmplStudy.getDescription(), tmplStudy.getCreationDate(), tmplStudy.getModificationDate(),
                    tmplStudy.getNotification(), tmplStudy.getStatus(), tmplStudy.getAdditionalInfo(), tmplStudy.getAttributes());

            logger.info("Study '{}' already exists. Updating the values.", tmplStudy.getId());
            try {
                catalogManager.getStudyManager().update(fqn, studyUpdateParams, QueryOptions.empty(), token);
            } catch (CatalogException e) {
                logger.warn(e.getMessage());
            }
        } else {
            logger.info("Study '{}' already exists.", tmplStudy.getId());
        }

        if (CollectionUtils.isNotEmpty(tmplStudy.getGroups())) {
            Set<String> existingGroups = origStudy != null
                    ? origStudy.getGroups().stream()
                    .flatMap(g -> Arrays.asList(g.getId(), g.getId().substring(1)).stream())
                    .collect(Collectors.toSet())
                    : new HashSet<>();
            for (GroupCreateParams group : tmplStudy.getGroups()) {
                if (existingGroups.contains(group.getId())) {
                    if (overwrite) {
                        logger.info("Updating users from group '{}'", group.getId());
                        catalogManager.getStudyManager().updateGroup(fqn, group.getId(), ParamUtils.BasicUpdateAction.ADD,
                                new GroupUpdateParams(group.getUsers()), token);
                    } else {
                        logger.info("Group '{}' already exists", group.getId());
                    }
                } else {
                    logger.info("Adding group '{}'", group.getId());
                    catalogManager.getStudyManager().createGroup(fqn, group.getId(), group.getUsers(), token);
                }
            }
        }
        if (CollectionUtils.isNotEmpty(tmplStudy.getVariableSets())) {
            Set<String> existingVariableSets = origStudy != null
                    ? origStudy.getVariableSets().stream().map(VariableSet::getId).collect(Collectors.toSet())
                    : new HashSet<>();
            // Create variable sets
            for (VariableSetCreateParams variableSetCreateParams : tmplStudy.getVariableSets()) {
                if (existingVariableSets.contains(variableSetCreateParams.getId())) {
                    logger.info("VariableSet '{}' already exists", variableSetCreateParams.getId());
                } else {
                    logger.info("Adding VariableSet '{}'", variableSetCreateParams.getId());
                    catalogManager.getStudyManager().createVariableSet(fqn, variableSetCreateParams.toVariableSet(), token);
                }
            }
        }
        if (CollectionUtils.isNotEmpty(tmplStudy.getAcl())) {
            // Set permissions
            for (StudyAclEntry studyAclEntry : tmplStudy.getAcl()) {
                logger.info("Setting permissions for '{}'", studyAclEntry.getMember());
                catalogManager.getStudyManager().updateAcl(fqn, studyAclEntry.getMember(),
                        new StudyAclParams(StringUtils.join(studyAclEntry.getPermissions(), ","), ""), ParamUtils.AclAction.SET, token);
            }
        }

        return fqn;
    }

//    protected String buildFqn(TemplateProject project, TemplateStudy study) {
//        return openCGAClient.getUserId() + "@" + project.getId() + ":" + study.getId();
//    }

    private void createIndividuals(String studyFqn, Path path) throws CatalogException {
        boolean hasParents = false;
        // Process/Create individuals without parents
        try (TemplateEntryIterator<IndividualUpdateParams> iterator =
                     new TemplateEntryIterator<>(path, "individuals", IndividualUpdateParams.class)) {
            int count = 0;
            while (iterator.hasNext()) {
                IndividualUpdateParams individual = iterator.next();

                Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), individual.getId());
                boolean exists = catalogManager.getIndividualManager().count(studyFqn, query, token).getNumMatches() > 0;

                if (exists && !resume) {
                    throw new CatalogException("Individual '" + individual.getId() + "' already exists. Do you want to resume the load?");
                }

                if (!hasParents && hasParents(individual)) {
                    hasParents = true;
                }

                // Remove parents and samples
                individual.setFather(null);
                individual.setMother(null);
                individual.setSamples(null);

                if (!exists) {
                    if (count == 0) {
                        logger.info("Creating individuals for study '{}'", studyFqn);
                    }

                    // Create individual
                    logger.info("Create individual '{}'", individual.getId());
                    catalogManager.getIndividualManager().create(studyFqn, individual.toIndividual(), QueryOptions.empty(), token);

                    count++;
                } else if (overwrite) {
                    String individualId = individual.getId();
                    // Remove individualId
                    individual.setId(null);

                    logger.info("Update individual '{}'", individual.getId());
                    catalogManager.getIndividualManager().update(studyFqn, individualId, individual, QueryOptions.empty(), token);

                    count++;
                }
            }
            if (count > 0) {
                logger.info("{} individuals processed for study '{}'", count, studyFqn);
            }
        }

        // Process parents
        if (hasParents) {
            try (TemplateEntryIterator<IndividualUpdateParams> iterator =
                         new TemplateEntryIterator<>(path, "individuals", IndividualUpdateParams.class)) {
                int count = 0;
                while (iterator.hasNext()) {
                    if (count == 0) {
                        logger.info("Processing individual parents for study '{}'", studyFqn);
                    }
                    IndividualUpdateParams individual = iterator.next();

                    if (hasParents(individual)) {
                        IndividualUpdateParams updateParams = new IndividualUpdateParams()
                                .setFather(individual.getFather())
                                .setMother(individual.getMother());
                        logger.info("Updating individual '{}' parents", individual.getId());

                        catalogManager.getIndividualManager().update(studyFqn, individual.getId(), updateParams, QueryOptions.empty(),
                                token);

                        count++;
                    }
                }
                if (count > 0) {
                    logger.info("Updated parents of {} individuals", count);
                }
            }
        }
    }

    private boolean hasParents(IndividualUpdateParams individual) {
        return (individual.getFather() != null && (StringUtils.isNotEmpty(individual.getFather().getId())
                || StringUtils.isNotEmpty(individual.getFather().getUuid())))
                || (individual.getMother() != null && (StringUtils.isNotEmpty(individual.getMother().getId())
                || StringUtils.isNotEmpty(individual.getMother().getUuid())));
    }

//    private void createIndividualsWithoutParents(String studyFqn, List<IndividualCreateParams> individuals) throws CatalogException {
//        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn);
//        Map<String, Individual> existing = Collections.emptyMap();
//        if (resume) {
//            openCGAClient.setThrowExceptionOnError(false);
//            existing = openCGAClient.getIndividualClient()
//                    .info(individuals.stream().map(IndividualCreateParams::getId).collect(Collectors.joining(",")),
//                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id"))
//                    .allResults()
//                    .stream()
//                    .collect(Collectors.toMap(Individual::getId, i -> i));
//            openCGAClient.setThrowExceptionOnError(true);
//        }
//
//
//        // Create individuals without parents and siblings
//        for (IndividualCreateParams individual : individuals) {
//            if (!existing.containsKey(individual.getId())) {
//                // Take parents
//                String father = individual.getFather();
//                String mother = individual.getMother();
//                individual.setFather(null);
//                individual.setMother(null);
//
//                openCGAClient.getIndividualClient().create(individual, params);
//
//                // Restore parents
//                individual.setFather(father);
//                individual.setMother(mother);
//            }
//        }
//    }

//    private void updateIndividualsParents(String studyFqn, List<IndividualCreateParams> individuals) throws CatalogException {
//        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn);
//        Map<String, Individual> existing = Collections.emptyMap();
//        if (resume) {
//            openCGAClient.setThrowExceptionOnError(false);
//            existing = openCGAClient.getIndividualClient()
//                    .info(individuals.stream().map(IndividualCreateParams::getId).collect(Collectors.joining(",")),
//                            new ObjectMap(params).append(QueryOptions.INCLUDE, "name,id,father.id,mother.id"))
//                    .allResults()
//                    .stream()
//                    .collect(Collectors.toMap(Individual::getId, i -> i));
//            openCGAClient.setThrowExceptionOnError(true);
//        }
//
//        // Update parents and siblings for each individual
//        for (IndividualCreateParams individual : individuals) {
//            IndividualUpdateParams updateParams = new IndividualUpdateParams();
//            boolean empty = true;
//            Individual existingIndividual = existing.get(individual.getId());
//            String father = individual.getFather();
//            if (father != null) {
//                if (existingIndividual == null || existingIndividual.getFather() == null) {
//                    // Only if father does not exist already for this individual
//                    updateParams.setFather(father);
//                    empty = false;
//                }
//            }
//            String mother = individual.getMother();
//            if (mother != null) {
//                if (existingIndividual == null || existingIndividual.getMother() == null) {
//                    // Only if mother does not exist already for this individual
//                    updateParams.setMother(mother);
//                    empty = false;
//                }
//            }
//            if (!empty) {
//                openCGAClient.getIndividualClient().update(individual.getId(), updateParams, params);
//            }
//        }
//    }

    private void createSamples(String studyFqn, Path path) throws CatalogException {
        // Process/Create samples
        try (TemplateEntryIterator<SampleUpdateParams> iterator =
                     new TemplateEntryIterator<>(path, "samples", SampleUpdateParams.class)) {
            int count = 0;
            while (iterator.hasNext()) {
                SampleUpdateParams sample = iterator.next();

                Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), sample.getId());
                boolean exists = catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches() > 0;

                if (exists && !resume) {
                    throw new CatalogException("Sample '" + sample.getId() + "' already exists. Do you want to resume the load?");
                }

                if (!exists) {
                    if (count == 0) {
                        logger.info("Creating samples for study '{}'", studyFqn);
                    }
                    // Create sample
                    logger.info("Create sample '{}'", sample.getId());
                    catalogManager.getSampleManager().create(studyFqn, sample.toSample(), QueryOptions.empty(), token);

                    count++;
                } else if (overwrite) {
                    String sampleId = sample.getId();
                    // Remove sampleId
                    sample.setId(null);

                    logger.info("Update sample '{}'", sample.getId());
                    catalogManager.getSampleManager().update(studyFqn, sampleId, sample, QueryOptions.empty(), token);

                    count++;
                }
            }
            if (count > 0) {
                logger.info("{} samples processed for study '{}'", count, studyFqn);
            }
        }
    }

    private void createCohorts(String studyFqn, Path path) throws CatalogException {
        // Process/Create cohorts
        try (TemplateEntryIterator<CohortUpdateParams> iterator =
                     new TemplateEntryIterator<>(path, "cohorts", CohortUpdateParams.class)) {
            int count = 0;
            while (iterator.hasNext()) {
                CohortUpdateParams cohort = iterator.next();

                Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), cohort.getId());
                boolean exists = catalogManager.getCohortManager().count(studyFqn, query, token).getNumMatches() > 0;

                if (exists && !resume) {
                    throw new CatalogException("Cohort '" + cohort.getId() + "' already exists. Do you want to resume the load?");
                }

                if (!exists) {
                    if (count == 0) {
                        logger.info("Creating cohorts for study '{}'", studyFqn);
                    }

                    // Create cohort
                    logger.info("Create cohort '{}'", cohort.getId());
                    catalogManager.getCohortManager().create(studyFqn, cohort.toCohort(), QueryOptions.empty(), token);

                    count++;
                } else if (overwrite) {
                    String cohortId = cohort.getId();
                    // Remove cohortId
                    cohort.setId(null);

                    logger.info("Update cohort '{}'", cohort.getId());
                    catalogManager.getCohortManager().update(studyFqn, cohortId, cohort, QueryOptions.empty(), token);

                    count++;
                }
            }
            if (count > 0) {
                logger.info("{} cohorts processed for study '{}'", count, studyFqn);
            }
        }
    }

    private void createFamilies(String studyFqn, Path path) throws CatalogException {
        // Process/Create families
        try (TemplateEntryIterator<FamilyUpdateParams> iterator =
                     new TemplateEntryIterator<>(path, "families", FamilyUpdateParams.class)) {
            int count = 0;
            while (iterator.hasNext()) {
                FamilyUpdateParams family = iterator.next();

                Query query = new Query(FamilyDBAdaptor.QueryParams.ID.key(), family.getId());
                boolean exists = catalogManager.getFamilyManager().count(studyFqn, query, token).getNumMatches() > 0;

                if (exists && !resume) {
                    throw new CatalogException("Family '" + family.getId() + "' already exists. Do you want to resume the load?");
                }

                if (!exists) {
                    if (count == 0) {
                        logger.info("Creating families for study '{}'", studyFqn);
                    }

                    // Create family
                    logger.info("Create family '{}'", family.getId());
                    Family completeFamily = family.toFamily();
                    if (CollectionUtils.isNotEmpty(completeFamily.getMembers())) {
                        List<String> memberIds = completeFamily.getMembers().stream().map(Individual::getId).collect(Collectors.toList());
                        completeFamily.setMembers(null);
                        catalogManager.getFamilyManager().create(studyFqn, completeFamily, memberIds, QueryOptions.empty(), token);
                    } else {
                        catalogManager.getFamilyManager().create(studyFqn, family.toFamily(), QueryOptions.empty(), token);
                    }

                    count++;
                } else if (overwrite) {
                    String familyId = family.getId();
                    // Remove familyId
                    family.setId(null);

                    logger.info("Update family '{}'", family.getId());
                    catalogManager.getFamilyManager().update(studyFqn, familyId, family, QueryOptions.empty(), token);

                    count++;
                }
            }
            if (count > 0) {
                logger.info("{} families processed for study '{}'", count, studyFqn);
            }
        }
    }

    private void createPanels(String studyFqn, Path path) throws CatalogException {
        // Process/Create panels
        try (TemplateEntryIterator<PanelUpdateParams> iterator =
                     new TemplateEntryIterator<>(path, "panels", PanelUpdateParams.class)) {
            int count = 0;
            while (iterator.hasNext()) {
                PanelUpdateParams panel = iterator.next();

                Query query = new Query(PanelDBAdaptor.QueryParams.ID.key(), panel.getId());
                boolean exists = catalogManager.getPanelManager().count(studyFqn, query, token).getNumMatches() > 0;

                if (exists && !resume) {
                    throw new CatalogException("Panel '" + panel.getId() + "' already exists. Do you want to resume the load?");
                }

                if (!exists) {
                    if (count == 0) {
                        logger.info("Creating panels for study '{}'", studyFqn);
                    }

                    // Create family
                    logger.info("Create panel '{}'", panel.getId());
                    catalogManager.getPanelManager().create(studyFqn, panel.toPanel(), QueryOptions.empty(), token);

                    count++;
                } else if (overwrite) {
                    String panelId = panel.getId();
                    // Remove panelId
                    panel.setId(null);

                    logger.info("Update panel '{}'", panel.getId());
                    catalogManager.getPanelManager().update(studyFqn, panelId, panel, QueryOptions.empty(), token);

                    count++;
                }
            }
            if (count > 0) {
                logger.info("{} panels processed for study '{}'", count, studyFqn);
            }
        }
    }

    private void createClinicalAnalyses(String studyFqn, Path path) throws CatalogException {
        // Process/Create Clinical Anlyses
        try (TemplateEntryIterator<ClinicalAnalysisUpdateParams> iterator =
                     new TemplateEntryIterator<>(path, "clinical", ClinicalAnalysisUpdateParams.class)) {
            int count = 0;
            while (iterator.hasNext()) {
                ClinicalAnalysisUpdateParams clinical = iterator.next();

                Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), clinical.getId());
                boolean exists = catalogManager.getClinicalAnalysisManager().count(studyFqn, query, token).getNumMatches() > 0;

                if (exists && !resume) {
                    throw new CatalogException("Clinical Analysis '" + clinical.getId()
                            + "' already exists. Do you want to resume the load?");
                }

                if (!exists) {
                    if (count == 0) {
                        logger.info("Creating Clinical Analyses for study '{}'", studyFqn);
                    }

                    // Create Clinical Analysis
                    logger.info("Create Clinical Analysis '{}'", clinical.getId());
                    catalogManager.getClinicalAnalysisManager().create(studyFqn, clinical.toClinicalAnalysis(), QueryOptions.empty(),
                            token);

                    count++;
                } else if (overwrite) {
                    String clinicalId = clinical.getId();
                    // Remove clinicalAnalysisId
                    clinical.setId(null);

                    logger.info("Update Clinical Analysis '{}'", clinical.getId());
                    catalogManager.getClinicalAnalysisManager().update(studyFqn, clinicalId, clinical, QueryOptions.empty(), token);

                    count++;
                }
            }
            if (count > 0) {
                logger.info("{} clinical analyses processed for study '{}'", count, studyFqn);
            }
        }
    }

    private void createFiles(String studyFqn, Path path) throws CatalogException {
        // Process/Create Files
        try (TemplateEntryIterator<TemplateFile> iterator =
                     new TemplateEntryIterator<>(path, "files", TemplateFile.class)) {
            int count = 0;
            while (iterator.hasNext()) {
                TemplateFile file = iterator.next();

                if (StringUtils.isEmpty(file.getPath())) {
                    throw new CatalogException("Missing mandatory parameter 'path'");
                }
                Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), file.getPath());
                boolean exists = catalogManager.getFileManager().count(studyFqn, query, token).getNumMatches() > 0;

                if (exists && !resume) {
                    throw new CatalogException("File '" + file.getPath() + "' already exists. Do you want to resume the load?");
                }

                boolean incomplete = false;
                if (!exists) {
                    if (StringUtils.isEmpty(file.getUri())) {
                        throw new CatalogException("Missing mandatory parameter 'uri'. Could not link file.");
                    }

                    if (count == 0) {
                        logger.info("Creating File for study '{}'", studyFqn);
                    }

                    // Create File
                    logger.info("Create File '{}'", file.getPath());
                    catalogManager.getFileManager().link(studyFqn,
                            new FileLinkParams(file.getUri(), file.getPath(), file.getDescription(), file.getCreationDate(),
                                    file.getModificationDate(), file.getRelatedFiles(), file.getStatus(), null), true, token);
                    incomplete = true;
                }

                if (incomplete || overwrite) {
                    if (overwrite && !incomplete) {
                        logger.info("Update Clinical Analysis '{}'", file.getPath());
                    }
                    catalogManager.getFileManager().update(studyFqn, file.getPath(), file, QueryOptions.empty(), token);

                    count++;
                }
            }
            if (count > 0) {
                logger.info("{} files processed for study '{}'", count, studyFqn);
            }
        }
    }


//    private void configureVariantEngine(String studyFqn, TemplateStudy study) throws CatalogException {
//        StudyVariantEngineConfiguration configuration = study.getVariantEngineConfiguration();
//        if (configuration == null) {
//            return;
//        }
//        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn);
//        if (configuration.getOptions() != null && !configuration.getOptions().isEmpty()) {
//            openCGAClient.getVariantOperationClient().configureVariant(configuration.getOptions(), params);
//        }
//        if (configuration.getSampleIndex() != null) {
//            openCGAClient.getVariantOperationClient().configureSampleIndex(configuration.getSampleIndex(), params);
//        }
//
//    }

//    private List<String> fetchFiles(TemplateConfiguration template, String studyFqn, TemplateStudy study)
//            throws CatalogException {
//        List<TemplateFile> files = study.getFiles();
//        if (CollectionUtils.isEmpty(files)) {
//            return Collections.emptyList();
//        }
//        List<String> indexVcfJobIds = new ArrayList<>(files.size());
//        for (List<TemplateFile> batch : batches(files)) {
//            indexVcfJobIds.addAll(fetchFiles(template, studyFqn, study, batch));
//        }
//        return indexVcfJobIds;
//    }
//
//    private List<String> fetchFiles(TemplateConfiguration template, String studyFqn, TemplateStudy study, List<TemplateFile> files)
//            throws CatalogException {
//        URI baseUrl = getBaseUrl(template, study);
//        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn);
//        Set<String> existing = new HashSet<>();
//        Set<String> existingAndIndexed = new HashSet<>();
//        if (resume) {
//            openCGAClient.setThrowExceptionOnError(false);
//            openCGAClient.getFileClient()
//                    .info(files.stream().map(file -> getFilePath(file).replace('/', ':')).collect(Collectors.joining(",")),
//                            new ObjectMap(params).append(QueryOptions.INCLUDE, "path,name,id,internal"))
//                    .allResults()
//                    .forEach(file -> {
//                        existing.add(file.getPath());
//                        if (file.getInternal() != null) {
//                            if (file.getInternal().getIndex() != null) {
//                                if (file.getInternal().getIndex().getStatus() != null) {
//                                    if (Status.READY.equals(file.getInternal().getIndex().getStatus().getName())) {
//                                        existingAndIndexed.add(file.getPath());
//                                    }
//                                }
//                            }
//                        }
//                    });
//            openCGAClient.setThrowExceptionOnError(true);
//        }
//        List<String> indexVcfJobIds = new ArrayList<>();
//        for (TemplateFile file : files) {
//            file.setPath(getFilePath(file));
//            file.setName(Paths.get(file.getPath()).getFileName().toString());
//            String fileId = file.getPath().replace("/", ":");
//
//            List<String> jobs = Collections.emptyList();
//            if (!existing.contains(getFilePath(file))) {
//                jobs = fetchFile(baseUrl, params, file);
//            } else {
//                updateFileIfChanged(fileId, file, params);
//            }
//            if (template.isIndex()) {
//                if (isVcf(file) && !existingAndIndexed.contains(getFilePath(file))) {
//                    indexVcfJobIds.add(indexVcf(studyFqn, study, getFilePath(file), jobs));
//                }
//            }
//        }
//        return indexVcfJobIds;
//    }

//    private String getFilePath(TemplateFile file) {
//        String path = file.getPath() == null ? "" : file.getPath();
//        String name = file.getName() == null ? "" : file.getName();
//        if (StringUtils.isEmpty(name) || path.endsWith(name)) {
//            return path;
//        }
//        return path + (path.endsWith("/") ? "" : "/") + name;
//    }
//
//    private List<String> fetchFile(URI baseUrl, ObjectMap params, TemplateFile file) throws CatalogException {
//        List<String> jobDependsOn;
//
//        String parentPath;
//        if (file.getPath().contains("/")) {
//            parentPath = Paths.get(file.getPath()).getParent().toString();
//        } else {
//            parentPath = "";
//        }
//        logger.info("Process file " + file.getName());
//
//        URI fileUri;
//        if (file.getUri() == null) {
//            fileUri = baseUrl.resolve(file.getName());
//        } else {
//            fileUri = URI.create(file.getUri());
//        }
//
//        if (!fileUri.getScheme().equals("file")) {
//            // Fetch file
//            String fetchUrl;
//            if (file.getUri() == null) {
//                fetchUrl = baseUrl + file.getName();
//            } else {
//                fetchUrl = file.getUri();
//            }
//            FileFetch fileFetch = new FileFetch(fetchUrl, parentPath);
//            String fetchJobId = checkJob(openCGAClient.getFileClient().fetch(fileFetch, params));
//            jobDependsOn = Collections.singletonList(fetchJobId);
//            // TODO: Other fields are not being updated!
//        } else {
//            // Link file
//            if (StringUtils.isNotEmpty(parentPath)) {
//                FileCreateParams createFolder = new FileCreateParams(parentPath, null, null, true, true);
//                openCGAClient.getFileClient().create(createFolder, params);
//            }
//            FileLinkParams data = new FileLinkParams(fileUri.toString(), parentPath,
//                    file.getDescription(), file.getRelatedFiles(), file.getStatus(), null);
//            String fileId = openCGAClient.getFileClient().link(data, params).firstResult().getId();
//            updateFile(fileId, file, params);
//            jobDependsOn = Collections.emptyList();
//        }
//        return jobDependsOn;
//    }
//
//    private void updateFileIfChanged(String fileId, TemplateFile file, ObjectMap params) throws CatalogException {
//        // TODO: Check if update is needed!
//        updateFile(fileId, file, params);
//    }
//
//    private void updateFile(String fileId, TemplateFile file, ObjectMap params) throws CatalogException {
//        openCGAClient.getFileClient().update(fileId, new FileUpdateParams(
//                file.getName(),
//                file.getDescription(),
//                file.getSampleIds(),
//                file.getChecksum(),
//                file.getFormat(),
//                file.getBioformat(),
//                file.getSoftware(),
//                file.getExperiment(),
//                file.getTags(),
//                file.getInternal(),
//                file.getSize(),
//                file.getRelatedFiles(),
//                file.getStatus(),
//                file.getAnnotationSets(),
//                file.getQualityControl(),
//                file.getStats(),
//                file.getAttributes()
//        ), params);
//    }

//    private URI getBaseUrl(TemplateConfiguration template, TemplateStudy study) {
//        String baseUrl = template.getBaseUrl();
//        if (StringUtils.isEmpty(baseUrl)) {
//            return null;
//        }
//        baseUrl = baseUrl.replaceAll("STUDY_ID", study.getId());
//        if (!baseUrl.endsWith("/")) {
//            baseUrl = baseUrl + "/";
//        }
//        return URI.create(baseUrl);
//    }

//    private boolean isVcf(TemplateFile file) {
//        String path = getFilePath(file);
//        return path.endsWith(".vcf.gz") || path.endsWith(".vcf")
//                || path.endsWith(".gvcf.gz") || path.endsWith(".gvcf");
//    }
//
//    private String indexVcf(String fqn, TemplateStudy study, String file, List<String> jobDependsOn) throws CatalogException {
//        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, fqn);
//        params.put(ParamConstants.JOB_DEPENDS_ON, jobDependsOn);
//        VariantIndexParams variantIndexParams = new VariantIndexParams().setFile(file);
//        if (study.getAttributes() != null) {
//            Object o = study.getAttributes().get("variant-index-run");
//            if (o instanceof Map) {
//                variantIndexParams.updateParams(new HashMap<>(((Map) o)));
//            }
//        }
//        return checkJob(openCGAClient.getVariantClient()
//                .runIndex(variantIndexParams, params));
//    }
//
//    private void postIndex(TemplateProject project, List<String> indexVcfJobIds, List<String> statsJobIds) throws CatalogException {
//        List<String> studies = getVariantStudies(project);
//        if (!studies.isEmpty()) {
//            List<String> jobs = new ArrayList<>(statsJobIds);
//            jobs.add(variantAnnot(project, indexVcfJobIds));
//            variantSecondaryIndex(project, jobs);
//        }
//    }
//
//    private String variantStats(String fqn, TemplateStudy study, List<String> indexVcfJobIds) throws CatalogException {
//        if (resume) {
//            Cohort cohort = openCGAClient.getCohortClient()
//                    .info(StudyEntry.DEFAULT_COHORT, new ObjectMap(ParamConstants.STUDY_PARAM, fqn))
//                    .firstResult();
//            if (cohort != null) {
//                if (cohort.getInternal() != null && cohort.getInternal().getStatus() != null) {
//                    if (Status.READY.equals(cohort.getInternal().getStatus().getName())) {
//                        logger.info("Variant stats already calculated. Skip");
//                        return null;
//                    }
//                }
//            }
//        }
//
//        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, fqn)
//                .append(ParamConstants.JOB_DEPENDS_ON, indexVcfJobIds);
//        VariantStatsIndexParams data = new VariantStatsIndexParams();
//
//        data.setAggregated(Aggregation.NONE);
//        for (TemplateFile file : study.getFiles()) {
//            if (file.getName().endsWith(".properties")
//                    && file.getAttributes() != null
//                    && Boolean.parseBoolean(String.valueOf(file.getAttributes().get("aggregationMappingFile")))) {
//                data.setAggregationMappingFile(getFilePath(file));
//                data.setAggregated(Aggregation.BASIC);
//            }
//        }
//
//        if (study.getAttributes() != null) {
//            Object aggregationObj = study.getAttributes().get("aggregation");
//            if (aggregationObj != null) {
//                data.setAggregated(AggregationUtils.valueOf(aggregationObj.toString()));
//            }
//        }
//
//        if (data.getAggregated().equals(Aggregation.NONE)) {
//            data.setCohort(Collections.singletonList(StudyEntry.DEFAULT_COHORT));
//        }
//
//        return checkJob(openCGAClient.getVariantOperationClient()
//                .indexVariantStats(data, params));
//    }
//
//    private String variantAnnot(TemplateProject project, List<String> indexVcfJobIds) throws CatalogException {
//        ObjectMap params = new ObjectMap()
//                .append(ParamConstants.PROJECT_PARAM, project.getId())
//                .append(ParamConstants.JOB_DEPENDS_ON, indexVcfJobIds);
//        return checkJob(openCGAClient.getVariantOperationClient()
//                .indexVariantAnnotation(new VariantAnnotationIndexParams(), params));
//    }
//
//    private void variantSecondaryIndex(TemplateProject project, List<String> jobs) throws CatalogException {
//        ObjectMap params = new ObjectMap()
//                .append(ParamConstants.PROJECT_PARAM, project.getId())
//                .append(ParamConstants.JOB_DEPENDS_ON, jobs);
//        checkJob(openCGAClient.getVariantOperationClient()
//                .secondaryIndexVariant(new VariantSecondaryIndexParams().setOverwrite(true), params));
//    }
//
//    private List<String> getVariantStudies(TemplateProject project) {
//        return project.getStudies()
//                .stream()
//                .filter(this::isVariantStudy)
//                .map(s -> buildFqn(project, s))
//                .collect(Collectors.toList());
//    }
//
//    private boolean isVariantStudy(TemplateStudy study) {
//        return study.getFiles() != null
//                && study.getFiles().stream().anyMatch(this::isVcf);
//    }

    private String checkJob(RestResponse<Job> jobRestResponse) throws CatalogException {
        Job job = jobRestResponse.firstResult();
        String status = job.getInternal().getStatus().getId();
        if (status.equals(Enums.ExecutionStatus.ABORTED) || status.equals(Enums.ExecutionStatus.ERROR)) {
            throw new CatalogException("Error submitting job " + job.getTool().getId() + " " + job.getId());
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
