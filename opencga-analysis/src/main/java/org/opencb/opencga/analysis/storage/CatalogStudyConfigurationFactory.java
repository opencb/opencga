
/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.analysis.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogStudyConfigurationFactory {

    public static final QueryOptions ALL_FILES_QUERY_OPTIONS = new QueryOptions()
            .append(CatalogFileDBAdaptor.FileFilterOption.bioformat.toString(), Arrays.asList(File.Bioformat.VARIANT, File.Bioformat.ALIGNMENT))
            .append("include", Arrays.asList("projects.studies.files.id", "projects.studies.files.name", "projects.studies.files.path",
                    "projects.studies.files.sampleIds", "projects.studies.files.attributes.variantSource.metadata.variantFileHeader"));
    public static final QueryOptions INDEXED_FILES_QUERY_OPTIONS = new QueryOptions()
            .append(CatalogFileDBAdaptor.FileFilterOption.index.toString() + ".status", Index.Status.READY)
            .append("include", Arrays.asList("projects.studies.files.id", "projects.studies.files.name", "projects.studies.files.path"));
    public static final QueryOptions SAMPLES_QUERY_OPTIONS = new QueryOptions("include", Arrays.asList("projects.studies.samples.id", "projects.studies.samples.name"));
    public static final QueryOptions COHORTS_QUERY_OPTIONS = new QueryOptions();
    public static final QueryOptions INVALID_COHORTS_QUERY_OPTIONS = new QueryOptions()
            .append(CatalogSampleDBAdaptor.CohortFilterOption.status.toString(), Cohort.Status.INVALID)
            .append("include", Arrays.asList("projects.studies.cohorts.name", "projects.studies.cohorts.id", "projects.studies.cohorts.status"));
    protected static Logger logger = LoggerFactory.getLogger(CatalogStudyConfigurationFactory.class);

    private final CatalogManager catalogManager;

    public static final String STUDY_CONFIGURATION_FIELD = "studyConfiguration";
    public static final QueryOptions STUDY_QUERY_OPTIONS = new QueryOptions("include", Arrays.asList(
            "projects.studies.id",
            "projects.studies.alias",
            "projects.studies.attributes." + STUDY_CONFIGURATION_FIELD,
            "projects.studies.attributes." + VariantStorageManager.Options.AGGREGATED_TYPE.key()
    ));
    private final ObjectMapper objectMapper;
    private QueryOptions options;


    public CatalogStudyConfigurationFactory(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        objectMapper = new ObjectMapper();
    }

    public StudyConfiguration getStudyConfiguration(int studyId, QueryOptions options, String sessionId) throws CatalogException {
        return getStudyConfiguration(studyId, null, options, sessionId);
    }

    public StudyConfiguration getStudyConfiguration(int studyId, StudyConfigurationManager studyConfigurationManager, QueryOptions options, String sessionId) throws CatalogException {
        Study study = catalogManager.getStudy(studyId, sessionId, STUDY_QUERY_OPTIONS).first();
        StudyConfiguration studyConfiguration = null;
        if (studyConfigurationManager != null) {
            studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, options).first();
        }
        studyConfiguration = fillStudyConfiguration(studyConfiguration, study, sessionId);

        return studyConfiguration;
    }

    private StudyConfiguration fillStudyConfiguration(StudyConfiguration studyConfiguration, Study study, String sessionId) throws CatalogException {
        int studyId = study.getId();
        boolean newStudyConfiguration = false;
        if (studyConfiguration == null) {
            studyConfiguration = new StudyConfiguration(0, "");
            newStudyConfiguration = true;
        }
        studyConfiguration.setStudyId(study.getId());
        int projectId = catalogManager.getProjectIdByStudyId(study.getId());
        String projectAlias = catalogManager.getProject(projectId, null, sessionId).first().getAlias();
        String userId = catalogManager.getUserIdByProjectId(projectId);
        studyConfiguration.setStudyName(userId + "@" + projectAlias + ":" + study.getAlias());

        fillNullMaps(studyConfiguration);

        //Clear maps
//        studyConfiguration.getIndexedFiles().clear();
//        studyConfiguration.getFileIds().clear();
//        studyConfiguration.getSamplesInFiles().clear();
//        studyConfiguration.getHeaders().clear();
//        studyConfiguration.getSampleIds().clear();
//        studyConfiguration.getCalculatedStats().clear();
//        studyConfiguration.getInvalidStats().clear();
//        studyConfiguration.getCohortIds().clear();
//        studyConfiguration.getCohorts().clear();

        if (study.getAttributes().containsKey(VariantStorageManager.Options.AGGREGATED_TYPE.key())) {
            logger.debug("setting study aggregation to {}", study.getAttributes().get(VariantStorageManager.Options.AGGREGATED_TYPE.key()).toString());
            studyConfiguration.setAggregation(VariantSource.Aggregation.valueOf(
                    study.getAttributes().get(VariantStorageManager.Options.AGGREGATED_TYPE.key()).toString()));
        } else {
            studyConfiguration.setAggregation(VariantSource.Aggregation.NONE);
        }
        logger.debug("studyConfiguration aggregation: {}", studyConfiguration.getAggregation());

        // DO NOT update "indexed files" list. This MUST be modified only by storage.
        // This field will never be modified from catalog to storage
        // *** Except if it is a new StudyConfiguration...
        if (newStudyConfiguration) {
            for (File file : catalogManager.getAllFiles(studyId, INDEXED_FILES_QUERY_OPTIONS, sessionId).getResult()) {
                studyConfiguration.getIndexedFiles().add(file.getId());
            }
        }

        logger.debug("Get Files");
        QueryResult<File> files = catalogManager.getAllFiles(studyId, ALL_FILES_QUERY_OPTIONS, sessionId);
        for (File file : files.getResult()) {

            studyConfiguration.getFileIds().forcePut(file.getName(), file.getId());
            studyConfiguration.getSamplesInFiles().put(file.getId(), new LinkedHashSet<>(file.getSampleIds()));


            if (studyConfiguration.getIndexedFiles().contains(file.getId()) && file.getAttributes().containsKey("variantSource")) {
                //attributes.variantSource.metadata.variantFileHeader
                Object object = file.getAttributes().get("variantSource");
                if (object instanceof Map) {
                    Map variantSource = ((Map) object);
                    object = variantSource.get("metadata");
                    if (object instanceof Map) {
                        Map metadata = (Map) object;
                        if (metadata.containsKey("variantFileHeader")) {
                            String variantFileHeader = metadata.get("variantFileHeader").toString();
                            studyConfiguration.getHeaders().put(file.getId(), variantFileHeader);
                        }
                    }
                }
            }
        }

        logger.debug("Get Samples");
        QueryResult<Sample> samples = catalogManager.getAllSamples(studyId, SAMPLES_QUERY_OPTIONS, sessionId);

        for (Sample sample : samples.getResult()) {
            studyConfiguration.getSampleIds().forcePut(sample.getName(), sample.getId());
        }

        logger.debug("Get Cohorts");
        QueryResult<Cohort> cohorts = catalogManager.getAllCohorts(studyId, COHORTS_QUERY_OPTIONS, sessionId);

        for (Cohort cohort : cohorts.getResult()) {
            studyConfiguration.getCohortIds().forcePut(cohort.getName(), cohort.getId());
            studyConfiguration.getCohorts().put(cohort.getId(), new HashSet<>(cohort.getSamples()));
            if (cohort.getStatus() == Cohort.Status.READY) {
                studyConfiguration.getCalculatedStats().add(cohort.getId());
                studyConfiguration.getInvalidStats().remove(cohort.getId());
            } else if (cohort.getStatus() == Cohort.Status.INVALID) {
                studyConfiguration.getCalculatedStats().remove(cohort.getId());
                studyConfiguration.getInvalidStats().add(cohort.getId());
            } else { //CALCULATING || NONE
                studyConfiguration.getCalculatedStats().remove(cohort.getId());
                studyConfiguration.getInvalidStats().remove(cohort.getId());
            }
        }

        return studyConfiguration;
    }

    private void fillNullMaps(StudyConfiguration studyConfiguration) {
        if (studyConfiguration.getFileIds() == null) {
            studyConfiguration.setFileIds(new HashMap<>());
        }
        if (studyConfiguration.getSamplesInFiles() == null) {
            studyConfiguration.setSamplesInFiles(new HashMap<>());
        }
        if (studyConfiguration.getSampleIds() == null) {
            studyConfiguration.setSampleIds(new HashMap<>());
        }
        if (studyConfiguration.getCohortIds() == null) {
            studyConfiguration.setCohortIds(new HashMap<>());
        }
        if (studyConfiguration.getCohorts() == null) {
            studyConfiguration.setCohorts(new HashMap<>());
        }
        if (studyConfiguration.getAttributes() == null) {
            studyConfiguration.setAttributes(new ObjectMap());
        }
    }

    public void updateStudyConfigurationFromCatalog(int studyId, StudyConfigurationManager studyConfigurationManager, String sessionId) throws CatalogException {
        StudyConfiguration studyConfiguration = getStudyConfiguration(studyId, studyConfigurationManager, new QueryOptions(), sessionId);
        studyConfigurationManager.updateStudyConfiguration(studyConfiguration, new QueryOptions());
    }

    public void updateCatalogFromStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options, String sessionId) throws CatalogException {
        if (options == null) {
            options = this.options;
        }
        logger.info("Updating StudyConfiguration " + studyConfiguration.getStudyId());

        //Check if any cohort stat has been updated
        if (!studyConfiguration.getCalculatedStats().isEmpty()) {
            for (Cohort cohort : catalogManager.getAllCohorts(studyConfiguration.getStudyId(),
                    new QueryOptions(CatalogSampleDBAdaptor.CohortFilterOption.id.toString(), new ArrayList<>(studyConfiguration.getCalculatedStats())), sessionId).getResult()) {
                if (cohort.getStatus() == null || !cohort.getStatus().equals(Cohort.Status.READY)) {
                    logger.debug("Cohort \"{}\":{} change status from {} to {}", cohort.getName(), cohort.getId(), cohort.getStats(), Cohort.Status.READY);
                    catalogManager.modifyCohort(cohort.getId(), new ObjectMap("status", Cohort.Status.READY), sessionId);
                }
            }
        }

        //Check if any cohort stat has been invalidated
        if (!studyConfiguration.getInvalidStats().isEmpty()) {
            for (Cohort cohort : catalogManager.getAllCohorts(studyConfiguration.getStudyId(),
                    new QueryOptions(CatalogSampleDBAdaptor.CohortFilterOption.id.toString(), new ArrayList<>(studyConfiguration.getInvalidStats())), sessionId).getResult()) {
                if (cohort.getStatus() == null || !cohort.getStatus().equals(Cohort.Status.INVALID)) {
                    logger.debug("Cohort \"{}\":{} change status from {} to {}", cohort.getName(), cohort.getId(), cohort.getStats(), Cohort.Status.INVALID);
                    catalogManager.modifyCohort(cohort.getId(), new ObjectMap("status", Cohort.Status.INVALID), sessionId);
                }
            }
        }

        if (!studyConfiguration.getIndexedFiles().isEmpty()) {
            for (File file : catalogManager.getAllFiles(studyConfiguration.getStudyId(),
                    new QueryOptions("id", new ArrayList<>(studyConfiguration.getIndexedFiles())), sessionId).getResult()) {
                if (file.getIndex() == null || !file.getIndex().getStatus().equals(Index.Status.READY)) {
                    final Index index;
                    index = file.getIndex() == null ? new Index() : file.getIndex();
                    index.setStatus(Index.Status.READY);
                    logger.debug("File \"{}\":{} change status from {} to {}", file.getName(), file.getId(), file.getIndex().getStatus(), Index.Status.READY);
                    catalogManager.modifyFile(file.getId(), new ObjectMap("index", index), sessionId);
                }
            }
        }
    }

}
