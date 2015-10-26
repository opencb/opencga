
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
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogStudyConfigurationManager extends StudyConfigurationManager {
    public static final String CATALOG_PROPERTIES_FILE = "catalogPropertiesFile";

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
    protected static Logger logger = LoggerFactory.getLogger(CatalogStudyConfigurationManager.class);

    private final CatalogManager catalogManager;
    private final String sessionId;

    public static final String STUDY_CONFIGURATION_FIELD = "studyConfiguration";
    public static final QueryOptions STUDY_QUERY_OPTIONS = new QueryOptions("include", Arrays.asList(
            "projects.studies.id",
            "projects.studies.alias",
            "projects.studies.attributes." + STUDY_CONFIGURATION_FIELD,
            "projects.studies.attributes." + VariantStorageManager.Options.AGGREGATED_TYPE.key()
    ));
    private final ObjectMapper objectMapper;
    private QueryOptions options;

    public CatalogStudyConfigurationManager(ObjectMap objectMap) throws CatalogException {
        super(objectMap);
        Properties catalogProperties = null;
        if (objectMap.containsKey(CATALOG_PROPERTIES_FILE)) {
            try {
                catalogProperties = new Properties();
                catalogProperties.load(new FileInputStream(objectMap.getString(CATALOG_PROPERTIES_FILE)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (catalogProperties == null){
            if (Config.getOpenCGAHome() == null || Config.getOpenCGAHome().isEmpty()) {
                Config.setOpenCGAHome();
            }
            catalogProperties = Config.getCatalogProperties();
        }
        catalogManager = new CatalogManager(catalogProperties);
        sessionId = objectMap.getString("sessionId");
        objectMapper = new ObjectMapper();
    }

    public CatalogStudyConfigurationManager(CatalogManager catalogManager, String sessionId) {
        super(null);
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
        objectMapper = new ObjectMapper();
    }

    @Override
    public void setDefaultQueryOptions(QueryOptions options) {
        super.setDefaultQueryOptions(options);
        this.options = options;
    }

    @Override
    protected QueryResult<StudyConfiguration> _getStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
        return _getStudyConfiguration(null, studyName, timeStamp, options);
    }

    @Override
    protected QueryResult<StudyConfiguration> _getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        return _getStudyConfiguration(studyId, null, timeStamp, options);
    }

    private QueryResult<StudyConfiguration> _getStudyConfiguration(Integer studyId, String studyName, Long timeStamp, QueryOptions options) {
        if (options == null) {
            options = this.options;
        } else if (this.options != null) {
            for (Map.Entry<String, Object> entry : this.options.entrySet()) {
                options.add(entry.getKey(), entry.getValue());
            }
        }

        // We need a valid sessionId. This can be passed in the constructor or in the options.
        // If it is not valid then ~/.opencga/opencga.yml is examined
        String sessionId = (options == null) ? this.sessionId : options.getString("sessionId", this.sessionId);
        if (sessionId == null || sessionId.isEmpty()) {
            sessionId = getLocalSessionId();
        }

        StudyConfiguration studyConfiguration = null;
        long start = System.currentTimeMillis();
        try {
            if (studyId == null) {
                studyId = catalogManager.getStudyId(studyName);
            }
            logger.debug("Reading StudyConfiguration from Catalog. study: {}, studyId: {}", studyName, studyId);
            logger.debug("CatalogStudyConfigurationManager - options = '{}'", ((options == null) ? null : options.toJson()));
            Study study = catalogManager.getStudy(studyId, sessionId, STUDY_QUERY_OPTIONS).first();
            studyConfiguration = new StudyConfiguration(studyId, study.getAlias());

            Object o = study.getAttributes().get(STUDY_CONFIGURATION_FIELD);
            if (o != null && o instanceof Map) {
                ObjectMapper objectMapper = new ObjectMapper();
                studyConfiguration = objectMapper.readValue(objectMapper.writeValueAsString(o), StudyConfiguration.class);
                logger.trace("Read StudyConfiguration from catalog");
                if (timeStamp != null && Objects.equals(studyConfiguration.getTimeStamp(), timeStamp)) {
                    logger.debug("Return empty StudyConfiguration");
                    return new QueryResult<>(studyName, (int) (System.currentTimeMillis() - start), 0, 0, "", "", Collections.emptyList());
                } else {
                    logger.debug("Given timeStamp ({}) not equals to stored({}).", timeStamp, ((Map) o).get("timeStamp"));
                }
            }

            if (studyConfiguration.getFileIds() == null || studyConfiguration.getFileIds().isEmpty() || options != null && options.getBoolean(FULL, false)) {
                fillStudyConfiguration(studyConfiguration, study, sessionId);
                logger.debug("Updating StudyConfiguration");
                _updateStudyConfiguration(studyConfiguration, options);
            }

        } catch (CatalogException e) {
            e.printStackTrace();
            logger.error("Unable to get StudyConfiguration from Catalog", e);
        } catch (IOException e) {
            logger.error("Unable to get StudyConfiguration from Catalog", e);
            e.printStackTrace();
        }

        logger.debug("Created StudyConfiguration in {}ms", System.currentTimeMillis() - start);
        if (studyConfiguration == null) {
            return new QueryResult<>(studyName, (int) (System.currentTimeMillis() - start), 0, 0, "", "", Collections.emptyList());
        } else {
            return new QueryResult<>(studyName, (int) (System.currentTimeMillis() - start), 1, 1, "", "", Collections.singletonList(studyConfiguration));
        }
    }

    private void fillStudyConfiguration(StudyConfiguration studyConfiguration, Study study, String sessionId) throws CatalogException {
        int studyId = study.getId();
        fillNullMaps(studyConfiguration);

        //Clear maps
        studyConfiguration.getIndexedFiles().clear();
        studyConfiguration.getFileIds().clear();
        studyConfiguration.getSamplesInFiles().clear();
        studyConfiguration.getHeaders().clear();
        studyConfiguration.getSampleIds().clear();
        studyConfiguration.getCalculatedStats().clear();
        studyConfiguration.getInvalidStats().clear();
        studyConfiguration.getCohortIds().clear();
        studyConfiguration.getCohorts().clear();

        if (study.getAttributes().containsKey(VariantStorageManager.Options.AGGREGATED_TYPE.key())) {
            logger.debug("setting study aggregation to {}", study.getAttributes().get(VariantStorageManager.Options.AGGREGATED_TYPE.key()).toString());
            studyConfiguration.setAggregation(VariantSource.Aggregation.valueOf(
                    study.getAttributes().get(VariantStorageManager.Options.AGGREGATED_TYPE.key()).toString()));
        } else {
            studyConfiguration.setAggregation(VariantSource.Aggregation.NONE);
        }
        logger.debug("studyConfiguration aggregation: {}", studyConfiguration.getAggregation());

//            Object o = study.getAttributes().get(STUDY_CONFIGURATION_FIELD);
//            if (o == null ) {
//                studyConfiguration = new StudyConfiguration(studyId, study.getName());
//            } else if (o instanceof StudyConfiguration) {
//                studyConfiguration = (StudyConfiguration) o;
//            } else {
//                studyConfiguration = objectMapper.readValue(objectMapper.writeValueAsString(o), StudyConfiguration.class);
//            }

        logger.debug("Get Indexed Files");
        QueryResult<File> indexedFiles = catalogManager.getAllFiles(studyId, INDEXED_FILES_QUERY_OPTIONS, sessionId);
        for (File file : indexedFiles.getResult()) {
            studyConfiguration.getIndexedFiles().add(file.getId());
        }

        logger.debug("Get Files");
        QueryResult<File> files = catalogManager.getAllFiles(studyId, ALL_FILES_QUERY_OPTIONS, sessionId);
        for (File file : files.getResult()) {

            studyConfiguration.getFileIds().put(file.getName(), file.getId());
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
            studyConfiguration.getSampleIds().put(sample.getName(), sample.getId());
        }

        logger.debug("Get Cohorts");
        QueryResult<Cohort> cohorts = catalogManager.getAllCohorts(studyId, COHORTS_QUERY_OPTIONS, sessionId);

        for (Cohort cohort : cohorts.getResult()) {
            studyConfiguration.getCohortIds().put(cohort.getName(), cohort.getId());
            studyConfiguration.getCohorts().put(cohort.getId(), new HashSet<>(cohort.getSamples()));
            if (cohort.getStatus() == Cohort.Status.READY) {
                studyConfiguration.getCalculatedStats().add(cohort.getId());
            } else if (cohort.getStatus() == Cohort.Status.INVALID) {
                studyConfiguration.getInvalidStats().add(cohort.getId());
            }
        }
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

    @Override
    public QueryResult _updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        if (options == null) {
            options = this.options;
        }
        try {
            logger.info("Updating StudyConfiguration " + studyConfiguration.getStudyId());
//            StudyConfiguration smallStudyConfiguration = new StudyConfiguration(studyConfiguration.getStudyId(), studyConfiguration.getStudyName(), null, null, null, null);
//            smallStudyConfiguration.setIndexedFiles(studyConfiguration.getIndexedFiles());
//            smallStudyConfiguration.setCalculatedStats(studyConfiguration.getCalculatedStats());
//            smallStudyConfiguration.setInvalidStats(studyConfiguration.getInvalidStats());
//            smallStudyConfiguration.setAttributes(studyConfiguration.getAttributes());
//            smallStudyConfiguration.setTimeStamp(studyConfiguration.getTimeStamp());

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

            return catalogManager.modifyStudy(studyConfiguration.getStudyId(), new ObjectMap("attributes", new ObjectMap(STUDY_CONFIGURATION_FIELD, studyConfiguration)), options.getString("sessionId", sessionId));
        } catch (CatalogException e) {
            logger.error("Unable to update StudyConfiguration in Catalog", e);
            return new QueryResult(Integer.toString(studyConfiguration.getStudyId()), -1, 0, 0, "", e.getMessage(), Collections.emptyList());
        }
    }

    private String getLocalSessionId() {
        String sessionId = "";
        try {
            List<String> lines = Files.readAllLines(Paths.get(System.getProperty("user.home"), ".opencga", "opencga.yml"));
            for (String line : lines) {
                if (line.trim().startsWith("sessionId")) {
                    sessionId = line.split(":")[1];
                }
            }
            sessionId = sessionId.trim().replace("\"", "");
            logger.debug("Session id read from local file 'opencga.yml is: '{}'", sessionId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sessionId;
    }

}
