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

package org.opencb.opencga.app.cli.admin.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.cli.admin.options.StorageCommandOptions;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by imedina on 02/03/15.
 */
public class StorageCommandExecutor extends AdminCommandExecutor {

    private final StorageCommandOptions storageCommandOptions;

    public StorageCommandExecutor(StorageCommandOptions storageCommandOptions) {
        super(storageCommandOptions.getCommonOptions());
        this.storageCommandOptions = storageCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        String subCommandString = storageCommandOptions.getSubCommand();
        logger.debug("Executing catalog admin {} command line", subCommandString);
        switch (subCommandString) {
            case "status":
                status();
                break;
            case "update-database-prefix":
                updateDatabasePrefix();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    private void status() throws Exception {
        StorageCommandOptions.StatusCommandOptions commandOptions = storageCommandOptions.getStatusCommandOptions();
        StorageEngineFactory factory = StorageEngineFactory.get(storageConfiguration);
        try (CatalogManager catalogManager = new CatalogManager(configuration);
             VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManager, factory)) {
            String adminPassword = getAdminPassword(true);
            token = catalogManager.getUserManager().loginAsAdmin(adminPassword).first().getToken();

            ObjectMap status = new ObjectMap();
            Collection<String> liveNodes;
            SolrClient solrClient = factory.getVariantStorageEngine(factory.getDefaultStorageEngineId(), "test_connection")
                    .getVariantSearchManager().getSolrManager().getSolrClient();
            if (solrClient instanceof CloudSolrClient) {
                liveNodes = ((CloudSolrClient) solrClient).getClusterStateProvider().getLiveNodes().stream()
                        .map(s -> "http://" + s)
                        .map(s -> StringUtils.removeEnd(s, "_solr"))
                        .collect(Collectors.toList());
            } else if (solrClient instanceof HttpSolrClient) {
                liveNodes = Collections.singleton(((HttpSolrClient) solrClient).getBaseURL());
            } else {
                liveNodes = storageConfiguration.getSearch().getHosts();
            }
            boolean solrAlive;
            Object collections = Collections.emptyList();
            try {
                solrAlive = variantStorageManager.isSolrAvailable();
                collections = solrClient.request(new CollectionAdminRequest.List()).get("collections");
            } catch (Exception e) {
                logger.warn("Solr not alive", e);
                solrAlive = false;
            }
            ObjectMap solrStatus = new ObjectMap();
            solrStatus.put("alive", solrAlive);
            solrStatus.put("live_nodes", liveNodes);
            solrStatus.put("collections", collections);
            status.put("solr", solrStatus);

            List<String> organizationIds = parseOrganizationIds(catalogManager, commandOptions.organizationId);
            logger.info("OrganizationIds: {}", organizationIds);
            List<ObjectMap> dataStores = new ArrayList<>();
            List<String> variantStorageProjects = getVariantStorageProjects(organizationIds, catalogManager, variantStorageManager);
            logger.info("Variant storage projects: {}", variantStorageProjects);
            for (String project : variantStorageProjects) {
                DataStore dataStore = variantStorageManager.getDataStoreByProjectId(project, token);
                ObjectMap map = new ObjectMap("project", project)
                        .append("bioformat", File.Bioformat.VARIANT.name())
                        .append("dataStore", dataStore);
                dataStores.add(map);
            }
            List<String> cvdbProjects = getCvdbProjects(organizationIds, catalogManager);
            logger.info("CVDB projects: {}", cvdbProjects);
            for (String project : cvdbProjects) {
                DataStore dataStore = getCvdbDatastore(project, catalogManager);
                ObjectMap map = new ObjectMap("project", project)
                        .append("bioformat", File.Bioformat.CVDB.name())
                        .append("dataStore", dataStore);
                dataStores.add(map);
            }
            status.put("dataStores", dataStores);

            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(status));
        }
    }

    // This method is implemented by the OpenCGA Enterprise
    protected List<String> getCvdbProjects(List<String> organizationIds, CatalogManager catalogManager) throws Exception {
        return Collections.emptyList();
    }

    // This method is implemented by the OpenCGA Enterprise
    protected DataStore getCvdbDatastore(String projectFqn, CatalogManager catalogManager) throws Exception {
        return new DataStore();
    }

    private void updateDatabasePrefix() throws Exception {
        StorageCommandOptions.UpdateDatabasePrefix commandOptions = storageCommandOptions.getUpdateDatabasePrefix();
        StorageEngineFactory.configure(storageConfiguration);

        if (commandOptions.projectsWithoutStorage && commandOptions.projectsWithStorage) {
            // If both true, ignore both.
            commandOptions.projectsWithoutStorage = false;
            commandOptions.projectsWithStorage = false;
        }

        Set<String> projects;
        if (commandOptions.projects != null) {
            projects = new HashSet<>(Arrays.asList(commandOptions.projects.split(",")));
        } else {
            projects = null;
        }

        StorageEngineFactory factory = StorageEngineFactory.get(storageConfiguration);
        try (CatalogManager catalogManager = new CatalogManager(configuration);
             VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManager, factory)) {
            String adminPassword = getAdminPassword(true);
            token = catalogManager.getUserManager().loginAsAdmin(adminPassword).first().getToken();
            Set<String> variantStorageProjects = Collections.emptySet();
            List<String> organizationIds = parseOrganizationIds(catalogManager, commandOptions.organizationId);

            if (commandOptions.projectsWithoutStorage || commandOptions.projectsWithStorage) {
                if (commandOptions.bioformat.equals("CVDB")) {
                    throw new IllegalArgumentException("Cannot use --projects-without-storage or --projects-with-storage with CVDB bioformat.");
                }
                variantStorageProjects = new HashSet<>(getVariantStorageProjects(organizationIds, catalogManager, variantStorageManager));
            }

            for (Project project : getAllProjects(catalogManager, organizationIds)) {
                if (projects != null && !projects.contains(project.getFqn())) {
                    logger.info("Skip project '{}'", project.getFqn());
                    continue;
                }

                switch (commandOptions.bioformat) {
                    case "VARIANT":
                        if (commandOptions.projectsWithoutStorage && variantStorageProjects.contains(project.getFqn())) {
                            // Only accept projects WITHOUT storage. so discard projects with storage
                            logger.info("Skip project '{}' as it has a variant storage.", project.getFqn());
                            continue;
                        }
                        if (commandOptions.projectsWithStorage && !variantStorageProjects.contains(project.getFqn())) {
                            // Only accept projects WITH storage. so discard projects without storage
                            logger.info("Skip project '{}' as it doesn't have a variant storage.", project.getFqn());
                            continue;
                        }

                        // Update variant datastore
                        updateVariantDataStore(commandOptions.dbPrefix, commandOptions.projectsWithUndefinedDBName, project, catalogManager);
                        break;
                    case "CVDB":
                        // Update CVDB datastore
                        updateCvdbDataStore(commandOptions.dbPrefix, commandOptions.projectsWithUndefinedDBName, project, catalogManager);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown bioformat '" + commandOptions.bioformat + "'");
                }
            }
        }
    }

    private void updateVariantDataStore(String dbPrefix, boolean projectsWithUndefinedDBName, Project project,
                                        CatalogManager catalogManager) throws CatalogException {
        DataStore currentDataStore;
        if (project.getInternal() != null && project.getInternal().getDatastores() != null) {
            currentDataStore = project.getInternal().getDatastores().getVariant();
        } else {
            currentDataStore = null;
        }
        if (projectsWithUndefinedDBName) {
            // Only accept projects with UNDEFINED dbname. so discard projects with DEFINED (without undefined) dbname
            boolean undefinedDBName = currentDataStore == null || StringUtils.isEmpty(currentDataStore.getDbName());
            if (!undefinedDBName) {
                logger.info("Skip project '{}' as its variant dbName is already defined. Variant dbName : '{}'", project.getFqn(),
                        currentDataStore.getDbName());
                return;
            }
        }

        // Variant datastore
        DataStore defaultDataStore;
        if (StringUtils.isEmpty(dbPrefix)) {
            defaultDataStore = VariantStorageManager.defaultDataStore(catalogManager, project);
        } else {
            defaultDataStore = VariantStorageManager.defaultDataStore(dbPrefix, project.getFqn());
        }
        DataStore newDataStore = createNewDataStore(currentDataStore, defaultDataStore, project);
        catalogManager.getProjectManager().setDatastoreVariant(project.getFqn(), newDataStore, token);
    }

    private void updateCvdbDataStore(String dbPrefix, boolean projectsWithUndefinedDBName, Project project,
                                     CatalogManager catalogManager) throws CatalogException {
        DataStore currentDataStore;
        if (project.getInternal() != null && project.getInternal().getDatastores() != null) {
            currentDataStore = project.getInternal().getDatastores().getCvdb();
        } else {
            currentDataStore = null;
        }
        if (projectsWithUndefinedDBName) {
            // Only accept projects with UNDEFINED dbname. so discard projects with DEFINED (without undefined) dbname
            boolean undefinedDBName = currentDataStore == null || StringUtils.isEmpty(currentDataStore.getDbName());
            if (!undefinedDBName) {
                logger.info("Skip project '{}' as its CVDB dbName is already defined. CVDB dbName : '{}'", project.getFqn(),
                        currentDataStore.getDbName());
                return;
            }
        }

        // CVDB datastore
        DataStore defaultDataStore;
        if (StringUtils.isEmpty(dbPrefix)) {
            defaultDataStore = VariantStorageManager.defaultCvdbDataStore(catalogManager, project);
        } else {
            defaultDataStore = VariantStorageManager.defaultCvdbDataStore(dbPrefix, project.getFqn());
        }
        DataStore newDataStore = createNewDataStore(currentDataStore, defaultDataStore, project);
        catalogManager.getProjectManager().setDatastoreCvdb(project.getFqn(), newDataStore, token);
    }

    private DataStore createNewDataStore(DataStore currentDataStore, DataStore defaultDataStore, Project project) {
        final DataStore newDataStore;
        logger.info("------");
        logger.info("Project '{}'", project.getFqn());
        if (currentDataStore == null) {
            newDataStore = defaultDataStore;
            logger.info("Old DBName: null");
        } else {
            logger.info("Old DBName: {}", currentDataStore.getDbName());
            currentDataStore.setDbName(defaultDataStore.getDbName());
            newDataStore = currentDataStore;
        }
        logger.info("New DBName: {}", newDataStore.getDbName());

        return newDataStore;
    }

    private List<String> parseOrganizationIds(CatalogManager catalogManager, String organizationId) throws CatalogException {
        return StringUtils.isEmpty(organizationId)
                ? catalogManager.getOrganizationManager().getOrganizationIds(token)
                : Arrays.asList(organizationId.split(","));
    }

    private List<Project> getAllProjects(CatalogManager catalogManager, List<String> organizationIds) throws CatalogException {
        List<Project> projects = new ArrayList<>();
        for (String organizationId : organizationIds) {
            projects.addAll(catalogManager.getProjectManager().search(organizationId, new Query(), new QueryOptions(), token).getResults());
        }
        return projects;
    }

    /**
     * Get list of projects that exist at the VariantStorage.
     * @return List of projects
     * @throws Exception on error
     */
    protected final List<String> getVariantStorageProjects(List<String> organizationIds, CatalogManager catalogManager,
                                                           VariantStorageManager variantStorageManager) throws Exception {
        Set<String> projects = new LinkedHashSet<>();

        for (String studyFqn : getVariantStorageStudies(organizationIds, catalogManager, variantStorageManager)) {
            projects.add(catalogManager.getStudyManager().getProjectFqn(studyFqn));
        }

        return new ArrayList<>(projects);
    }

    /**
     * Get list of studies that exist at the VariantStorage.
     * @return List of projects
     * @throws Exception on error
     */
    protected final List<String> getVariantStorageStudies(List<String> organizationIds, CatalogManager catalogManager,
                                                          VariantStorageManager variantStorageManager) throws Exception {
        Set<String> studies = new LinkedHashSet<>();
        for (String organizationId : organizationIds) {
            int studiesCount = 0;
            for (Study study : catalogManager.getStudyManager().searchInOrganization(organizationId, new Query(),
                    new QueryOptions(QueryOptions.INCLUDE,
                            Arrays.asList(StudyDBAdaptor.QueryParams.FQN.key(), StudyDBAdaptor.QueryParams.ID.key())),
                    token).getResults()) {
                studiesCount++;
                if (variantStorageManager.exists(study.getFqn(), token)) {
                    logger.info("Study '{}' exists at the VariantStorage", study.getFqn());
                    studies.add(study.getFqn());
                } else {
                    logger.info("Study '{}' does not exist at the VariantStorage", study.getFqn());
                }
            }
            logger.info("Found {} studies in organization '{}'", studiesCount, organizationId);
        }
        return new ArrayList<>(studies);
    }
}
