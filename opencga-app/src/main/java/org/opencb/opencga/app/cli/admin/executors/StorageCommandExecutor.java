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

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.cli.admin.options.StorageCommandOptions;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
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
//            token = catalogManager.getUserManager().loginAsAdmin(commandOptions.commonOptions.adminPassword).getToken();
            token = catalogManager.getUserManager().loginAsAdmin(adminPassword).getToken();

            ObjectMap status = new ObjectMap();
            Collection<String> liveNodes;
            SolrClient solrClient = factory.getVariantStorageEngine(factory.getDefaultStorageEngineId(), "test_connection").getVariantSearchManager().getSolrManager().getSolrClient();
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
            try {
                solrAlive = variantStorageManager.isSolrAvailable();
            } catch (Exception e) {
                logger.warn("Solr not alive", e);
                solrAlive = false;
            }
            ObjectMap solrStatus = new ObjectMap();
            solrStatus.put("alive", solrAlive);
            solrStatus.put("live_nodes", liveNodes);
            solrStatus.put("collections", solrClient.request(new CollectionAdminRequest.List()).get("collections"));
            status.put("solr", solrStatus);

            List<ObjectMap> dataStores = new ArrayList<>();
            List<String> variantStorageProjects = getVariantStorageProjects(catalogManager, variantStorageManager);
            for (String project : variantStorageProjects) {
                DataStore dataStore = variantStorageManager.getDataStoreByProjectId(project, token);
                ObjectMap map = new ObjectMap("project", project);
                map.put("dataStore", dataStore);
                dataStores.add(map);
            }
            status.put("dataStores", dataStores);

            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(status));
        }
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
            token = catalogManager.getUserManager().loginAsAdmin(adminPassword).getToken();
            Set<String> variantStorageProjects = Collections.emptySet();
            if (commandOptions.projectsWithoutStorage || commandOptions.projectsWithStorage) {
                variantStorageProjects = new HashSet<>(getVariantStorageProjects(catalogManager, variantStorageManager));
            }

            for (Project project : catalogManager.getProjectManager().search(organizationId, new Query(), new QueryOptions(), token).getResults()) {
                if (projects != null && !projects.contains(project.getFqn())) {
                    logger.info("Skip project '{}'", project.getFqn());
                    continue;
                }
                if (commandOptions.projectsWithoutStorage) {
                    // Only accept projects WITHOUT storage. so discard projects with storage
                    if (variantStorageProjects.contains(project.getFqn())) {
                        logger.info("Skip project '{}' as it has a variant storage.", project.getFqn());
                        continue;
                    }
                }
                if (commandOptions.projectsWithStorage) {
                    // Only accept projects WITH storage. so discard projects without storage
                    if (!variantStorageProjects.contains(project.getFqn())) {
                        logger.info("Skip project '{}' as it doesn't have a variant storage.", project.getFqn());
                        continue;
                    }
                }
                final DataStore actualDataStore;
                if (project.getInternal() != null && project.getInternal().getDatastores() != null) {
                    actualDataStore = project.getInternal().getDatastores().getVariant();
                } else {
                    actualDataStore = null;
                }
                if (commandOptions.projectsWithUndefinedDBName) {
                    // Only accept projects with UNDEFINED dbname. so discard projects with DEFINED (without undefined) dbname
                    boolean undefinedDBName = actualDataStore == null || StringUtils.isEmpty(actualDataStore.getDbName());
                    if (!undefinedDBName) {
                        logger.info("Skip project '{}' as its dbName is already defined. dbName : '{}'",
                                project.getFqn(),
                                actualDataStore.getDbName());
                        continue;
                    }
                }

                final DataStore defaultDataStore;
                if (StringUtils.isEmpty(commandOptions.dbPrefix)) {
                    defaultDataStore = VariantStorageManager.defaultDataStore(catalogManager, project, token);
                } else {
                    defaultDataStore = VariantStorageManager.defaultDataStore(catalogManager, project, commandOptions.dbPrefix, token);
                }

                final DataStore newDataStore;
                logger.info("------");
                logger.info("Project " + project.getFqn());
                if (actualDataStore == null) {
                    newDataStore = defaultDataStore;
                    logger.info("Old DBName: null");
                } else {
                    logger.info("Old DBName: " + actualDataStore.getDbName());
                    actualDataStore.setDbName(defaultDataStore.getDbName());
                    newDataStore = actualDataStore;
                }
                logger.info("New DBName: " + newDataStore.getDbName());

                catalogManager.getProjectManager().setDatastoreVariant(organizationId, project.getUuid(), newDataStore, token);                catalogManager.getProjectManager().setDatastoreVariant(organizationId, project.getUuid(), defaultDataStore, token);
            }
        }
    }

    /**
     * Get list of projects that exist at the VariantStorage.
     * @return List of projects
     * @throws Exception on error
     */
    protected final List<String> getVariantStorageProjects(CatalogManager catalogManager, VariantStorageManager variantStorageManager) throws Exception {
        Set<String> projects = new LinkedHashSet<>();

        for (String studyFqn : getVariantStorageStudies(catalogManager, variantStorageManager)) {
            projects.add(catalogManager.getStudyManager().getProjectFqn(studyFqn));
        }

        return new ArrayList<>(projects);
    }

    /**
     * Get list of studies that exist at the VariantStorage.
     * @return List of projects
     * @throws Exception on error
     */
    protected final List<String> getVariantStorageStudies(CatalogManager catalogManager, VariantStorageManager variantStorageManager) throws Exception {
        Set<String> studies = new LinkedHashSet<>();
        for (Study study : catalogManager.getStudyManager().search(organizationId, new Query(), new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList("fqn")), token).getResults()) {
            if (variantStorageManager.exists(study.getFqn(), token)) {
                studies.add(study.getFqn());
            }
        }
        return new ArrayList<>(studies);
    }


}
