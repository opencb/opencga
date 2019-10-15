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

package org.opencb.opencga.analysis;

import org.opencb.biodata.models.commons.Analyst;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;
import org.opencb.oskar.core.annotations.AnalysisExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;

public abstract class OpenCgaAnalysis extends OskarAnalysis {

    protected CatalogManager catalogManager;
    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected VariantStorageManager variantStorageManager;

    protected String opencgaHome;
    protected String sessionId;

    protected Logger logger;

    public OpenCgaAnalysis() {
    }

    public OpenCgaAnalysis(String opencgaHome, String sessionId) {
        this.opencgaHome = opencgaHome;
        this.sessionId = sessionId;
    }

    public final OpenCgaAnalysis setUp(String opencgaHome, CatalogManager catalogManager, VariantStorageManager variantStorageManager,
                                       ObjectMap executorParams, Path outDir, String sessionId) {
        this.opencgaHome = opencgaHome;
        this.catalogManager = catalogManager;
        this.configuration = catalogManager.getConfiguration();
        this.variantStorageManager = variantStorageManager;
        this.storageConfiguration = variantStorageManager.getStorageConfiguration();
        this.sessionId = sessionId;

        return setUp(executorParams, outDir);
    }

    public final OpenCgaAnalysis setUp(String opencgaHome, ObjectMap executorParams, Path outDir, String sessionId)
            throws AnalysisException {
        this.opencgaHome = opencgaHome;
        this.sessionId = sessionId;
        this.executorParams = executorParams;
        this.outDir = outDir;

        try {
            loadConfiguration();
            loadStorageConfiguration();

            this.catalogManager = new CatalogManager(configuration);
            this.variantStorageManager = new VariantStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
        } catch (IOException | CatalogException e) {
            throw new AnalysisException(e);
        }

        return setUp(executorParams, outDir);
    }

    private OpenCgaAnalysis setUp(ObjectMap executorParams, Path outDir) {
        logger = LoggerFactory.getLogger(this.getClass().toString());

        availableFrameworks = new ArrayList<>();
        sourceTypes = new ArrayList<>();
        if (storageConfiguration.getDefaultStorageEngineId().equals("mongodb")) {
            if (getAnalysisData().equals(Analysis.AnalysisData.VARIANT)) {
                sourceTypes.add(AnalysisExecutor.Source.MONGODB);
            }
        } else if (storageConfiguration.getDefaultStorageEngineId().equals("hadoop")) {
            availableFrameworks.add(AnalysisExecutor.Framework.MAP_REDUCE);
            // TODO: Check from configuration if spark is available
//            availableFrameworks.add(AnalysisExecutor.Framework.SPARK);
            if (getAnalysisData().equals(Analysis.AnalysisData.VARIANT)) {
                sourceTypes.add(AnalysisExecutor.Source.HBASE);
            }
        }

        availableFrameworks.add(AnalysisExecutor.Framework.ITERATOR);
        sourceTypes.add(AnalysisExecutor.Source.OPENCGA);

        setUp(executorParams, outDir, sourceTypes, availableFrameworks);
        return this;
    }

    protected final void setUpStorageEngineExecutor(String study) throws AnalysisException {
        executorParams.put("opencgaHome", opencgaHome);
        executorParams.put("sessionId", sessionId);
        try {
            DataStore dataStore = variantStorageManager.getDataStore(study, sessionId);

            executorParams.put("storageEngineId", dataStore.getStorageEngine());
            executorParams.put("dbName", dataStore.getDbName());
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }
    }

    /**
     * This method attempts to load general configuration from OpenCGA installation folder, if not exists then loads JAR configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    private void loadConfiguration() throws IOException {
        this.configuration = ConfigurationUtils.loadConfiguration(opencgaHome);
    }

    /**
     * This method attempts to load storage configuration from OpenCGA installation folder, if not exists then loads JAR storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    private void loadStorageConfiguration() throws IOException {
        this.storageConfiguration = ConfigurationUtils.loadStorageConfiguration(opencgaHome);
    }


    protected final Analyst getAnalyst(String token) throws AnalysisException {
        try {
            String userId = catalogManager.getUserManager().getUserId(token);
            QueryResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);

            return new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization());
        } catch (CatalogException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

}
