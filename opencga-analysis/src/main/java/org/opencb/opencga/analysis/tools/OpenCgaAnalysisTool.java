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

package org.opencb.opencga.analysis.tools;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.ConfigurationUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.tools.OpenCgaTool;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;
import java.nio.file.Path;

public abstract class OpenCgaAnalysisTool extends OpenCgaTool {

    protected CatalogManager catalogManager;
    protected VariantStorageManager variantStorageManager;

    public OpenCgaAnalysisTool() {
        super();
    }

    public final OpenCgaAnalysisTool setUp(String opencgaHome, CatalogManager catalogManager, StorageEngineFactory engineFactory,
                                           ObjectMap params, Path outDir, String jobId, String token) {
        VariantStorageManager manager = new VariantStorageManager(catalogManager, engineFactory);
        return setUp(opencgaHome, catalogManager, manager, params, outDir, jobId, token);
    }

    public final OpenCgaAnalysisTool setUp(String opencgaHome, CatalogManager catalogManager, VariantStorageManager variantStorageManager,
                                           ObjectMap params, Path outDir, String jobId, String token) {
        this.catalogManager = catalogManager;
        this.variantStorageManager = variantStorageManager;
        setUp(opencgaHome, catalogManager.getConfiguration(), variantStorageManager.getStorageConfiguration(), params, outDir, jobId,
                token);
        return this;
    }

    public final OpenCgaAnalysisTool setUp(String opencgaHome, ObjectMap params, Path outDir, String token) throws ToolException {
        try {
            loadConfiguration();
            loadStorageConfiguration();

            this.catalogManager = new CatalogManager(configuration);
            this.variantStorageManager = new VariantStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
        } catch (IOException | CatalogException e) {
            throw new ToolException(e);
        }

        setUp(opencgaHome, catalogManager.getConfiguration(), variantStorageManager.getStorageConfiguration(), params, outDir, null, token);
        return this;
    }

    public final CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public final VariantStorageManager getVariantStorageManager() {
        return variantStorageManager;
    }

    protected final void moveFile(String study, Path source, Path destiny, String catalogDirectoryPath, String token) throws ToolException {
        File file;
        try {
            file = catalogManager.getFileManager().moveAndRegister(study, source, destiny, catalogDirectoryPath, token).first();
        } catch (Exception e) {
            throw new ToolException("Error moving file from " + source + " to " + destiny, e);
        }
        // Add only if move is successful
        addGeneratedFile(file);
    }

    protected final void registerExternalFile(String study, String uri, String catalogDirectoryPath, String token) throws ToolException {
        File file;
        try {
            FileLinkParams linkParams = new FileLinkParams(uri, catalogDirectoryPath, "", TimeUtils.getTime(), TimeUtils.getTime(), null,
                    null, null);
            file = catalogManager.getFileManager().link(study, linkParams, true, token).first();
        } catch (Exception e) {
            throw new ToolException("Error linking file " + uri + " to " + catalogDirectoryPath, e);
        }
        // Add only if link is successful
        addGeneratedFile(file);
    }

    protected final String getParentDirectory(String path) {
        if (path.endsWith("/")) {
            // Remove trailing /
            path = path.substring(0, path.length() - 1);
        }

        // Find position of last /
        int i = path.lastIndexOf("/");
        if (i > 0) {
            return path.substring(0, i + 1);
        }
        return "/";
    }

    protected final void setUpStorageEngineExecutor(String study) throws ToolException {
        setUpStorageEngineExecutor(null, study);
    }

    protected final void setUpStorageEngineExecutorByProjectId(String projectId) throws ToolException {
        setUpStorageEngineExecutor(projectId, null);
    }

    private final void setUpStorageEngineExecutor(String projectId, String study) throws ToolException {
        executorParams.put("opencgaHome", opencgaHome);
        executorParams.put(ParamConstants.TOKEN, token);
        try {
            DataStore dataStore;
            if (study == null) {
                dataStore = variantStorageManager.getDataStoreByProjectId(projectId, token);
            } else {
                dataStore = variantStorageManager.getDataStore(study, token);
            }

            executorParams.put("storageEngineId", dataStore.getStorageEngine());
            executorParams.put("dbName", dataStore.getDbName());
        } catch (CatalogException e) {
            throw new ToolException(e);
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

}
