/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.analysis.execution.plugins;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.catalog.models.tool.Manifest;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created on 26/11/15.
 *
 * TODO: Move non abstract methods to a Context class
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class OpenCGAAnalysis {

    private Logger logger;
    private ObjectMap configuration;
    private CatalogManager catalogManager;
    private String sessionId;
    private boolean initialized;
    private StorageEngineFactory storageEngineFactory;
    private long studyId;

    public Manifest getManifest() {
        try {
            return loadManifest(getIdentifier());
        } catch (IOException ignore) {
            logger.error("Unable to load manifest");
            return null;
        }
    }

    public static Manifest loadManifest(String identifier) throws IOException {
        final String file;
        JsonFactory factory;
        if (OpenCGAAnalysis.class.getResource("/" + identifier + "/manifest.yml") != null) {
            file = "/" + identifier + "/manifest.yml";
            factory = new YAMLFactory();
        } else if (OpenCGAAnalysis.class.getResource("/" + identifier + "/manifest.json") != null) {
            file = "/" + identifier + "/manifest.json";
            factory = new JsonFactory();
        } else if (OpenCGAAnalysis.class.getResource("/" + identifier + "/manifest.xml") != null) {
            file = "/" + identifier + "/manifest.xml";
            factory = new XmlFactory();
        } else {
            return null;
        }
        try (InputStream stream = OpenCGAAnalysis.class.getResourceAsStream(file)) {
            return new ObjectMapper(factory).readValue(stream, Manifest.class);
        }
    }

    public abstract String getIdentifier();

    public abstract int run() throws Exception;

    /*
     *  Util methods
     */

    final void init(Logger logger, ObjectMap configuration, CatalogManager catalogManager,
                    StorageEngineFactory storageEngineFactory, long studyId, String sessionId) {
        if (initialized) {
            throw new IllegalStateException("The plugin was already initialized! Can't init twice");
        }
        this.logger = logger;
        this.configuration = configuration;
        this.catalogManager = catalogManager;
        this.storageEngineFactory = storageEngineFactory;
        this.studyId = studyId;
        this.sessionId = sessionId;
        initialized = true;
    }

    protected final Logger getLogger() {
        return logger;
    }

    protected final ObjectMap getConfiguration() {
        return configuration;
    }

    protected final CatalogManager getCatalogManager() {
        return catalogManager;
    }

    protected final String getSessionId() {
        return sessionId;
    }

    //TODO: Return a VariantDBAdaptor which checks catalog permissions
    protected final VariantDBAdaptor getVariantDBAdaptor(long studyId)
            throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException,
            StorageEngineException {

        StorageEngineFactory storageEngineFactory = this.storageEngineFactory;
        
        DataStore dataStore = StorageOperation.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        String storageEngine = dataStore.getStorageEngine();
        String dbName = dataStore.getDbName();

        return storageEngineFactory.getVariantStorageEngine(storageEngine).getDBAdaptor(dbName);
    }

    //TODO: Return an AlignmentDBAdaptor which checks catalog permissions
    protected final AlignmentDBAdaptor getAlignmentDBAdaptor(long studyId) {
        throw new UnsupportedOperationException();
    }

    protected final long getStudyId() {
        return studyId;
    }
}
