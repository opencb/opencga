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

package org.opencb.opencga.analysis.storage.variant;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

 import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public abstract class CatalogVariantStorageEngine extends StorageEngine<VariantDBAdaptor> implements StoragePipeline {
//public class CatalogVariantStorageEngine extends VariantStorageEngine {


    private CatalogManager catalogManager;
//    private VariantStorageEngine storageManager;
    private Properties properties;
    private ObjectMap params;
    private final List<URI> configUris;

    public CatalogVariantStorageEngine() {
        this.properties = new Properties();
        configUris = new LinkedList<>();
    }

    public CatalogVariantStorageEngine(CatalogManager catalogManager) {
        this();
        this.catalogManager = catalogManager;
//        this.storageManager = variantStorageManager;
    }

    @Override
    public void setConfiguration(StorageConfiguration configuration, String s) {

    }
//
//    @Override
//    public URI extract(URI input, URI ouput) throws StorageEngineException {
//        return getStorageManager(params).extract(input, ouput);
//    }
//
//    @Override
//    public URI preTransform(URI input) throws IOException, FileFormatException, StorageEngineException {
//        return getStorageManager(params).preTransform(input);
//    }
//
//    @Override
//    public URI transform(URI input, URI pedigree, URI output) throws IOException, FileFormatException, StorageEngineException {
//        return getStorageManager(params).transform(input, pedigree, output);
//    }
//
//    @Override
//    public URI postTransform(URI input) throws IOException, FileFormatException, StorageEngineException {
//        return getStorageManager(params).postTransform(input);
//    }
//
//    @Override
//    public URI preLoad(URI input, URI output) throws IOException, StorageEngineException {
//        return getStorageManager(params).preLoad(input, output);
//    }
//
//    @Override
//    public URI load(URI input) throws IOException, StorageEngineException {
//        return getStorageManager(params).load(input);
//    }
//
//    @Override
//    public URI postLoad(URI input, URI output) throws IOException, StorageEngineException {
//        return getStorageManager(params).postLoad(input, output);
//    }

//    @Override
//    public VariantWriter getDBWriter(String dbName) throws StorageEngineException {
//        if (dbName == null) {
//            dbName = getCatalogManager().getUserIdBySessionId(params.getString("sessionId"));
//        }
//        return getStorageManager(params).getDBWriter(dbName);
//    }

    @Override
    public VariantDBAdaptor getDBAdaptor(String dbName) throws StorageEngineException {
        if (dbName == null) {
            try {
                dbName = getCatalogManager().getUserIdBySessionId(params.getString("sessionId"));
            } catch (CatalogException e) {
                e.printStackTrace();
            }
        }
        return getStorageManager(params).getDBAdaptor(dbName);
    }

    public CatalogManager getCatalogManager() {
        if (catalogManager == null) {
            try {
                Configuration configuration = Configuration.load(new FileInputStream(Paths.get(Config.getOpenCGAHome(),
                        "conf", "configuration.yml").toFile()));
                catalogManager = new CatalogManager(configuration);
            } catch (CatalogException | IOException e) {
                e.printStackTrace();
            }
        }
        return catalogManager;
    }

//    public void setCatalogManager(CatalogManager catalogManager) {
//        this.catalogManager = catalogManager;
//    }

    public VariantStorageEngine getStorageManager(ObjectMap params) throws StorageEngineException {
        try {
            QueryResult<File> file = getCatalogManager().getFile(params.getInt("fileId"), params.getString("sessionId"));
            String storageEngine = file.getResult().get(0).getAttributes().get("storageEngine").toString();
            return StorageEngineFactory.get().getVariantStorageEngine(storageEngine);
        } catch (Exception e) {
            throw new StorageEngineException("Can't get StorageEngine", e);
        }
    }

//    public void setStorageManager(VariantStorageEngine storageManager) {
//        this.storageManager = storageManager;
//    }
}
