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

package org.opencb.opencga.analysis.storage.variant;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.storage.core.StorageETL;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
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
public abstract class CatalogVariantStorageManager extends StorageManager<VariantDBAdaptor> implements StorageETL {
//public class CatalogVariantStorageManager extends VariantStorageManager {


    private CatalogManager catalogManager;
//    private VariantStorageManager storageManager;
    private Properties properties;
    private ObjectMap params;
    private final List<URI> configUris;

    public CatalogVariantStorageManager() {
        this.properties = new Properties();
        configUris = new LinkedList<>();
    }

    public CatalogVariantStorageManager(CatalogManager catalogManager) {
        this();
        this.catalogManager = catalogManager;
//        this.storageManager = variantStorageManager;
    }

    @Override
    public void setConfiguration(StorageConfiguration configuration, String s) {

    }
//
//    @Override
//    public URI extract(URI input, URI ouput) throws StorageManagerException {
//        return getStorageManager(params).extract(input, ouput);
//    }
//
//    @Override
//    public URI preTransform(URI input) throws IOException, FileFormatException, StorageManagerException {
//        return getStorageManager(params).preTransform(input);
//    }
//
//    @Override
//    public URI transform(URI input, URI pedigree, URI output) throws IOException, FileFormatException, StorageManagerException {
//        return getStorageManager(params).transform(input, pedigree, output);
//    }
//
//    @Override
//    public URI postTransform(URI input) throws IOException, FileFormatException, StorageManagerException {
//        return getStorageManager(params).postTransform(input);
//    }
//
//    @Override
//    public URI preLoad(URI input, URI output) throws IOException, StorageManagerException {
//        return getStorageManager(params).preLoad(input, output);
//    }
//
//    @Override
//    public URI load(URI input) throws IOException, StorageManagerException {
//        return getStorageManager(params).load(input);
//    }
//
//    @Override
//    public URI postLoad(URI input, URI output) throws IOException, StorageManagerException {
//        return getStorageManager(params).postLoad(input, output);
//    }

//    @Override
//    public VariantWriter getDBWriter(String dbName) throws StorageManagerException {
//        if (dbName == null) {
//            dbName = getCatalogManager().getUserIdBySessionId(params.getString("sessionId"));
//        }
//        return getStorageManager(params).getDBWriter(dbName);
//    }

    @Override
    public VariantDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        if (dbName == null) {
            dbName = getCatalogManager().getUserIdBySessionId(params.getString("sessionId"));
        }
        return getStorageManager(params).getDBAdaptor(dbName);
    }

    public CatalogManager getCatalogManager() {
        if (catalogManager == null) {
            try {
                CatalogConfiguration catalogConfiguration = CatalogConfiguration.load(new FileInputStream(Paths.get(Config.getOpenCGAHome(),
                        "conf", "catalog-configuration.yml").toFile()));
                catalogManager = new CatalogManager(catalogConfiguration);
            } catch (CatalogException | IOException e) {
                e.printStackTrace();
            }
        }
        return catalogManager;
    }

//    public void setCatalogManager(CatalogManager catalogManager) {
//        this.catalogManager = catalogManager;
//    }

    public VariantStorageManager getStorageManager(ObjectMap params) throws StorageManagerException {
        try {
            QueryResult<File> file = getCatalogManager().getFile(params.getInt("fileId"), params.getString("sessionId"));
            String storageEngine = file.getResult().get(0).getAttributes().get("storageEngine").toString();
            return StorageManagerFactory.get().getVariantStorageManager(storageEngine);
        } catch (Exception e) {
            throw new StorageManagerException("Can't get StorageEngine", e);
        }
    }

//    public void setStorageManager(VariantStorageManager storageManager) {
//        this.storageManager = storageManager;
//    }
}
