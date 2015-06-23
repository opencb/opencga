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

package org.opencb.opencga.catalog.io;

import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.UriUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by imedina on 03/10/14.
 */
public class CatalogIOManagerFactory {

    private String defaultCatalogScheme = "file";
    private Map<String, CatalogIOManager> catalogIOManagers = new HashMap<>();
    private final Properties properties;
    private final URI mainRootdir;
    protected static Logger logger = LoggerFactory.getLogger(CatalogIOManagerFactory.class);

    public CatalogIOManagerFactory(Properties properties) throws CatalogIOException {
        this.properties = properties;
        String property = properties.getProperty(CatalogManager.CATALOG_MAIN_ROOTDIR);
        try {
            mainRootdir = UriUtils.getUri(property.endsWith("/") ? property : property + "/");
        } catch (URISyntaxException e) {
            throw new CatalogIOException("Malformed URI '" + CatalogManager.CATALOG_MAIN_ROOTDIR + "'", e);
        }

        String scheme = mainRootdir.getScheme();
        if (scheme != null) {
            defaultCatalogScheme = scheme;
        }
    }

    public CatalogIOManager get(URI uri) throws CatalogIOException {
        return get(uri.getScheme());
    }

    public CatalogIOManager getDefault() throws CatalogIOException {
        return get(defaultCatalogScheme);
    }

    public CatalogIOManager get(String io) throws CatalogIOException {
        if(io == null) {
            io = defaultCatalogScheme;
        }

        if(!catalogIOManagers.containsKey(io)) {
            switch(io) {
                case "file":
                    catalogIOManagers.put("file", new PosixCatalogIOManager(properties));
                    break;
                case "hdfs":
                    catalogIOManagers.put("hdfs", new HdfsCatalogIOManager(properties));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported file system : " + io);
            }
        }
        return catalogIOManagers.get(io);
    }

    public void setDefaultCatalogScheme(String defaultCatalogScheme) {
        this.defaultCatalogScheme = defaultCatalogScheme;
    }

    public String getDefaultCatalogScheme() {
        return defaultCatalogScheme;
    }

}
