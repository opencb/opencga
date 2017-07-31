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

package org.opencb.opencga.catalog.io;

import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.common.UriUtils;
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

    protected static Logger logger = LoggerFactory.getLogger(CatalogIOManagerFactory.class);
    private final Properties properties;
    private final Configuration configuration;
    private final URI mainRootdir;
    private String defaultCatalogScheme = "file";
    private Map<String, CatalogIOManager> catalogIOManagers = new HashMap<>();

    public CatalogIOManagerFactory(Configuration configuration) throws CatalogIOException {
        this.properties = null;
        this.configuration = configuration;
        try {
            mainRootdir = UriUtils.createDirectoryUri(configuration.getDataDir());
        } catch (URISyntaxException e) {
            throw new CatalogIOException("Malformed URI 'OPENCGA.CATALOG.MAIN.ROOTDIR'", e);
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
        if (io == null) {
            io = defaultCatalogScheme;
        }

        if (!catalogIOManagers.containsKey(io)) {
            switch (io) {
                case "file":
                    catalogIOManagers.put("file", new PosixCatalogIOManager(configuration));
                    break;
                case "hdfs":
                    catalogIOManagers.put("hdfs", new HdfsCatalogIOManager(configuration));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported file system : " + io);
            }
        }
        return catalogIOManagers.get(io);
    }

    public String getDefaultCatalogScheme() {
        return defaultCatalogScheme;
    }

    public void setDefaultCatalogScheme(String defaultCatalogScheme) {
        this.defaultCatalogScheme = defaultCatalogScheme;
    }

}
