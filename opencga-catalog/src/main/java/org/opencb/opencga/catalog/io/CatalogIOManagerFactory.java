package org.opencb.opencga.catalog.io;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by imedina on 03/10/14.
 */
public class CatalogIOManagerFactory {

    public static final String DEFAULT_CATALOG_SCHEME = "file";
    private static Map<String, CatalogIOManager> catalogIOManagers = new HashMap<>();
    private final Properties properties;

    public CatalogIOManagerFactory(Properties properties) {
        this.properties = properties;
    }

    public CatalogIOManager get(URI uri) throws IOException, CatalogIOManagerException {
        return get(uri.getScheme());
    }
    public CatalogIOManager get(String io) throws CatalogIOManagerException {
        if(io == null) {
            io = DEFAULT_CATALOG_SCHEME;
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
}
