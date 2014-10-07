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

    private static Map<String, CatalogIOManager> catalogIOManagers = new HashMap<>();
    private final Properties properties;

    public CatalogIOManagerFactory(Properties properties) {
        this.properties = properties;
    }

    public CatalogIOManager get(String io) throws IOException, CatalogIOManagerException {
        if(!catalogIOManagers.containsKey(io)) {
            switch(io) {
                case "file":
                    catalogIOManagers.put("file", new PosixCatalogIOManager(properties));
                    break;
                case "hdfs":
                    catalogIOManagers.put("hdfs", new HdfsCatalogIOManager(properties));
                    break;
                default:
                    System.out.println("mmm... that shouldn't be happening.");
                    throw new UnsupportedOperationException("Unsupported file system : " + io);
            }
        }
        return catalogIOManagers.get(io);
    }
}
