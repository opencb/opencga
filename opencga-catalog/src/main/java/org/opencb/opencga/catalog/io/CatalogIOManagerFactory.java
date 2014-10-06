package org.opencb.opencga.catalog.io;

import java.util.Map;
import java.util.Properties;

/**
 * Created by imedina on 03/10/14.
 */
public class CatalogIOManagerFactory {

    private static Map<String, CatalogIOManager> catalogIOManagers;


    public CatalogIOManagerFactory(Properties properties) {


    }


    public CatalogIOManager get(String io) {
        if(!catalogIOManagers.containsKey(io)) {
            switch(io) {
                case "file":
                    catalogIOManagers.put(new PosixCatalogIOManager(""));
                    break;
                case "hdfs":
                    catalogIOManagers.put(new HdfsCatalogIOManager(""));
                    break;
                default:
                    System.out.println("mmm...");
                    break;
            }

        }
        return catalogIOManagers.get(io);
    }

}
