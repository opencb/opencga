package org.opencb.opencga.storage.mongodb.variant.annotator;

import org.junit.Test;
import org.opencb.cellbase.core.client.CellBaseClient;
import org.opencb.cellbase.core.common.core.CellbaseConfiguration;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.CellBaseVariantAnnotator;

import java.net.URI;
import java.nio.file.Paths;

public class CellBaseVariantAnnotatorTest extends GenericTest {

    private String cellbaseSpecies = "hsapiens";
    private String cellbaseAssembly = "GRCh37";

    @Test
    public void testCreateAnnotationREST() throws Exception {
        VariantDBAdaptor variantDBAdaptor = getDbAdaptor();

        CellBaseClient cellBaseClient;
//        cellBaseClient = new CellBaseClient("wwwdev.ebi.ac.uk", 80, "/cellbase/webservices/rest/", "v3", "hsapiens");
        cellBaseClient = new CellBaseClient("localhost", 8080, "/cellbase/webservices/rest/", "v3", "hsapiens");

        CellBaseVariantAnnotator annotator = new CellBaseVariantAnnotator(cellBaseClient);

        URI test = annotator.createAnnotation(variantDBAdaptor, Paths.get("/tmp"), "testREST", null);

        System.out.println(test.toString());
    }

    @Test
    public void testCreateAnnotationDBAdaptor() throws Exception {
        VariantDBAdaptor variantDBAdaptor = getDbAdaptor();

        /**
         * Connecting to CellBase database
         */
        CellbaseConfiguration cellbaseConfiguration = getCellbaseConfiguration();

        CellBaseVariantAnnotator annotator = new CellBaseVariantAnnotator(cellbaseConfiguration, cellbaseSpecies, cellbaseAssembly);

        URI test = annotator.createAnnotation(variantDBAdaptor, Paths.get("/tmp"), "testDBAdaptor", null);

        System.out.println(test.toString());

    }

    @Test
    public void testLoadAnnotation() throws Exception {
        VariantDBAdaptor variantDBAdaptor = getDbAdaptor();

        CellBaseVariantAnnotator annotator = new CellBaseVariantAnnotator();

        annotator.loadAnnotation(variantDBAdaptor, URI.create("file:///tmp/testREST.annot.json.gz"), new QueryOptions());

    }


    private CellbaseConfiguration getCellbaseConfiguration() {
        CellbaseConfiguration cellbaseConfiguration = new CellbaseConfiguration();
        String mongoHost = "mongodb-hxvm-var-001.ebi.ac.uk";
        int mongoPort = 27017;
        String mongoDbName = "cellbase_agambiae_agamp4_v3";
        String mongoUser = "biouser";
        String mongoPassword = "B10p@ss";

        cellbaseConfiguration.addSpeciesConnection(cellbaseSpecies, cellbaseAssembly, mongoHost,
                mongoDbName, mongoPort, "mongo",
                mongoUser, mongoPassword, 10, 500);
        cellbaseConfiguration.addSpeciesAlias(cellbaseSpecies, cellbaseSpecies);

        System.out.println(cellbaseSpecies + "-" + cellbaseAssembly);
        return cellbaseConfiguration;
    }

    private VariantDBAdaptor getDbAdaptor() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Config.setGcsaHome("/opt/opencga/");
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();

        String dbName;
//        dbName = "testCompression";
        dbName = "bierapp";
        return variantStorageManager.getDBAdaptor(dbName, null);
    }
}