package org.opencb.opencga.storage.mongodb.variant.annotator;

import org.junit.Test;
import org.opencb.cellbase.core.client.CellBaseClient;
import org.opencb.cellbase.core.common.core.CellbaseConfiguration;
import org.opencb.cellbase.core.lib.DBAdaptorFactory;
import org.opencb.cellbase.core.lib.api.variation.VariantAnnotationDBAdaptor;
import org.opencb.cellbase.lib.mongodb.db.MongoDBAdaptorFactory;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.CellBaseVariantAnnotator;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

import java.net.URI;
import java.nio.file.Paths;

public class CellBaseVariantAnnotatorTest extends GenericTest {

    @Test
    public void testCreateAnnotationREST() throws Exception {
        VariantDBAdaptor variantDBAdaptor = getDbAdaptor();

        CellBaseClient cellBaseClient;
//        cellBaseClient = new CellBaseClient("wwwdev.ebi.ac.uk", 80, "/cellbase/webservices/rest/", "v3", "hsapiens");
        cellBaseClient = new CellBaseClient("cafetal", 8080, "/cellbase/webservices/rest/", "v3", "hsapiens");

        CellBaseVariantAnnotator annotator = new CellBaseVariantAnnotator(cellBaseClient);

        URI test = annotator.createAnnotation(variantDBAdaptor, Paths.get("/tmp"), "test", null);

        System.out.println(test.toString());
    }

    @Test
    public void testCreateAnnotationDBAdaptor() throws Exception {
        VariantDBAdaptor variantDBAdaptor = getDbAdaptor();

        /**
         * Connecting to CellBase database
         */
        CellbaseConfiguration cellbaseConfiguration = new CellbaseConfiguration();
        String cellbaseSpecies = "hsapiens";
        String cellbaseAssembly = "GRCh37";
        MongoCredentials cellbaseCredentials = new MongoCredentials("mongodb-hxvm-var-001", 27017, "cellbase_agambiae_agamp4_v3", "biouser", "B10p@ss");

        //      ./opencga-storage.sh annotate-variants --opencga-database eva_agambiae_agamp4  --opencga-password B10p@ss
//      --cellbase-species agambiae  --cellbase-assembly "GRCh37" --cellbase-host mongodb-hxvm-var-001
//      --opencga-user biouser --opencga-port 27017    --opencga-host mongodb-hxvm-var-001    --cellbase-user biouser
//      --cellbase-port 27017    --cellbase-password B10p@ss    --cellbase-database cellbase_agambiae_agamp4_v3

        cellbaseConfiguration.addSpeciesConnection(cellbaseSpecies, cellbaseAssembly, cellbaseCredentials.getMongoHost(),
                cellbaseCredentials.getMongoDbName(), cellbaseCredentials.getMongoPort(), "mongo",
                cellbaseCredentials.getUsername(), String.copyValueOf(cellbaseCredentials.getPassword()), 10, 10);
        cellbaseConfiguration.addSpeciesAlias(cellbaseSpecies, cellbaseSpecies);

        System.out.println(cellbaseSpecies + "-" + cellbaseAssembly);
        DBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(cellbaseConfiguration);
        VariantAnnotationDBAdaptor variantAnnotationDBAdaptor = dbAdaptorFactory.getGenomicVariantAnnotationDBAdaptor(cellbaseSpecies, cellbaseAssembly);


        CellBaseVariantAnnotator annotator = new CellBaseVariantAnnotator(variantAnnotationDBAdaptor);

        URI test = annotator.createAnnotation(variantDBAdaptor, Paths.get("/tmp"), "test", null);

        System.out.println(test.toString());

    }

    public void testLoadAnnotation() throws Exception {

    }

    private VariantDBAdaptor getDbAdaptor() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Config.setGcsaHome("/opt/opencga/");
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();

        String dbName = "bierapp";
        return variantStorageManager.getDBAdaptor(dbName, null);
    }
}