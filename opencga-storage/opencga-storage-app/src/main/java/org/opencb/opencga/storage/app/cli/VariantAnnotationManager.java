package org.opencb.opencga.storage.app.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.opencb.biodata.models.variation.GenomicVariant;
import org.opencb.cellbase.core.common.core.CellbaseConfiguration;
import org.opencb.cellbase.core.lib.DBAdaptorFactory;
import org.opencb.cellbase.core.lib.api.variation.VariantAnnotationDBAdaptor;
import org.opencb.cellbase.core.lib.dbquery.QueryResult;
import org.opencb.cellbase.lib.mongodb.db.MongoDBAdaptorFactory;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

/**
 * Created by imedina on 19/12/14.
 */
public class VariantAnnotationManager {

    private MongoCredentials cellbaseCredentials;
    private MongoCredentials opencgaStorageEngineCredentials;

    private ObjectMapper mapper;
    private ObjectWriter writer;

    public VariantAnnotationManager(MongoCredentials cellbaseCredentials, MongoCredentials opencgaStorageEngineCredentials) {
        this.cellbaseCredentials = cellbaseCredentials;
        this.opencgaStorageEngineCredentials = opencgaStorageEngineCredentials;

        mapper = new ObjectMapper();
        writer = mapper.writer();
    }

//      ./opencga-storage.sh annotate-variants --opencga-database eva_agambiae_agamp4  --opencga-password B10p@ss
//      --cellbase-species agambiae  --cellbase-assembly "GRCh37" --cellbase-host mongodb-hxvm-var-001
//      --opencga-user biouser --opencga-port 27017    --opencga-host mongodb-hxvm-var-001    --cellbase-user biouser
//      --cellbase-port 27017    --cellbase-password B10p@ss    --cellbase-database cellbase_agambiae_agamp4_v3


    public void annotate(String cellbaseSpecies, String cellbaseAssembly, QueryOptions options) {

        /**
         * Connecting to CellBase database
         */
        CellbaseConfiguration cellbaseConfiguration = new CellbaseConfiguration();
        cellbaseConfiguration.addSpeciesConnection(cellbaseSpecies, cellbaseAssembly, cellbaseCredentials.getMongoHost(),
                cellbaseCredentials.getMongoDbName(), cellbaseCredentials.getMongoPort(), "mongo",
                cellbaseCredentials.getUsername(), String.copyValueOf(cellbaseCredentials.getPassword()), 10, 10);
        cellbaseConfiguration.addSpeciesAlias(cellbaseSpecies, cellbaseSpecies);

        System.out.println(cellbaseSpecies + "-" + cellbaseAssembly);
        DBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(cellbaseConfiguration);
        VariantAnnotationDBAdaptor variantAnnotationDBAdaptor = dbAdaptorFactory.getGenomicVariantAnnotationDBAdaptor(cellbaseSpecies, cellbaseAssembly);

        /**
         * Connecting to OpenCGA Variant database
         */
        MongoDataStoreManager openCGADataStoreManager = new MongoDataStoreManager(opencgaStorageEngineCredentials.getMongoHost(),
                opencgaStorageEngineCredentials.getMongoPort());
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", opencgaStorageEngineCredentials.getUsername())
                .add("password", String.copyValueOf(opencgaStorageEngineCredentials.getPassword()))
                .build();
        MongoDataStore mongoDataStore = openCGADataStoreManager.get(opencgaStorageEngineCredentials.getMongoDbName(), mongoDBConfiguration);
        MongoDBCollection variantDBCollection = mongoDataStore.getCollection("variants_0_9");

        /**
         * We are going to start reading variants from OpenCGA
         * and calculate the annotations using CellBase.
         */
        DBObject query = new BasicDBObject();
        QueryOptions queryOptions = new QueryOptions("limit", 1000);
        DBCursor cursor = variantDBCollection.nativeQuery().find(query, queryOptions);
        while (cursor.hasNext()) {
            DBObject dbObject = cursor.next();
//            System.out.println("cursor.toString() = " + cursor.next().toString());
            GenomicVariant genomicVariant = new GenomicVariant(dbObject.get("chr").toString(), Integer.parseInt(dbObject.get("start").toString()),
                    dbObject.get("ref").toString(), dbObject.get("alt").toString());
            System.out.println("genomicVariant = " + genomicVariant);
            QueryResult queryResult = variantAnnotationDBAdaptor.getAllConsequenceTypesByVariant(genomicVariant, new org.opencb.cellbase.core.lib.dbquery.QueryOptions());
//            List<ConsequenceType> consequenceTypeList = (List<ConsequenceType>) queryResult.getNumResults();
            if(queryResult.getNumResults() > 0) {
                System.out.println("queryResult = " + queryResult.getNumResults());
                try {
                    System.out.println("queryResult = " + writer.writeValueAsString(queryResult.getResult()));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
