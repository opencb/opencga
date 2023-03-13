package org.opencb.opencga.app.migrations.v2_8_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.api.ParamConstants;

@Migration(id = "calculate_pedigree_graph" ,
        description = "Calculate Pedigree Graph for all the families",
        version = "2.8.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20230313
)
public class CalculatePedigreeGraphMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        queryMongo(MongoDBAdaptorFactory.FAMILY_COLLECTION,
                Filters.exists("pedigreeGraph", false),
                Projections.include("studyUid", "uid", "id"), document -> {
                    Query query = new Query()
                            .append("studyUid", document.get("studyUid"))
                            .append("uid", document.get("uid"));

                    // Update pedigree graph
                    QueryOptions options = new QueryOptions(ParamConstants.FAMILY_UPDATE_PEDIGREEE_GRAPH_PARAM, true);
                    try {
                        dbAdaptorFactory.getCatalogFamilyDBAdaptor().update(query, new ObjectMap(), options);
                    } catch (CatalogDBException |CatalogParameterException | CatalogAuthorizationException e) {
                        logger.error("Could not migrate family '{}' ({}) from study uid {}", document.get("id"), document.get("uid"),
                                document.get("studyUid"));
                        throw new RuntimeException(e);
                    }
                });
    }

}
