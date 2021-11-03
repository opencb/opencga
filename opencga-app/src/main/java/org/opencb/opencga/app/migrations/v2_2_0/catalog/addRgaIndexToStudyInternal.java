package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_rga_index_summary_to_study_internal",
        description = "Add RGA Index information to Study Internal #", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        date = 20210719)
public class addRgaIndexToStudyInternal extends MigrationTool {

    @Override
    protected void run() throws Exception {

        migrateCollection(MongoDBAdaptorFactory.STUDY_COLLECTION,
                new Document("internal.index", new Document("$exists", false)),
                Projections.include("_id", StudyDBAdaptor.QueryParams.CREATION_DATE.key(), "internal"),
                (study, bulk) -> {

                    String creationDate = study.getString(StudyDBAdaptor.QueryParams.CREATION_DATE.key());
                    Document internal = study.get("internal", Document.class);

                    internal.put("index", new Document("recessiveGene", new Document()
                            .append("status", "NOT_INDEXED")
                            .append("modificationDate", creationDate)
                    ));

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", study.get("_id")),
                                    new Document("$set", new Document("internal", internal))
                            )
                    );
                }
        );
    }
}
