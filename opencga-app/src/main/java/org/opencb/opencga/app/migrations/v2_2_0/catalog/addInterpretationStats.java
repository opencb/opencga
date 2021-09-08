package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.InterpretationConverter;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.catalog.utils.InterpretationUtils;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.InterpretationStats;

import static com.mongodb.client.model.Filters.eq;

@Migration(id="add_interpretation_stats",
        description = "Add interpretation stats #1819", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        rank = 4)
public class addInterpretationStats extends MigrationTool {

    @Override
    protected void run() throws Exception {
        InterpretationConverter converter = new InterpretationConverter();
        GenericDocumentComplexConverter<InterpretationStats> statsConverter = new GenericDocumentComplexConverter<>(InterpretationStats.class);

        migrateCollection(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION,
                new Document(InterpretationDBAdaptor.QueryParams.STATS.key(), new Document("$exists", false)),
                Projections.include("_id", InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(),
                        InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key()),
                (interpretationDoc, bulk) -> {
                    Interpretation interpretation = converter.convertToDataModelType(interpretationDoc);
                    InterpretationStats interpretationStats = InterpretationUtils.calculateStats(interpretation);
                    Document statsDocument = statsConverter.convertToStorageType(interpretationStats);

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", interpretationDoc.get("_id")),
                                    new Document("$set", new Document(InterpretationDBAdaptor.QueryParams.STATS.key(), statsDocument))
                            )
                    );
                }
        );
    }
}
