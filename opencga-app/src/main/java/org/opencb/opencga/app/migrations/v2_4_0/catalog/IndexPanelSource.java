package org.opencb.opencga.app.migrations.v2_4_0.catalog;

import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;

@Migration(id = "index_panel_source_TASK-473",
        description = "Index panel source #TASK-473", version = "2.4.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220530)
public class IndexPanelSource extends MigrationTool {

    @Override
    protected void run() throws Exception {
        createIndexes(Arrays.asList(
                        MongoDBAdaptorFactory.PANEL_COLLECTION,
                        MongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION
                ),
                Arrays.asList(
                        new Document()
                                .append("source.id", 1)
                                .append("studyUid", 1),
                        new Document()
                                .append("source.name", 1)
                                .append("studyUid", 1)
                )
        );

    }

}
