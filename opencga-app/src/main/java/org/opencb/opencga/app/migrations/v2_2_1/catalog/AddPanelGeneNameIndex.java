package org.opencb.opencga.app.migrations.v2_2_1.catalog;

import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_panel_gene_name_index",
        description = "Add Panel gene.name index #TASK-602", version = "2.2.1",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220406)
public class AddPanelGeneNameIndex extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Document newIndex = new Document()
                .append("genes.name", 1)
                .append("studyUid", 1);
        createIndex(MongoDBAdaptorFactory.PANEL_COLLECTION, newIndex);
    }

}
