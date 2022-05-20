package org.opencb.opencga.app.migrations.v2_3_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.panel.PanelInternal;

@Migration(id = "add_panel_internal_field-TASK_734",
        description = "Add 'internal' to Panel data model #TASK-734", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220509)
public class AddPanelInternalField extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MongoCollection<Document> collection = getMongoCollection(MongoDBAdaptorFactory.PANEL_COLLECTION);

        PanelInternal internal = PanelInternal.init();
        internal.getStatus().setDescription("Internal status added during migration");
        Document internalDoc = convertToDocument(internal);

        Bson query = Filters.exists("internal", false);
        Bson update = Updates.set("internal", internalDoc);
        collection.updateMany(query, update);

        Document panelIndex = new Document()
                .append("internal.status.name", 1)
                .append("studyUid", 1);
        createIndex(collection, panelIndex);
    }
}
