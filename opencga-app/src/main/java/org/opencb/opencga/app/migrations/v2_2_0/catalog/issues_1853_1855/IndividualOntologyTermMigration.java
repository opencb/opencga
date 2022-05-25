package org.opencb.opencga.app.migrations.v2_2_0.catalog.issues_1853_1855;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.biodata.models.core.OntologyTermAnnotation;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.HashMap;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "individual_ontology_term_migration_#1853",
        description = "Change sex and ethnicity types for OntologyTermAnnotation #1855", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211203)
public class IndividualOntologyTermMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                new Document("sex.id", new Document("$exists", false)),
                Projections.include("_id", "sex", "ethnicity"),
                (doc, bulk) -> {
                    String sex = doc.getString("sex");
                    SexOntologyTermAnnotation sexAnnotation = new SexOntologyTermAnnotation(sex, "", "", "", "", new HashMap<>());
                    Document sexDoc = convertToDocument(sexAnnotation);

                    String ethnicity = doc.getString("ethnicity");
                    OntologyTermAnnotation ethnicityAnnotation = new OntologyTermAnnotation(ethnicity, "", "", "", "", new HashMap<>());
                    Document ethnicityDoc = convertToDocument(ethnicityAnnotation);

                    bulk.add(new UpdateOneModel<>(
                            eq("_id", doc.get("_id")),
                            new Document("$set", new Document()
                                    .append("sex", sexDoc)
                                    .append("ethnicity", ethnicityDoc)
                            ))
                    );
                }
        );

        MongoCollection<Document> collection = getMongoCollection(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION);
        dropIndex(collection, new Document().append("sex", 1).append("studyUid", 1));
        dropIndex(collection, new Document().append("ethnicity", 1).append("studyUid", 1));
        createIndex(collection, new Document().append("sex.id", 1).append("studyUid", 1), new IndexOptions().background(true));
        createIndex(collection, new Document().append("ethnicity.id", 1).append("studyUid", 1), new IndexOptions().background(true));
    }

}
