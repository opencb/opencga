package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.*;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "fix_family_references_in_individual",
        description = "Fix Family references, #TASK-489", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220324)
public class FixFamilyReferences extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // Get all the family ids: studyUid - {familyIds}
        Map<String, Set<String>> familyIds = new HashMap<>();
        queryMongo(MongoDBAdaptorFactory.FAMILY_COLLECTION, new Document(),
                Projections.include(Arrays.asList("id", "studyUid")), (doc) -> {
                    String studyId = doc.get("studyUid", Number.class).toString();
                    if (!familyIds.containsKey(studyId)) {
                        familyIds.put(studyId, new HashSet<>());
                    }
                    familyIds.get(studyId).add(doc.getString("id"));
                });

        migrateCollection(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                new Document("familyIds", new Document("$ne", Collections.emptyList())),
                Projections.include("familyIds", "studyUid"),
                (individual, bulk) -> {
                    String studyId = individual.get("studyUid", Number.class).toString();

                    if (!familyIds.containsKey(studyId)) {
                        return;
                    }

                    boolean changed = false;
                    List<String> newIds = new ArrayList<>();
                    List<String> iFamilyIds = individual.getList("familyIds", String.class);

                    // Store all existing familyIds in newIds so non-existing ones are removed
                    for (String iFamilyId : iFamilyIds) {
                        if (familyIds.get(studyId).contains(iFamilyId)) {
                            newIds.add(iFamilyId);
                        } else {
                            // Found a non-existing familyId, so we'll need to apply the update !
                            changed = true;
                        }
                    }

                    if (changed) {
                        bulk.add(new UpdateOneModel<>(
                                        eq("_id", individual.get("_id")),
                                        new Document("$set", new Document("familyIds", newIds))
                                )
                        );
                    }
                }
        );
    }

}
