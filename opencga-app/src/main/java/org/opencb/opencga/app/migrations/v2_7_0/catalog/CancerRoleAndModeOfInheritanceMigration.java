package org.opencb.opencga.app.migrations.v2_7_0.catalog;


import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "panel_roles_and_modesOfInheritance" ,
        description = "Change panel cancer.role and modeOfInheritance for cancer.roles[] and modesOfInheritance[]",
        version = "2.7.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20230117
)
public class CancerRoleAndModeOfInheritanceMigration extends MigrationTool {

    private List<Document> replaceFields(Document doc, String key) {
        List<Document> list = doc.getList(key, Document.class);
        if (CollectionUtils.isNotEmpty(list)) {
            for (Document common : list) {
                String modeOfInheritance = common.getString("modeOfInheritance");
                common.put("modesOfInheritance",
                        StringUtils.isNotEmpty(modeOfInheritance)
                                ? Collections.singletonList(modeOfInheritance)
                                : Collections.emptyList()
                );
                common.remove("modeOfInheritance");

                Document cancer = common.get("cancer", Document.class);
                if (cancer != null) {
                    String role = cancer.getString("role");
                    cancer.put("roles",
                            StringUtils.isNotEmpty(role)
                                    ? Collections.singletonList(role)
                                    : Collections.emptyList());
                    cancer.remove("role");
                }
            }
        }
        return list;
    }


    @Override
    protected void run() throws Exception {
        migrateCollection(Arrays.asList(MongoDBAdaptorFactory.PANEL_COLLECTION, MongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION,
                        MongoDBAdaptorFactory.DELETED_PANEL_COLLECTION),
                Filters.or(
                        Filters.exists("genes.modeOfInheritance"),
                        Filters.exists("genes.cancer.role"),
                        Filters.exists("variants.modeOfInheritance"),
                        Filters.exists("variants.cancer.role"),
                        Filters.exists("strs.modeOfInheritance"),
                        Filters.exists("strs.cancer.role"),
                        Filters.exists("regions.modeOfInheritance"),
                        Filters.exists("regions.cancer.role")
                ),
                Projections.include("genes", "variants", "strs", "regions"),
                (document, bulk) -> {
                    List<Document> genes = replaceFields(document, "genes");
                    List<Document> variants = replaceFields(document, "variants");
                    List<Document> strs = replaceFields(document, "strs");
                    List<Document> regions = replaceFields(document, "regions");

                    bulk.add(new UpdateOneModel<>(
                            eq("_id", document.get("_id")),
                            new Document("$set", new Document()
                                    .append("genes", genes)
                                    .append("variants", variants)
                                    .append("strs", strs)
                                    .append("regions", regions)
                            ))
                    );
                });
    }
}
