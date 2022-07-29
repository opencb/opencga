package org.opencb.opencga.app.migrations.v2_4_2.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_clinical_discussion_TASK-1472",
        description = "Add ClinicalDiscussion #TASK-1472", version = "2.4.2",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220727)
public class AddClinicalDiscussion extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // Replace discussion from ClinicalAnalysis report.discussion
        migrateCollection(
                Arrays.asList(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION, MongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION),
                new Document("report.discussion.author", new Document("$exists", false)),
                Projections.include("report", "analyst"),
                ((document, bulk) -> {
                    Document report = document.get("report", Document.class);
                    if (report != null) {
                        String author = extractAuthor(document);
                        Document discussionDoc = migrateDiscussion(report, author);;
                        bulk.add(new UpdateOneModel<>(
                                        eq("_id", document.get("_id")),
                                        new Document("$set", new Document("report.discussion", discussionDoc))
                                )
                        );
                    }
                })
        );

        // We will be replacing the findings array which may be really big so we change the batch size to 1
        setBatchSize(1);
        // Replace discussion from ClinicalVariants from Interpretations primaryFindings.discussion, secondaryFindings.discussion
        migrateCollection(
                Arrays.asList(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION, MongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION,
                        MongoDBAdaptorFactory.DELETED_INTERPRETATION_COLLECTION),
                new Document("$or", Arrays.asList(
                        new Document("primaryFindings.discussion.author", new Document("$exists", false)),
                        new Document("secondaryFindings.discussion.author", new Document("$exists", false))
                )),
                Projections.include("primaryFindings", "secondaryFindings", "analyst"),
                ((document, bulk) -> {
                    String author = extractAuthor(document);
                    List<Document> primaryFindings = document.getList("primaryFindings", Document.class);
                    List<Document> secondaryFindings = document.getList("secondaryFindings", Document.class);
                    if (CollectionUtils.isNotEmpty(primaryFindings)) {
                        for (Document primaryFinding : primaryFindings) {
                            migrateClinicalVariant(primaryFinding, author);
                        }
                    }
                    if (CollectionUtils.isNotEmpty(secondaryFindings)) {
                        for (Document secondaryFinding : secondaryFindings) {
                            migrateClinicalVariant(secondaryFinding, author);
                        }
                    }

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", document.get("_id")),
                                    new Document("$set", new Document()
                                            .append("primaryFindings", primaryFindings)
                                            .append("secondaryFindings", secondaryFindings))
                            )
                    );
                })
        );
    }

    private void migrateClinicalVariant(Document finding, String author) {
        // Migrate discussion in root
        migrateDiscussion(finding, author);

        // Iterate over evidences
        List<Document> evidences = finding.getList("evidences", Document.class);
        if (CollectionUtils.isNotEmpty(evidences)) {
            for (Document evidence : evidences) {
                Document review = evidence.get("review", Document.class);
                if (review != null) {
                    migrateDiscussion(review, author);
                }
            }
        }
    }

    private Document migrateDiscussion(Document document, String author) {
        Object discussion = document.get("discussion");
        if (discussion == null || discussion instanceof String) {
            if (discussion != null) {
                ClinicalDiscussion cDiscussion = new ClinicalDiscussion(author, TimeUtils.getTime(), String.valueOf(discussion));
                discussion = convertToDocument(cDiscussion);
            } else {
                discussion = new Document();
            }
        }
        // Replace discussion field
        document.put("discussion", discussion);

        return (Document) discussion;
    }

    private String extractAuthor(Document document) {
        String author = "";
        Document analyst = document.get("analyst", Document.class);
        if (analyst != null) {
            String id = analyst.getString("id");
            if (StringUtils.isNotEmpty(id)) {
                author = id;
            }
        }
        return author;
    }

}
