package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.StudyConverter;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Migration(id = "delete_unused_variablesets",
        description = "Delete unused VariableSets, #1859", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 2,
        date = 20211210)
public class DeleteUnusedVariableSets extends MigrationTool {

    @Override
    protected void run() throws Exception {
        StudyConverter converter = new StudyConverter();
        MongoCursor<Document> studyIterator = getMongoCollection(MongoDBAdaptorFactory.STUDY_COLLECTION)
                .find(new Document())
                .projection(Projections.include("fqn", "variableSets")).iterator();

        List<Study> studyList = new ArrayList<>();
        while (studyIterator.hasNext()) {
            Document studyDoc = studyIterator.next();
            Study study = converter.convertToDataModelType(studyDoc);
            studyList.add(study);
        }

        for (Study study : studyList) {
            logger.info("Deleting VariableSets from study '{}'", study.getFqn());
            delete(study, "opencga_alignment_samtools_flagstat");
            delete(study, "opencga_alignment_stats");
            delete(study, "opencga_sample_qc");
            delete(study, "opencga_file_variant_stats");
        }
    }

    private void deleteAnnotationSets(VariableSet variableSet, String collection) {
        MongoCollection<Document> mongoCollection = getMongoCollection(collection);
        Bson query = new Document()
                .append("$or", Arrays.asList(
                    new Document("customAnnotationSets.vs", variableSet.getUid()),
                    new Document("customInternalAnnotationSets.vs", variableSet.getUid())
                ));
        Bson update = new Document()
                .append("$pull", new Document()
                        .append("customAnnotationSets", new Document("vs", variableSet.getUid())))
                        .append("customInternalAnnotationSets", new Document("vs", variableSet.getUid()))
                .append("$unset", new Document()
                        .append("_vsMap." + variableSet.getUid(), "")
                        .append("_ivsMap." + variableSet.getUid(), "")
                );
        mongoCollection.updateMany(query, update);
    }

    private void delete(Study study, String variableSetId) {
        // Find variable set
        VariableSet variableSet = null;
        for (VariableSet tmpVariableSet : study.getVariableSets()) {
            if (tmpVariableSet.getId().equals(variableSetId)) {
                variableSet = tmpVariableSet;
            }
        }
        if (variableSet == null) {
            return;
        }
        deleteAnnotationSets(variableSet, MongoDBAdaptorFactory.FILE_COLLECTION);
        deleteAnnotationSets(variableSet, MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        deleteAnnotationSets(variableSet, MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION);
        deleteAnnotationSets(variableSet, MongoDBAdaptorFactory.FAMILY_COLLECTION);
        deleteAnnotationSets(variableSet, MongoDBAdaptorFactory.COHORT_COLLECTION);

        try {
            catalogManager.getStudyManager().deleteVariableSet(study.getFqn(), variableSetId, true, token);
        } catch (CatalogException e) {
            logger.warn(e.getMessage());
        }
    }

}
