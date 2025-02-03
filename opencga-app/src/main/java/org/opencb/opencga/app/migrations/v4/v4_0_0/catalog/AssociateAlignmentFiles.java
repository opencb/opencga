package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;

import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.ArrayList;
import java.util.List;

@Migration(id = "associate_alignment_files__task_5662",
        description = "Associate BAM files with BAI and BIGWIG files and CRAM files with CRAI files #5662", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20241028)
public class AssociateAlignmentFiles extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson studyProjection = Projections.include(StudyDBAdaptor.QueryParams.FQN.key());
        List<String> studyFqns = new ArrayList<>();
        queryMongo(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION, new Document(), studyProjection, document -> {
            studyFqns.add(document.getString(StudyDBAdaptor.QueryParams.FQN.key()));
        });

        for (String studyFqn : studyFqns) {
            logger.info("Checking alignment files from study '{}'...", studyFqn);
            catalogManager.getFileManager().associateAlignmentFiles(studyFqn, token);
        }
    }

}
