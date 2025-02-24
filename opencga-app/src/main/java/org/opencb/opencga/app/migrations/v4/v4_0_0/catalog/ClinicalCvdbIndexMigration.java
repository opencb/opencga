package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.clinical.ClinicalStatusValue;
import org.opencb.opencga.core.models.clinical.CvdbIndexStatus;

import java.util.Arrays;

@Migration(id = "add_cvdb_index_to_clinical_analysis",
        description = "Add CVDB index status to Clinical Analysis #TASK-5610", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20241118)
public class ClinicalCvdbIndexMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson closedCaseQuery = Filters.and(
                Filters.exists(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_CVDB_INDEX.key(), false),
                Filters.eq("status.type", ClinicalStatusValue.ClinicalStatusType.CLOSED)
        );
        Bson openCaseQuery = Filters.and(
                Filters.exists(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_CVDB_INDEX.key(), false),
                Filters.ne("status.type", ClinicalStatusValue.ClinicalStatusType.CLOSED)
        );
        CvdbIndexStatus pendingCvdbIndexStatus = new CvdbIndexStatus(CvdbIndexStatus.PENDING);
        Document pendingCvdbIndexDoc = convertToDocument(pendingCvdbIndexStatus);
        Bson updateClosedCase = Updates.set(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_CVDB_INDEX.key(), pendingCvdbIndexDoc);

        CvdbIndexStatus noneCvdbIndexStatus = new CvdbIndexStatus(CvdbIndexStatus.NONE);
        Document noneCvdbIndexDoc = convertToDocument(noneCvdbIndexStatus);
        Bson updateOpenCase = Updates.set(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_CVDB_INDEX.key(), noneCvdbIndexDoc);

        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION)) {
            getMongoCollection(collection).updateMany(closedCaseQuery, updateClosedCase);
            getMongoCollection(collection).updateMany(openCaseQuery, updateOpenCase);
        }
    }

}
