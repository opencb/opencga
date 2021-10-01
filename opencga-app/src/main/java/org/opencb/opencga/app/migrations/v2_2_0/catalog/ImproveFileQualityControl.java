package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.biodata.formats.sequence.ascat.AscatMetrics;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.FileConverter;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileQualityControl;

import java.util.Collections;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "improve_quality_control",
        description = "Quality control normalize comments and fields #1826", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        rank = 7)
public class ImproveFileQualityControl extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // File
        improvementsFile();
    }

    private void improvementsFile() {
        FileConverter fileConverter = new FileConverter();
        migrateCollection(MongoDBAdaptorFactory.FILE_COLLECTION,
                new Document(FileDBAdaptor.QueryParams.QUALITY_CONTROL.key() + ".comments", new Document("$exists", false)),
                Projections.include("_id", FileDBAdaptor.QueryParams.QUALITY_CONTROL.key()),
                (fileDoc, bulk) -> {
                    //List<String> files = fileDoc.getList("files", String.class);
                    File file = fileConverter.convertToDataModelType(fileDoc);
                    FileQualityControl fqc = file.getQualityControl();
                    if (fqc != null) {
                        if (fqc.getComments() == null) {
                            fqc.setComments(Collections.emptyList());
                        }
                        if (fqc.getVariant() != null && fqc.getVariant().getAscatMetrics() == null) {
                            fqc.getVariant().setAscatMetrics(new AscatMetrics());
                        }

                        Document doc = fileConverter.convertToStorageType(file);
                        bulk.add(new UpdateOneModel<>(
                                        eq("_id", fileDoc.get("_id")),
                                        new Document("$set", new Document(FileDBAdaptor.QueryParams.QUALITY_CONTROL.key(),
                                                doc.get(FileDBAdaptor.QueryParams.QUALITY_CONTROL.key())))
                                )
                        );
                    }
                }
        );
    }
}
