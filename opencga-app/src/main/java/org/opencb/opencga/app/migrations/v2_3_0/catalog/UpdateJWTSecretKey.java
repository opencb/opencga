package org.opencb.opencga.app.migrations.v2_3_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.opencb.opencga.catalog.auth.authentication.JwtManager;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.PasswordUtils;

@Migration(id = "update_jwt_secret_key_TASK-807",
        description = "Update JWT secret key #TASK-807", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220513)
public class UpdateJWTSecretKey extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MongoCollection<Document> collection = getMongoCollection(MongoDBAdaptorFactory.METADATA_COLLECTION);
        collection.updateOne(new Document(), Updates.set("admin.secretKey",
                PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH)));
        logger.info("JWT secret key successfully changed");
    }

}
