package org.opencb.opencga.app.migrations.v5.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Migration(id = "external_tool__task_7610",
        description = "Extend Workflow data model for new ExternalTool, #TASK-7610", version = "5.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250506)
public class ExternalToolTask7610Migration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.exists("manager");
        Bson projection = new Document();
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.DEPRECATED_WORKFLOW_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DEPRECATED_WORKFLOW_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DEPRECATED_DELETED_WORKFLOW_COLLECTION), query, projection, (document, bulk) -> {
            MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();

            updateDocument.getSet().put("scope", document.get("type"));
            updateDocument.getSet().put("type", "WORKFLOW");
            Document repository = document.get("repository", Document.class);
            if (repository != null) {
                repository.put("name", repository.get("id"));
                repository.put("tag", repository.get("version"));
                repository.put("user", "");
                repository.put("password", "");
                repository.remove("id");
                repository.remove("version");
            }
            List<Document> scripts = document.getList("scripts", Document.class);
            if (CollectionUtils.isNotEmpty(scripts)) {
                for (Document script : scripts) {
                    script.put("name", script.get("fileName"));
                    script.remove("fileName");
                }
            }

            updateDocument.getSet().put("workflow", new Document()
                    .append("manager", document.get("manager"))
                    .append("repository", repository)
                    .append("scripts", scripts)
            );
            updateDocument.getUnset().add("manager");
            updateDocument.getUnset().add("repository");
            updateDocument.getUnset().add("scripts");
            updateDocument.getSet().put("container", null);

            bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
        });

        renameCollection(OrganizationMongoDBAdaptorFactory.DEPRECATED_WORKFLOW_COLLECTION, OrganizationMongoDBAdaptorFactory.EXTERNAL_TOOL_COLLECTION);
        renameCollection(OrganizationMongoDBAdaptorFactory.DEPRECATED_WORKFLOW_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.EXTERNAL_TOOL_ARCHIVE_COLLECTION);
        renameCollection(OrganizationMongoDBAdaptorFactory.DEPRECATED_DELETED_WORKFLOW_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_EXTERNAL_TOOL_COLLECTION);

        // Migrate Study permissions from WORKFLOW_* to USER_TOOL_*
        Bson studyQuery = Filters.or(Filters.exists("_acl"), Filters.exists("_userAcls"));
        Bson studyProjection = Projections.include("_id", "_acl", "_userAcls");
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION), studyQuery, studyProjection, (document, bulk) -> {
            MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
            boolean needsUpdate = false;

            // Update _acl array (format: "member__permission")
            List<String> acl = document.getList("_acl", String.class);
            if (CollectionUtils.isNotEmpty(acl)) {
                List<String> updatedAcl = renameWorkflowToUserToolPermissions(acl);
                if (updatedAcl != null) {
                    updateDocument.getSet().put("_acl", updatedAcl);
                    needsUpdate = true;
                }
            }

            // Update _userAcls array (format: "member__permission")
            List<String> userAcls = document.getList("_userAcls", String.class);
            if (CollectionUtils.isNotEmpty(userAcls)) {
                List<String> updatedUserAcls = renameWorkflowToUserToolPermissions(userAcls);
                if (updatedUserAcls != null) {
                    updateDocument.getSet().put("_userAcls", updatedUserAcls);
                    needsUpdate = true;
                }
            }

            if (needsUpdate) {
                bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
            }
        });

        // Update job type values
        for (String col : Arrays.asList(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_JOB_COLLECTION)) {
            MongoCollection<Document> mongoCollection = getMongoCollection(col);
            mongoCollection.updateMany(Filters.eq("type", "NATIVE"), new Document("$set", new Document("type", "NATIVE_TOOL")));
            mongoCollection.updateMany(Filters.eq("type", "CUSTOM"), new Document("$set", new Document("type", "CUSTOM_TOOL")));
            mongoCollection.updateMany(Filters.eq("type", "WALKER"), new Document("$set", new Document("type", "VARIANT_WALKER")));
        }
    }

    /**
     * Renames workflow permissions to user tool permissions by replacing WORKFLOWS with USER_TOOLS in the strings.
     * Works for both _acl and _userAcls arrays which use the format "member__permission".
     *
     * @param permissions List of permission strings to check and update
     * @return a new list with updated permissions, or null if no changes were made
     */
    private List<String> renameWorkflowToUserToolPermissions(List<String> permissions) {
        boolean changed = false;
        List<String> updatedPermissions = new ArrayList<>(permissions.size());

        for (String permission : permissions) {
            String newPermission = permission
                    .replace("__VIEW_WORKFLOWS", "__VIEW_USER_TOOLS")
                    .replace("__WRITE_WORKFLOWS", "__WRITE_USER_TOOLS")
                    .replace("__DELETE_WORKFLOWS", "__DELETE_USER_TOOLS");

            if (!permission.equals(newPermission)) {
                changed = true;
            }
            updatedPermissions.add(newPermission);
        }

        return changed ? updatedPermissions : null;
    }

}
