package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileInternalAlignment;
import org.opencb.opencga.core.models.file.FileInternalVariant;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.include;

@Migration(id = "new_file_internal_index_1850",
        description = "Add new FileInternalVariant and FileInternalAlignment index #1850", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211127)
public class AddNewFileInternalIndex_1850 extends MigrationTool {

    @Override
    protected void run() throws Exception {
        final AtomicInteger filesWithNewInternalField = new AtomicInteger();
        final AtomicInteger filesWithoutNewInternalField = new AtomicInteger();

        migrateCollection(MongoDBAdaptorFactory.FILE_COLLECTION,
                and(
                        in("bioformat", File.Bioformat.VARIANT.toString(), File.Bioformat.ALIGNMENT.toString()),
                        exists("internal")
                ),
                include("_id", "internal", "bioformat"),
                (doc, bulk) -> {
                    File.Bioformat bioformat = File.Bioformat.valueOf(doc.getString("bioformat"));
                    Document internal = doc.get("internal", Document.class);
                    Document index = internal.get("index", Document.class);

                    if (internal.containsKey("variant") || internal.containsKey("alignment")) {
                        // Skip file. It already has the new internal field.
                        filesWithNewInternalField.incrementAndGet();
                        return;
                    } else {
                        filesWithoutNewInternalField.incrementAndGet();
                    }

                    FileInternalVariant variant = FileInternalVariant.init();
                    FileInternalAlignment alignment = FileInternalAlignment.init();

                    if (index != null) {
                        Number release = index.get("release", Number.class);
                        if (release != null) {
                            variant.getIndex().setRelease(release.intValue());
                        }

                        Document status = index.get("status", Document.class);
                        if (status != null) {
                            InternalStatus internalStatus = null;
                            if (bioformat == File.Bioformat.VARIANT) {
                                internalStatus = variant.getIndex().getStatus();
                            } else if (bioformat == File.Bioformat.ALIGNMENT) {
                                // Initialise new Alignment internal status
                                internalStatus = new InternalStatus(InternalStatus.READY);
                                alignment.getIndex().setStatus(internalStatus);
                            }

                            if (internalStatus != null) {
                                String statusId = status.getString("name");
                                String description = status.getString("description");
                                String date = status.getString("date");

                                if (statusId != null) {
                                    internalStatus.setId(statusId);
                                    internalStatus.setName(statusId);
                                }
                                if (description != null) {
                                    internalStatus.setDescription(description);
                                }
                                if (date != null) {
                                    internalStatus.setDate(date);
                                }
                            }
                        }

                        Document transformedFile = index.get("transformedFile", Document.class);
                        if (transformedFile != null) {
                            Number uid = transformedFile.get("id", Number.class);
                            Number metadataUid = transformedFile.get("metadataId", Number.class);

                            if (uid != null && uid.longValue() > 0) {
                                variant.getIndex().getTransform().setFileId(getFileId(uid.longValue()));
                            }
                            if (metadataUid != null && metadataUid.longValue() > 0) {
                                variant.getIndex().getTransform().setMetadataFileId(getFileId(metadataUid.longValue()));
                            }
                        }

                        Document localFileIndex = index.get("localFileIndex", Document.class);
                        if (localFileIndex != null) {
                            Number fileUid = localFileIndex.get("fileId", Number.class);
                            String indexer = localFileIndex.getString("indexer");

                            if (fileUid != null && fileUid.longValue() > 0) {
                                alignment.getIndex().setFileId(getFileId(fileUid.longValue()));
                            }
                            if (indexer != null) {
                                alignment.getIndex().setIndexer(indexer);
                            }
                        }
                    }

                    Document variantDoc = convertToDocument(variant);
                    Document alignmentDoc = convertToDocument(alignment);

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", doc.get("_id")),
                                    new Document()
                                            .append("$set", new Document()
                                                    .append("internal.variant", variantDoc)
                                                    .append("internal.alignment", alignmentDoc)
                                            )
                                            .append("$unset", new Document("internal.index", ""))
                            )
                    );
                });
        logger.info("Finish add new file internal index field.");
        logger.info(" - Total files inspected : {}", filesWithNewInternalField.intValue() + filesWithoutNewInternalField.intValue());
        logger.info(" - Files updated : {}", filesWithoutNewInternalField.intValue());
        logger.info(" - Files already updated (skipped) : {}", filesWithNewInternalField.intValue());
    }

    private String getFileId(long fileUid) {
        OpenCGAResult<File> fileOpenCGAResult;
        try {
            fileOpenCGAResult = dbAdaptorFactory.getCatalogFileDBAdaptor().get(fileUid, FileManager.INCLUDE_FILE_IDS);
        } catch (CatalogException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        if (fileOpenCGAResult.getNumResults() == 0) {
            throw new RuntimeException("Could not find 'id' for file 'uid': " + fileUid);
        }
        return fileOpenCGAResult.first().getId();

    }

}
