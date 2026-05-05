package org.opencb.opencga.storage.mongodb.variant;

import org.bson.Document;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineDeleteTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

import java.net.URI;
import java.util.List;

import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.FILE_GENOTYPE_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.SAMPLE_DATA_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.STUDYID_FIELD;

@Category(MediumTests.class)
public class MongoDBVariantStorageEngineDeleteTest extends VariantStorageEngineDeleteTest implements MongoDBVariantStorageTest {

    @Override
    protected void checkAfterRemoveFileAgainstReference(String removedStudy, String removedFileName,
                                                        List<String> otherStudies) throws Exception {
        // Build a fresh "expected" engine that mirrors the actual engine's post-removal state:
        // load every file the actual engine still has indexed, and pre-register (without loading)
        // the removed file so internal file/sample IDs stay aligned between engines. After this,
        // the two engines' variants collections should match document by document, proving the
        // removal correctly trimmed the per-file entries from the files-to-root schema rather
        // than leaving stale residue.
        //
        // This hook is wired to testRemoveFileAndPrune and knows that test's file selection:
        //   - removedStudy:   platinum[0] (kept) + platinum[1] (removed)
        //   - otherStudies[i]: platinum[2 + i]
        URI removedFileUri = getPlatinumFile(1);

        MongoDBVariantStorageEngine expected = getVariantStorageEngine("_expected");
        StudyMetadata expectedRemovedStudy = expected.getMetadataManager().createStudy(removedStudy);
        ObjectMap options = new ObjectMap()
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                .append(VariantStorageOptions.ANNOTATE.key(), false);

        // Kept file
        runDefaultETL(getPlatinumFile(0), expected, expectedRemovedStudy, options);
        // Pre-register the removed file to align sample/file IDs with the actual engine
        int registeredFileId = expected.getMetadataManager().registerFile(
                expectedRemovedStudy.getId(), UriUtils.fileName(removedFileUri));
        expected.getMetadataManager().registerFileSamples(expectedRemovedStudy.getId(), registeredFileId,
                expected.getVariantReaderUtils().readVariantFileMetadata(removedFileUri));

        // Mirror the untouched studies
        for (int i = 0; i < otherStudies.size(); i++) {
            StudyMetadata expectedOther = expected.getMetadataManager().createStudy(otherStudies.get(i));
            runDefaultETL(getPlatinumFile(2 + i), expected, expectedOther, options);
        }

        // Compare. Normalize sample-ID-dependent fields for the other studies: the "expected"
        // engine loads them with default ETL options while the actual engine loads them under
        // the Delete test's options, so sample IDs can diverge outside the removed study.
        // Also strip the 'stats' field: the actual engine computes stats during the test flow
        // but the expected engine does not, so stats content naturally differs and is irrelevant
        // to this test (which is about verifying the file/sample cleanup after removal).
        final int strictStudyId = expectedRemovedStudy.getId();
        VariantMongoDBAdaptor actualDbAdaptor = getVariantStorageEngine().getDBAdaptor();
        compareCollections(
                expected.getDBAdaptor().getVariantsCollection(),
                actualDbAdaptor.getVariantsCollection(),
                d -> {
                    d.remove(DocumentToVariantConverter.STATS_FIELD);
                    List<Document> filesDocs = d.getList(DocumentToVariantConverter.FILES_FIELD, Document.class);
                    if (filesDocs != null) {
                        for (Document fileDoc : filesDocs) {
                            if (fileDoc.getInteger(STUDYID_FIELD) != strictStudyId) {
                                fileDoc.remove(FILE_GENOTYPE_FIELD);
                                fileDoc.remove(SAMPLE_DATA_FIELD);
                            }
                        }
                    }
                    return d;
                });
    }
}
