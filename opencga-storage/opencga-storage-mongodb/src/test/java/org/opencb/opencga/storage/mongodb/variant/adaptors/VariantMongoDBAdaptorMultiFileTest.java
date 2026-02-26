package org.opencb.opencga.storage.mongodb.variant.adaptors;

import org.bson.Document;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorMultiFileTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * Created on 24/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class VariantMongoDBAdaptorMultiFileTest extends VariantDBAdaptorMultiFileTest implements MongoDBVariantStorageTest {

    @Before
    public void setUpLoggers() throws Exception {
        logLevel("debug");
    }

    @After
    public void resetLoggers() throws Exception {
        logLevel("info");
    }

    @Override
    public ObjectMap getOptions() {
        return new ObjectMap(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
    }

    @Override
    public void testGetAllVariants_format() {
        Assume.assumeTrue(false);
        super.testGetAllVariants_format();
    }

    /**
     * Verify that nativeIterator applies the aggregation pipeline file-array filter correctly:
     * when INCLUDE_FILE specifies 2 of the 4 files in study1, each returned document's root
     * {@code files[]} array must contain only entries belonging to those 2 files.
     */
    @Test
    public void testNativeIteratorFileProjection() throws Exception {
        VariantMongoDBAdaptor mongoAdaptor = (VariantMongoDBAdaptor) dbAdaptor;
        VariantStorageMetadataManager mm = mongoAdaptor.getMetadataManager();

        int studyId = mm.getStudyId(study1);
        int fileId1 = mm.getFileId(studyId, file12877);
        int fileId2 = mm.getFileId(studyId, file12878);
        Set<Integer> expectedFileIds = new HashSet<>();
        expectedFileIds.add(fileId1);
        expectedFileIds.add(fileId2);

        Query query = new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877 + "," + file12878)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleNA12877 + "," + sampleNA12878);
        ParsedVariantQuery parsedQuery = variantStorageEngine.parseQuery(query, new QueryOptions());

        int docCount = 0;
        int docWithFilesCount = 0;
        try (MongoDBIterator<Document> iter = mongoAdaptor.nativeIterator(parsedQuery, new QueryOptions(), false)) {
            while (iter.hasNext()) {
                Document doc = iter.next();
                docCount++;
                List<Document> files = doc.getList(DocumentToVariantConverter.FILES_FIELD, Document.class);
                if (files == null || files.isEmpty()) {
                    continue;
                }
                docWithFilesCount++;
                // Must have at most 2 file entries (one per requested file)
                assertTrue("Expected at most 2 file entries, got " + files.size() + " in doc " + doc.get("_id"),
                        files.size() <= 2);
                for (Document fileDoc : files) {
                    int sid = ((Number) fileDoc.get(DocumentToStudyEntryConverter.STUDYID_FIELD)).intValue();
                    int fid = ((Number) fileDoc.get(DocumentToStudyEntryConverter.FILEID_FIELD)).intValue();
                    // Only entries from study1 are expected (other studies have no requested files)
                    if (sid == studyId) {
                        assertTrue("Unexpected file id " + fid + " in doc " + doc.get("_id"),
                                expectedFileIds.contains(fid));
                    }
                }
            }
        }

        assertTrue("nativeIterator returned no documents", docCount > 0);
        assertTrue("No document had any files[] entries", docWithFilesCount > 0);

        // Sanity: querying all 4 files must return documents where some have more than 2 file entries
        Query queryAll = new Query()
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study1)
                .append(VariantQueryParam.INCLUDE_FILE.key(), file12877 + "," + file12878 + "," + file12879 + "," + file12880)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(),
                        sampleNA12877 + "," + sampleNA12878 + "," + sampleNA12879 + "," + sampleNA12880);
        ParsedVariantQuery parsedQueryAll = variantStorageEngine.parseQuery(queryAll, new QueryOptions());
        boolean foundMoreThanTwo = false;
        try (MongoDBIterator<Document> iter = mongoAdaptor.nativeIterator(parsedQueryAll, new QueryOptions(), false)) {
            while (iter.hasNext()) {
                Document doc = iter.next();
                List<Document> files = doc.getList(DocumentToVariantConverter.FILES_FIELD, Document.class);
                if (files != null && files.size() > 2) {
                    foundMoreThanTwo = true;
                    break;
                }
            }
        }
        assertTrue("Sanity check: expected some documents with >2 files when requesting all 4 files", foundMoreThanTwo);
    }
}