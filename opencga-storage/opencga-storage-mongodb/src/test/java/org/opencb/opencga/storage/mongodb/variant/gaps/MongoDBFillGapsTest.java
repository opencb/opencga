package org.opencb.opencga.storage.mongodb.variant.gaps;

import org.bson.Document;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.gaps.FillGapsTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.*;

@Category(LongTests.class)
public class MongoDBFillGapsTest extends FillGapsTest implements MongoDBVariantStorageTest {

    @Override
    protected void logVariansStatus(VariantDBAdaptor dbAdaptor) throws Exception {
        // no op
    }

    @Override
    protected void logVariansStatus(StudyMetadata studyMetadata, VariantDBAdaptor dbAdaptor, URI outputUri) throws Exception {
        // no op
    }

    @Override
    @Test
    @Ignore("Requires aggregate() which is not supported in MongoDB")
    public void testFillMissingFilterNonRef() throws Exception {
    }

    @Override
    @Test
    @Ignore("Requires aggregate() which is not supported in MongoDB")
    public void testFillMissingPlatinumFiles() throws Exception {
    }

    /**
     * Reads raw MongoDB documents and verifies that aggregateFamily on the aggregated study
     * did not corrupt any data in the reference study, and did not modify any original file
     * documents in the aggregated study.
     *
     * <p>Uses the {@code _ovs} (overlapping status) field to distinguish gap-filled file documents
     * (added by aggregateFamily) from original ones (loaded normally).</p>
     *
     * <p>For each variant document:
     * <ul>
     *   <li>Reference study must have zero gap-filled file docs (no {@code _ovs} field)</li>
     *   <li>Each original (non-gap-filled) file doc in the aggregated study must have an identical
     *       counterpart in the reference study (same attrs, sampleData, alternates, ori; mgt and sfd
     *       compared with remapped sample IDs)</li>
     * </ul>
     */
    @Override
    protected void checkInputValuesAreUnmodified(String aggregatedStudy, String referenceStudy) throws Exception {
        VariantMongoDBAdaptor dbAdaptor = ((VariantMongoDBAdaptor) variantStorageEngine.getDBAdaptor());
        MongoDBCollection collection = dbAdaptor.getVariantsCollection();

        int studyId1 = metadataManager.getStudyId(aggregatedStudy);
        int studyId2 = metadataManager.getStudyId(referenceStudy);

        // Build fileId mapping: study1 fileId → study2 fileId (via shared file name)
        Map<Integer, Integer> fileId1To2 = new HashMap<>();
        for (int fid1 : metadataManager.getIndexedFiles(studyId1)) {
            FileMetadata fm1 = metadataManager.getFileMetadata(studyId1, fid1);
            int fid2 = metadataManager.getFileId(studyId2, fm1.getName());
            fileId1To2.put(fid1, fid2);
        }

        // Build sampleId mapping: study1 sampleId → study2 sampleId (via shared sample name)
        Map<Integer, Integer> sampleId1To2 = new HashMap<>();
        for (int fid1 : metadataManager.getIndexedFiles(studyId1)) {
            for (int sid1 : metadataManager.getFileMetadata(studyId1, fid1).getSamples()) {
                if (!sampleId1To2.containsKey(sid1)) {
                    String sampleName = metadataManager.getSampleName(studyId1, sid1);
                    int sid2 = metadataManager.getSampleId(studyId2, sampleName);
                    sampleId1To2.put(sid1, sid2);
                }
            }
        }

        int variantsChecked = 0;
        int filesCompared = 0;

        try (MongoDBIterator<Document> it = collection.nativeQuery().find(new Document(), new QueryOptions())) {
            while (it.hasNext()) {
                Document doc = it.next();
                String variantId = doc.getString("_id");
                List<Document> files = doc.get(DocumentToVariantConverter.FILES_FIELD, List.class);
                if (files == null || files.isEmpty()) {
                    continue;
                }

                // Partition file docs by study, separating original from gap-filled
                Map<Integer, Document> study1OrigByFid = new LinkedHashMap<>();
                Map<Integer, Document> study2ByFid = new LinkedHashMap<>();

                for (Document fileDoc : files) {
                    int sid = fileDoc.getInteger(STUDYID_FIELD);
                    int fid = fileDoc.getInteger(FILEID_FIELD);
                    boolean gapFilled = fileDoc.containsKey(MongoDBFillGapsFromFile.OVERLAPPING_STATUS_KEY);

                    if (sid == studyId2) {
                        assertFalse("Reference study should not have gap-filled file docs (_ovs) at " + variantId,
                                gapFilled);
                        study2ByFid.put(fid, fileDoc);
                    } else if (sid == studyId1 && !gapFilled) {
                        study1OrigByFid.put(fid, fileDoc);
                    }
                    // gap-filled docs (has _ovs) in study1 are expected additions — skip
                }

                // Compare each original study1 file with its study2 counterpart
                for (Map.Entry<Integer, Document> entry : study1OrigByFid.entrySet()) {
                    int fid1 = entry.getKey();
                    Document f1 = entry.getValue();
                    Integer fid2 = fileId1To2.get(Math.abs(fid1));
                    assertNotNull("No file mapping for fid=" + fid1 + " at " + variantId, fid2);
                    Document f2 = study2ByFid.remove(fid2);
                    assertNotNull("Missing reference file fid=" + fid2 + " at " + variantId, f2);

                    // Fields without study-specific IDs — direct comparison
                    assertEquals(variantId + " attrs fid=" + fid1,
                            f1.get(ATTRIBUTES_FIELD), f2.get(ATTRIBUTES_FIELD));
                    assertEquals(variantId + " sampleData fid=" + fid1,
                            f1.get(SAMPLE_DATA_FIELD), f2.get(SAMPLE_DATA_FIELD));
                    assertEquals(variantId + " alts fid=" + fid1,
                            f1.get(ALTERNATES_FIELD), f2.get(ALTERNATES_FIELD));
                    assertEquals(variantId + " ori fid=" + fid1,
                            f1.get(ORI_FIELD), f2.get(ORI_FIELD));

                    // mgt: compare with remapped sample IDs
                    assertMgtEquals(variantId, fid1,
                            f1.get(FILE_GENOTYPE_FIELD, Document.class),
                            f2.get(FILE_GENOTYPE_FIELD, Document.class),
                            sampleId1To2);

                    // sfd: compare with remapped sample IDs
                    assertSfdEquals(variantId, fid1,
                            f1.get(SAMPLE_FILTERABLE_DATA_FIELD, Document.class),
                            f2.get(SAMPLE_FILTERABLE_DATA_FIELD, Document.class),
                            sampleId1To2);

                    filesCompared++;
                }

                // Every study2 file should have been matched and removed
                assertTrue("Unmatched reference study files at " + variantId + ": " + study2ByFid.keySet(),
                        study2ByFid.isEmpty());

                variantsChecked++;
            }
        }

        assertTrue("No variants checked", variantsChecked > 0);
        assertTrue("No files compared", filesCompared > 0);
    }

    private static void assertMgtEquals(String variantId, int fid,
                                        Document mgt1, Document mgt2,
                                        Map<Integer, Integer> sampleId1To2) {
        if ((mgt1 == null || mgt1.isEmpty()) && (mgt2 == null || mgt2.isEmpty())) {
            return;
        }
        assertNotNull(variantId + " mgt null in aggregated study fid=" + fid, mgt1);
        assertNotNull(variantId + " mgt null in reference study fid=" + fid, mgt2);
        assertEquals(variantId + " mgt genotype keys fid=" + fid, mgt1.keySet(), mgt2.keySet());

        for (String gt : mgt1.keySet()) {
            List<Integer> ids1 = mgt1.getList(gt, Integer.class);
            List<Integer> ids2 = mgt2.getList(gt, Integer.class);
            // Remap study1 sample IDs to study2 and compare as sorted sets
            Set<Integer> remapped1 = new TreeSet<>();
            for (int id : ids1) {
                remapped1.add(sampleId1To2.get(id));
            }
            assertEquals(variantId + " mgt[" + gt + "] fid=" + fid,
                    remapped1, new TreeSet<>(ids2));
        }
    }

    private static void assertSfdEquals(String variantId, int fid,
                                        Document sfd1, Document sfd2,
                                        Map<Integer, Integer> sampleId1To2) {
        if ((sfd1 == null || sfd1.isEmpty()) && (sfd2 == null || sfd2.isEmpty())) {
            return;
        }
        assertNotNull(variantId + " sfd null in aggregated study fid=" + fid, sfd1);
        assertNotNull(variantId + " sfd null in reference study fid=" + fid, sfd2);
        assertEquals(variantId + " sfd field keys fid=" + fid, sfd1.keySet(), sfd2.keySet());

        for (String field : sfd1.keySet()) {
            Document fieldSfd1 = sfd1.get(field, Document.class);
            Document fieldSfd2 = sfd2.get(field, Document.class);
            // Remap sample IDs in study1 sfd and compare with study2
            Map<String, Object> remapped = new TreeMap<>();
            for (String sIdStr : fieldSfd1.keySet()) {
                int mapped = sampleId1To2.get(Integer.parseInt(sIdStr));
                remapped.put(String.valueOf(mapped), fieldSfd1.get(sIdStr));
            }
            assertEquals(variantId + " sfd[" + field + "] fid=" + fid,
                    remapped, new TreeMap<>(fieldSfd2));
        }
    }
}
