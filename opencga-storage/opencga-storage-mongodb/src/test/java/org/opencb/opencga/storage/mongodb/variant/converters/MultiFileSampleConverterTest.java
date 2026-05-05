package org.opencb.opencga.storage.mongodb.variant.converters;

import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.IssueEntry;
import org.opencb.biodata.models.variant.avro.IssueType;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.query.ResourceId;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.protobuf.VariantMongoDBProto;
import org.bson.types.Binary;

import java.util.*;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.*;

/**
 * Unit tests for the two-way document conversion of multi-file sample data.
 * <p>
 * Write path: {@link StudyEntryToDocumentConverter} + {@link SampleToDocumentConverter}
 * Read path:  {@link DocumentToSamplesConverter}
 */
@Category(ShortTests.class)
public class MultiFileSampleConverterTest {

    private static final String STUDY_NAME = "testStudy";
    private static final int STUDY_ID = 1;

    // Samples and files
    private static final String SAMPLE_A = "sampleA";
    private static final String SAMPLE_B = "sampleB";
    private static final String SAMPLE_C = "sampleC"; // multi-file
    private static final List<String> SAMPLES_FILE1 = Arrays.asList(SAMPLE_A, SAMPLE_C);
    private static final List<String> SAMPLES_FILE2 = Arrays.asList(SAMPLE_B, SAMPLE_C);

    private VariantStorageMetadataManager metadataManager;
    private StudyMetadata studyMetadata;
    private int fileId1;
    private int fileId2;
    private int sampleIdA;
    private int sampleIdB;
    private int sampleIdC;
    private Map<String, Integer> sampleIdsMap;

    @Before
    public void setUp() throws StorageEngineException {
        DummyVariantStorageEngine.clear();
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());

        metadataManager.createStudy(STUDY_NAME);
        metadataManager.updateStudyMetadata(STUDY_ID, sm -> {
            sm.getAttributes().put(MongoDBVariantStorageOptions.DEFAULT_GENOTYPE.key(), "0/0");
        });

        // Register file 1 with sampleA, sampleC
        fileId1 = metadataManager.registerFile(STUDY_ID, "file1.vcf", SAMPLES_FILE1);
        // Register file 2 with sampleB, sampleC
        fileId2 = metadataManager.registerFile(STUDY_ID, "file2.vcf", SAMPLES_FILE2);

        sampleIdA = metadataManager.getSampleId(STUDY_ID, SAMPLE_A);
        sampleIdB = metadataManager.getSampleId(STUDY_ID, SAMPLE_B);
        sampleIdC = metadataManager.getSampleId(STUDY_ID, SAMPLE_C);

        // Mark sampleC as MULTI split-data
        metadataManager.updateSampleMetadata(STUDY_ID, sampleIdC, sm -> sm.setSplitData(VariantStorageEngine.SplitData.MULTI));

        sampleIdsMap = new LinkedHashMap<>();
        sampleIdsMap.put(SAMPLE_A, sampleIdA);
        sampleIdsMap.put(SAMPLE_B, sampleIdB);
        sampleIdsMap.put(SAMPLE_C, sampleIdC);

        studyMetadata = metadataManager.getStudyMetadata(STUDY_ID);
        metadataManager.addIndexedFiles(STUDY_ID, Arrays.asList(fileId1, fileId2));
    }

    // ---------------------- Write path tests ----------------------

    /**
     * Verify that SampleToDocumentConverter stores per-file genotypes in the {@code mgt} field
     * of the file document for multi-file samples.
     */
    @Test
    public void testWritePath_mgtFieldOnFileDocument() throws Exception {
        SampleToDocumentConverter samplesConverter = new SampleToDocumentConverter(studyMetadata, sampleIdsMap);

        // Build a StudyEntry as if loading file1 (sampleA=0/0=default, sampleC=0/1)
        StudyEntry studyEntry = buildStudyEntry(fileId1, SAMPLES_FILE1, Arrays.asList("0/0", "0/1"));
        StudyEntryToDocumentConverter studyConverter = new StudyEntryToDocumentConverter(samplesConverter, false);

        Variant variant = new Variant("1:100:A:T");
        Pair<Document, List<Document>> pair = studyConverter.convertToStorageType(variant, studyEntry,
                studyEntry.getFile(String.valueOf(fileId1)),
                new LinkedHashSet<>(SAMPLES_FILE1));
        Document studyDoc = pair.getKey();

        // No study-level GENOTYPES_FIELD: GT lives exclusively in files[].mgt
        assertNull("Study-level gt field should not be written", studyDoc.get("gt"));
        assertNull("Study-level gt field should not be written", studyDoc.get(FILES_FIELD));

        // File document should have an mgt field with non-default GTs
        List<Document> files = pair.getValue();
        assertEquals(1, files.size());
        Document fileDoc = files.get(0);
        Document mgt = fileDoc.get(FILE_GENOTYPE_FIELD, Document.class);
        assertNotNull("File document should have an mgt field", mgt);
        List<Integer> mgtEntry = mgt.get(DocumentToSamplesConverter.genotypeToStorageType("0/1"), List.class);
        assertNotNull("mgt should have a 0/1 bucket", mgtEntry);
        assertTrue("sampleC should be in the mgt 0/1 bucket", mgtEntry.contains(sampleIdC));

        // sampleA has default GT (0/0) → should NOT appear in mgt
        for (Object val : mgt.values()) {
            assertFalse("sampleA (default GT=0/0) should not appear in mgt", ((List<?>) val).contains(sampleIdA));
        }
    }

    /**
     * Verify that mgt is written for all non-default GTs regardless of whether a sample is multi-file.
     * In Stage 2, mgt is the only GT store and is written for every sample with a non-default GT.
     */
    @Test
    public void testWritePath_mgtWrittenForAllNonDefaultGTs() throws Exception {
        SampleToDocumentConverter samplesConverter = new SampleToDocumentConverter(studyMetadata, sampleIdsMap);
        StudyEntryToDocumentConverter studyConverter = new StudyEntryToDocumentConverter(samplesConverter, false);

        // sampleA=0/0 (default), sampleC=0/1 (non-default)
        StudyEntry studyEntry = buildStudyEntry(fileId1, SAMPLES_FILE1, Arrays.asList("0/0", "0/1"));
        Variant variant = new Variant("1:100:A:T");
        Pair<Document, List<Document>> pair = studyConverter.convertToStorageType(variant, studyEntry,
                studyEntry.getFile(String.valueOf(fileId1)),
                new LinkedHashSet<>(SAMPLES_FILE1));
        List<Document> files = pair.getValue();

        assertEquals(1, files.size());
        Document fileDoc = files.get(0);
        Document mgt = fileDoc.get(FILE_GENOTYPE_FIELD, Document.class);
        assertNotNull("mgt field must be written for non-default GTs", mgt);
        List<Integer> bucket = mgt.get(DocumentToSamplesConverter.genotypeToStorageType("0/1"), List.class);
        assertNotNull("0/1 bucket should exist", bucket);
        assertTrue("sampleC (0/1) should be in mgt", bucket.contains(sampleIdC));
        for (Object val : mgt.values()) {
            assertFalse("sampleA (0/0=default) should not be in mgt", ((List<?>) val).contains(sampleIdA));
        }
    }

    // ---------------------- Read path tests ----------------------

    /**
     * Full round-trip: write both files, then read back and verify:
     * <ul>
     *   <li>Primary SampleEntry for sampleC carries the most significant genotype (MAIN_ALT wins)</li>
     *   <li>An IssueEntry(DISCREPANCY) is created for the secondary file's genotype</li>
     * </ul>
     */
    @Test
    public void testReadPath_issueEntryCreatedForSecondaryFile() throws Exception {
        SampleToDocumentConverter samplesConverter = new SampleToDocumentConverter(studyMetadata, sampleIdsMap);
        StudyEntryToDocumentConverter studyConverter = new StudyEntryToDocumentConverter(samplesConverter, false);
        Variant variant = new Variant("1:100:A:T");

        // File 1: sampleA=0/0 (default, not in mgt), sampleC=0/1 → file1.mgt={"0/1": [sampleIdC]}
        StudyEntry studyEntry1 = buildStudyEntry(fileId1, SAMPLES_FILE1, Arrays.asList("0/0", "0/1"));
        Document file1Doc = getFileDocument(studyConverter, variant, studyEntry1, fileId1, SAMPLES_FILE1);

        // File 2: sampleB=1/1, sampleC=1/1 → file2.mgt={"1/1": [sampleIdB, sampleIdC]}
        // sampleC=1/1 (non-default) so it IS stored in mgt, allowing the DISCREPANCY to be detected.
        StudyEntry studyEntry2 = buildStudyEntry(fileId2, SAMPLES_FILE2, Arrays.asList("1/1", "1/1"));
        Document file2Doc = getFileDocument(studyConverter, variant, studyEntry2, fileId2, SAMPLES_FILE2);

        List<Document> filesDoc = Arrays.asList(file1Doc, file2Doc);

        // Build a VariantQueryProjection returning all three samples and both files
        VariantQueryProjection projection = buildProjection(
                Arrays.asList(sampleIdA, sampleIdB, sampleIdC),
                Arrays.asList(fileId1, fileId2),
                Collections.singleton(sampleIdC));

        DocumentToSamplesConverter readConverter = new DocumentToSamplesConverter(metadataManager, projection);
        StudyEntry result = new StudyEntry(STUDY_NAME);
        readConverter.convertToDataModelType(filesDoc, result, STUDY_ID);

        // Primary samples: A, B, C (one entry each)
        assertEquals(3, result.getSamples().size());

        // sampleC's primary GT should be 0/1 (MAIN_ALT wins over 1/1, from file1)
        SampleEntry sampleCEntry = result.getSamples().get(result.getSamplesPosition().get(SAMPLE_C));
        assertEquals("sampleC primary GT should be 0/1 (MAIN_ALT)", "0/1", sampleCEntry.getData().get(0));

        // There must be exactly one IssueEntry for sampleC (from file2 with GT 1/1)
        List<IssueEntry> issues = result.getIssues();
        assertNotNull("Issues list should not be null", issues);
        assertEquals("Exactly one IssueEntry expected", 1, issues.size());

        IssueEntry issue = issues.get(0);
        assertEquals(IssueType.DISCREPANCY, issue.getType());
        assertEquals(SAMPLE_C, issue.getSample().getSampleId());
        assertEquals("1/1", issue.getSample().getData().get(0));
    }

    /**
     * Verify that IssueEntry for a secondary file carries the extra FORMAT fields (e.g. DP)
     * from the secondary file's sampleData, and that the primary entry's extra fields come
     * from the primary file.
     */
    @Test
    public void testReadPath_issueEntryExtraFormatFields() throws Exception {
        // Register DP as an extra format field
        metadataManager.updateStudyMetadata(STUDY_ID, sm -> {
            sm.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "DP");
            sm.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), "String");
        });
        studyMetadata = metadataManager.getStudyMetadata(STUDY_ID);

        Variant variant = new Variant("1:100:A:T");

        // file1: sampleA DP=10, sampleC DP=20
        Binary dpFile1 = new Binary(VariantMongoDBProto.OtherFields.newBuilder()
                .addStringValues("10").addStringValues("20").build().toByteArray());
        // file2: sampleB DP=30, sampleC DP=40
        Binary dpFile2 = new Binary(VariantMongoDBProto.OtherFields.newBuilder()
                .addStringValues("30").addStringValues("40").build().toByteArray());

        Document file1Doc = new Document(FILEID_FIELD, fileId1)
                .append(SAMPLE_DATA_FIELD, new Document("dp", dpFile1));
        Document file2Doc = new Document(FILEID_FIELD, fileId2)
                .append(SAMPLE_DATA_FIELD, new Document("dp", dpFile2));

        // mgt on file1: sampleC=0/1; mgt on file2: sampleC=1/1 (must be non-default so it is stored)
        Document mgt1 = new Document(DocumentToSamplesConverter.genotypeToStorageType("0/1"),
                Collections.singletonList(sampleIdC));
        file1Doc.append(FILE_GENOTYPE_FIELD, mgt1);
        Document mgt2 = new Document(DocumentToSamplesConverter.genotypeToStorageType("1/1"),
                Collections.singletonList(sampleIdC));
        file2Doc.append(FILE_GENOTYPE_FIELD, mgt2);

        List<Document> filesDoc = Arrays.asList(file1Doc, file2Doc);

        VariantQueryProjection projection = buildProjection(
                Arrays.asList(sampleIdA, sampleIdB, sampleIdC),
                Arrays.asList(fileId1, fileId2),
                Collections.singleton(sampleIdC));

        DocumentToSamplesConverter readConverter = new DocumentToSamplesConverter(metadataManager, projection);
        StudyEntry result = new StudyEntry(STUDY_NAME);
        readConverter.convertToDataModelType(filesDoc, result, STUDY_ID);

        // sampleC primary: GT=0/1 (MAIN_ALT from file1.mgt), DP=20 (from file1)
        SampleEntry sampleCEntry = result.getSamples().get(result.getSamplesPosition().get(SAMPLE_C));
        assertEquals("0/1", sampleCEntry.getData().get(0));
        assertEquals("sampleC primary DP should come from file1", "20", sampleCEntry.getData().get(1));

        // IssueEntry: GT=1/1 (from file2), DP=40 (from file2's sampleData)
        List<IssueEntry> issues = result.getIssues();
        assertEquals(1, issues.size());
        IssueEntry issue = issues.get(0);
        assertEquals("1/1", issue.getSample().getData().get(0));
        assertEquals("IssueEntry DP should come from file2", "40", issue.getSample().getData().get(1));
    }

    /**
     * Verify that when a variant appears in only one file for a multi-file sample,
     * no IssueEntry is created.
     */
    @Test
    public void testReadPath_noIssueEntryWhenOnlyOneFilePresent() throws Exception {
        SampleToDocumentConverter samplesConverter = new SampleToDocumentConverter(studyMetadata, sampleIdsMap);
        StudyEntryToDocumentConverter studyConverter = new StudyEntryToDocumentConverter(samplesConverter, false);
        Variant variant = new Variant("1:200:C:G");

        // Only file1 is present for this variant (sampleC appears only in file1)
        StudyEntry studyEntry1 = buildStudyEntry(fileId1, SAMPLES_FILE1, Arrays.asList("0/1", "0/1"));
        Document file1Doc = getFileDocument(studyConverter, variant, studyEntry1, fileId1, SAMPLES_FILE1);

        List<Document> filesDoc = Collections.singletonList(file1Doc);

        VariantQueryProjection projection = buildProjection(
                Arrays.asList(sampleIdA, sampleIdC),
                Collections.singletonList(fileId1),
                Collections.singleton(sampleIdC));

        DocumentToSamplesConverter readConverter = new DocumentToSamplesConverter(metadataManager, projection);
        StudyEntry result = new StudyEntry(STUDY_NAME);
        readConverter.convertToDataModelType(filesDoc, result, STUDY_ID);

        List<IssueEntry> issues = result.getIssues();
        assertTrue("No IssueEntry when sampleC appears in only one file",
                issues == null || issues.isEmpty());
    }

    /**
     * When one of the variant's file documents has no {@code mgt} field (e.g. it was indexed before
     * Stage 2 or the sample's GT was the default), the read path must NOT create an IssueEntry for
     * the missing file — only files that explicitly carry mgt data for sampleC participate in
     * DISCREPANCY detection.
     *
     * <p>Layout:
     * <ul>
     *   <li>file1 (has mgt): sampleC=0/1 — stored in file1.mgt</li>
     *   <li>file2 (no mgt): no GT data for sampleC in this file document</li>
     * </ul>
     * Expected: sampleC primary GT = 0/1 (from file1.mgt), no IssueEntry.
     */
    @Test
    public void testReadPath_noIssueEntryWhenFileHasNoMgt() throws Exception {
        SampleToDocumentConverter samplesConverter = new SampleToDocumentConverter(studyMetadata, sampleIdsMap);
        StudyEntryToDocumentConverter studyConverter = new StudyEntryToDocumentConverter(samplesConverter, false);
        Variant variant = new Variant("1:300:G:C");

        // file1 has mgt with sampleC=0/1
        StudyEntry studyEntry1 = buildStudyEntry(fileId1, SAMPLES_FILE1, Arrays.asList("0/0", "0/1"));
        Document file1Doc = getFileDocument(studyConverter, variant, studyEntry1, fileId1, SAMPLES_FILE1);

        // file2 has NO mgt field at all
        Document file2Doc = new Document(FILEID_FIELD, fileId2);

        List<Document> filesDoc = Arrays.asList(file1Doc, file2Doc);

        VariantQueryProjection projection = buildProjection(
                Arrays.asList(sampleIdA, sampleIdB, sampleIdC),
                Arrays.asList(fileId1, fileId2),
                Collections.singleton(sampleIdC));

        DocumentToSamplesConverter readConverter = new DocumentToSamplesConverter(metadataManager, projection);
        StudyEntry result = new StudyEntry(STUDY_NAME);
        readConverter.convertToDataModelType(filesDoc, result, STUDY_ID);

        // sampleC primary GT = 0/1 (from file1.mgt — the only mgt source)
        SampleEntry sampleCEntry = result.getSamples().get(result.getSamplesPosition().get(SAMPLE_C));
        assertEquals("sampleC primary GT should be 0/1 (from file1.mgt)", "0/1", sampleCEntry.getData().get(0));

        // No IssueEntry: file2 has no mgt, so no discrepancy can be detected
        List<IssueEntry> issues = result.getIssues();
        assertTrue("No IssueEntry when secondary file has no mgt", issues == null || issues.isEmpty());
    }

    /**
     * Two files for the same variant carry different secondary alternates and per-allele AC values:
     * <ul>
     *   <li>file3: 1:1000:A:T,C — sample1 GT=1/2, AC=1,2 (T/C)</li>
     *   <li>file4: 1:1000:A:T,G — sample2 GT=1/2, AC=3,4 (T/G)</li>
     * </ul>
     * Expected merged result: secondary alternates=[C, G];
     * sample1 GT=1/2 (unchanged), AC=1,2,0 (pad 0 for absent alt G);
     * sample2 GT=1/3 (G remapped from index 2 to 3 in the merged allele list [A,T,C,G]),
     * AC=3,0,4 (pad 0 for absent alt C, remap G value to position 3).
     */
    @Test
    public void testReadPath_secondaryAlternatesMerge() throws Exception {
        // Register two single-sample files with non-overlapping samples
        String s1 = "alt_sample1";
        String s2 = "alt_sample2";
        int fid3 = metadataManager.registerFile(STUDY_ID, "file3.vcf", Collections.singletonList(s1));
        int fid4 = metadataManager.registerFile(STUDY_ID, "file4.vcf", Collections.singletonList(s2));
        metadataManager.addIndexedFiles(STUDY_ID, Arrays.asList(fid3, fid4));
        int sid1 = metadataManager.getSampleId(STUDY_ID, s1);
        int sid2 = metadataManager.getSampleId(STUDY_ID, s2);

        // Register AC as an extra format field (String type for comma-separated per-allele values)
        metadataManager.updateStudyMetadata(STUDY_ID, sm -> {
            sm.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "AC");
            sm.getAttributes().put(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key(), "String");
        });
        // Refresh studyMetadata after adding new files and attributes
        studyMetadata = metadataManager.getStudyMetadata(STUDY_ID);

        Variant variant = new Variant("1:1000:A:T");

        // Encode AC values as protobuf sampleData blobs (one sample per file)
        Binary acFile3 = new Binary(VariantMongoDBProto.OtherFields.newBuilder().addStringValues("1,2").build().toByteArray());
        Binary acFile4 = new Binary(VariantMongoDBProto.OtherFields.newBuilder().addStringValues("3,4").build().toByteArray());

        // file3 document: secondary alt = C (alleles: A=0, T=1, C=2 → sample1 GT 1/2 = T/C)
        Document altC = new Document(ALTERNATES_ALT, "C").append(ALTERNATES_TYPE, "SNV");
        Document file3Doc = new Document(FILEID_FIELD, fid3)
                .append(STUDYID_FIELD, STUDY_ID)
                .append(ALTERNATES_FIELD, Collections.singletonList(altC))
                .append(SAMPLE_DATA_FIELD, new Document("ac", acFile3))
                .append(FILE_GENOTYPE_FIELD, new Document(DocumentToSamplesConverter.genotypeToStorageType("1/2"),
                        Collections.singletonList(sid1)));

        // file4 document: secondary alt = G (alleles: A=0, T=1, G=2 → sample2 GT 1/2 = T/G)
        Document altG = new Document(ALTERNATES_ALT, "G").append(ALTERNATES_TYPE, "SNV");
        Document file4Doc = new Document(FILEID_FIELD, fid4)
                .append(STUDYID_FIELD, STUDY_ID)
                .append(ALTERNATES_FIELD, Collections.singletonList(altG))
                .append(SAMPLE_DATA_FIELD, new Document("ac", acFile4))
                .append(FILE_GENOTYPE_FIELD, new Document(DocumentToSamplesConverter.genotypeToStorageType("1/2"),
                        Collections.singletonList(sid2)));

        // Both samples have GT "1/2" stored in their respective file's mgt field
        List<Document> files = Arrays.asList(file3Doc, file4Doc);
        Document studyDoc = new Document(STUDYID_FIELD, STUDY_ID);

        VariantQueryProjection projection = buildProjection(
                Arrays.asList(sid1, sid2), Arrays.asList(fid3, fid4), Collections.emptySet());

        DocumentToSamplesConverter samplesConv = new DocumentToSamplesConverter(metadataManager, projection);
        DocumentToStudyEntryConverter studyConv = new DocumentToStudyEntryConverter(false,
                Collections.singletonMap(STUDY_ID, Arrays.asList(fid3, fid4)), samplesConv);
        studyConv.setMetadataManager(metadataManager);
        studyConv.addStudyName(STUDY_ID, STUDY_NAME);

        StudyEntry result = studyConv.convertToDataModelType(studyDoc, files, variant);

        // Secondary alternates: [C, G] in that insertion order
        List<AlternateCoordinate> alts = result.getSecondaryAlternates();
        assertNotNull("Secondary alternates should not be null", alts);
        assertEquals("Should have 2 secondary alternates", 2, alts.size());
        assertEquals("First secondary alt should be C", "C", alts.get(0).getAlternate());
        assertEquals("Second secondary alt should be G", "G", alts.get(1).getAlternate());

        // sample1 GT = 1/2: C is still at allele index 2 in the merged list [A, T, C, G]
        SampleEntry s1Entry = result.getSamples().get(result.getSamplesPosition().get(s1));
        assertNotNull("sample1 should have a SampleEntry", s1Entry);
        assertEquals("sample1 GT should be 1/2 (T/C, unchanged)", "1/2", s1Entry.getData().get(0));
        // sample1 AC: 1 for T, 2 for C, 0 for absent G → "1,2,0"
        assertEquals("sample1 AC should be 1,2,0", "1,2,0", s1Entry.getData().get(1));

        // sample2 GT = 1/3: G moved from index 2 (in file4's list) to index 3 (in merged list)
        SampleEntry s2Entry = result.getSamples().get(result.getSamplesPosition().get(s2));
        assertNotNull("sample2 should have a SampleEntry", s2Entry);
        assertEquals("sample2 GT should be 1/3 (T/G, remapped)", "1/3", s2Entry.getData().get(0));
        // sample2 AC: 3 for T, 0 for absent C, 4 for G → "3,0,4"
        assertEquals("sample2 AC should be 3,0,4", "3,0,4", s2Entry.getData().get(1));
    }

    // ---------------------- Helpers ----------------------

    private StudyEntry buildStudyEntry(int fileId, List<String> sampleNames, List<String> genotypes) {
        StudyEntry studyEntry = new StudyEntry(String.valueOf(STUDY_ID));
        studyEntry.setSampleDataKeys(Collections.singletonList("GT"));

        LinkedHashMap<String, Integer> position = new LinkedHashMap<>();
        for (int i = 0; i < sampleNames.size(); i++) {
            position.put(sampleNames.get(i), i);
        }
        studyEntry.setSamplesPosition(position);

        for (int i = 0; i < sampleNames.size(); i++) {
            studyEntry.addSampleData(sampleNames.get(i), "GT", genotypes.get(i));
        }
        studyEntry.getSamples().forEach(s -> s.setFileIndex(0));

        FileEntry fileEntry = new FileEntry(String.valueOf(fileId), null, new HashMap<>());
        studyEntry.setFiles(Collections.singletonList(fileEntry));
        return studyEntry;
    }

    private Document getFileDocument(StudyEntryToDocumentConverter studyConverter, Variant variant,
                                     StudyEntry studyEntry, int fileId, List<String> sampleNames) {
        return studyConverter.convertToStorageType(variant, studyEntry,
                studyEntry.getFile(String.valueOf(fileId)),
                new LinkedHashSet<>(sampleNames)).getValue().get(0);
    }

    private VariantQueryProjection buildProjection(List<Integer> sampleIds, List<Integer> fileIds,
                                                   Set<Integer> multiFileSamples) {
        List<ResourceId> sampleResources = new ArrayList<>();
        for (Integer sampleId : sampleIds) {
            sampleResources.add(new ResourceId(ResourceId.Type.SAMPLE, sampleId,
                    metadataManager.getSampleName(STUDY_ID, sampleId)));
        }
        List<ResourceId> fileResources = new ArrayList<>();
        for (Integer fileId : fileIds) {
            fileResources.add(new ResourceId(ResourceId.Type.FILE, fileId,
                    metadataManager.getFileName(STUDY_ID, fileId)));
        }

        VariantQueryProjection.StudyVariantQueryProjection studyProjection = new VariantQueryProjection.StudyVariantQueryProjection()
                .setStudyMetadata(studyMetadata)
                .setSamples(sampleResources)
                .setFiles(fileResources)
                .setMultiFileSamples(multiFileSamples);

        Map<Integer, VariantQueryProjection.StudyVariantQueryProjection> studies =
                Collections.singletonMap(STUDY_ID, studyProjection);
        return new VariantQueryProjection(
                org.opencb.opencga.storage.core.variant.adaptors.VariantField.getIncludeFields(null),
                studies, false, sampleIds.size(), sampleIds.size());
    }
}
