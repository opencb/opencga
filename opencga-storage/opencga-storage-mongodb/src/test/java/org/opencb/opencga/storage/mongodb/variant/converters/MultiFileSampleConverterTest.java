package org.opencb.opencga.storage.mongodb.variant.converters;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
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
import org.opencb.opencga.storage.core.variant.query.ResourceId;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;

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
        Set<Integer> multiFileSampleIds = Collections.singleton(sampleIdC);
        SampleToDocumentConverter samplesConverter = new SampleToDocumentConverter(studyMetadata, sampleIdsMap, multiFileSampleIds);

        // Build a StudyEntry as if loading file1 (sampleA=0/0, sampleC=0/1)
        StudyEntry studyEntry = buildStudyEntry(fileId1, SAMPLES_FILE1, Arrays.asList("0/0", "0/1"));
        StudyEntryToDocumentConverter studyConverter = new StudyEntryToDocumentConverter(samplesConverter, false);

        Variant variant = new Variant("1:100:A:T");
        Document studyDoc = studyConverter.convertToStorageType(variant, studyEntry,
                studyEntry.getFile(String.valueOf(fileId1)),
                new LinkedHashSet<>(SAMPLES_FILE1));

        // Study-level gt should contain sampleA (non-default: 0/0 is default) and sampleC (0/1)
        Document gt = studyDoc.get(GENOTYPES_FIELD, Document.class);
        assertNotNull("GENOTYPES_FIELD should be present", gt);
        // sampleC has genotype 0/1 → must appear in gt
        List<Integer> gtEntry = gt.get(DocumentToSamplesConverter.genotypeToStorageType("0/1"), List.class);
        assertNotNull("0/1 bucket should exist in gt", gtEntry);
        assertTrue("sampleC should be in the 0/1 gt bucket", gtEntry.contains(sampleIdC));

        // File document should have an mgt field for sampleC
        List<Document> files = studyDoc.get(FILES_FIELD, List.class);
        assertEquals(1, files.size());
        Document fileDoc = files.get(0);
        Document mgt = fileDoc.get(MULTI_FILE_GENOTYPE_FIELD, Document.class);
        assertNotNull("File document should have an mgt field for multi-file samples", mgt);
        List<Integer> mgtEntry = mgt.get(DocumentToSamplesConverter.genotypeToStorageType("0/1"), List.class);
        assertNotNull("mgt should have a 0/1 bucket", mgtEntry);
        assertTrue("sampleC should be in the mgt 0/1 bucket", mgtEntry.contains(sampleIdC));

        // sampleA (non-multi-file) should NOT appear in mgt
        for (Object val : mgt.values()) {
            assertFalse("sampleA (non-multi) should not appear in mgt", ((List<?>) val).contains(sampleIdA));
        }
    }

    /**
     * Verify that when a single-file sample is in the study entry, no {@code mgt} field is written.
     */
    @Test
    public void testWritePath_noMgtForSingleFileSample() throws Exception {
        // No multi-file samples
        SampleToDocumentConverter samplesConverter = new SampleToDocumentConverter(studyMetadata, sampleIdsMap);
        StudyEntryToDocumentConverter studyConverter = new StudyEntryToDocumentConverter(samplesConverter, false);

        StudyEntry studyEntry = buildStudyEntry(fileId1, SAMPLES_FILE1, Arrays.asList("0/0", "0/1"));
        Variant variant = new Variant("1:100:A:T");
        Document studyDoc = studyConverter.convertToStorageType(variant, studyEntry,
                studyEntry.getFile(String.valueOf(fileId1)),
                new LinkedHashSet<>(SAMPLES_FILE1));

        List<Document> files = studyDoc.get(FILES_FIELD, List.class);
        assertEquals(1, files.size());
        Document fileDoc = files.get(0);
        assertNull("No mgt field when no multi-file samples are configured",
                fileDoc.get(MULTI_FILE_GENOTYPE_FIELD));
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
        Set<Integer> multiFileSampleIds = Collections.singleton(sampleIdC);
        SampleToDocumentConverter samplesConverter = new SampleToDocumentConverter(studyMetadata, sampleIdsMap, multiFileSampleIds);
        StudyEntryToDocumentConverter studyConverter = new StudyEntryToDocumentConverter(samplesConverter, false);
        Variant variant = new Variant("1:100:A:T");

        // File 1: sampleA=0/0 (default, stored only if non-default), sampleC=0/1
        StudyEntry studyEntry1 = buildStudyEntry(fileId1, SAMPLES_FILE1, Arrays.asList("0/0", "0/1"));
        Document file1Doc = getFileDocument(studyConverter, variant, studyEntry1, fileId1, SAMPLES_FILE1);

        // File 2: sampleB=1/1, sampleC=0/0
        StudyEntry studyEntry2 = buildStudyEntry(fileId2, SAMPLES_FILE2, Arrays.asList("1/1", "0/0"));
        Document file2Doc = getFileDocument(studyConverter, variant, studyEntry2, fileId2, SAMPLES_FILE2);

        // Build the MongoDB study document as it would be stored in the DB.
        // Study-level gt: sampleA is default (0/0, not stored), sampleC=0/1, sampleB=1/1
        // (sampleC's gt from file2 is not stored in the study-level gt, per design)
        Document gts = new Document()
                .append(DocumentToSamplesConverter.genotypeToStorageType("0/1"), Collections.singletonList(sampleIdC))
                .append(DocumentToSamplesConverter.genotypeToStorageType("1/1"), Collections.singletonList(sampleIdB));
        Document studyDoc = new Document(STUDYID_FIELD, STUDY_ID)
                .append(FILES_FIELD, Arrays.asList(file1Doc, file2Doc))
                .append(GENOTYPES_FIELD, gts);

        // Build a VariantQueryProjection returning all three samples and both files
        VariantQueryProjection projection = buildProjection(
                Arrays.asList(sampleIdA, sampleIdB, sampleIdC),
                Arrays.asList(fileId1, fileId2),
                Collections.singleton(sampleIdC));

        DocumentToSamplesConverter readConverter = new DocumentToSamplesConverter(metadataManager, projection);
        StudyEntry result = new StudyEntry(STUDY_NAME);
        readConverter.convertToDataModelType(studyDoc, result, STUDY_ID);

        // Primary samples: A, B, C (one entry each)
        assertEquals(3, result.getSamples().size());

        // sampleC's primary GT should be 0/1 (MAIN_ALT, from file1)
        SampleEntry sampleCEntry = result.getSamples().get(result.getSamplesPosition().get(SAMPLE_C));
        assertEquals("sampleC primary GT should be 0/1 (MAIN_ALT)", "0/1", sampleCEntry.getData().get(0));

        // There must be exactly one IssueEntry for sampleC (from file2 with GT 0/0)
        List<IssueEntry> issues = result.getIssues();
        assertNotNull("Issues list should not be null", issues);
        assertEquals("Exactly one IssueEntry expected", 1, issues.size());

        IssueEntry issue = issues.get(0);
        assertEquals(IssueType.DISCREPANCY, issue.getType());
        assertEquals(SAMPLE_C, issue.getSample().getSampleId());
        assertEquals("0/0", issue.getSample().getData().get(0));
    }

    /**
     * Verify that when a variant appears in only one file for a multi-file sample,
     * no IssueEntry is created.
     */
    @Test
    public void testReadPath_noIssueEntryWhenOnlyOneFilePresent() throws Exception {
        Set<Integer> multiFileSampleIds = Collections.singleton(sampleIdC);
        SampleToDocumentConverter samplesConverter = new SampleToDocumentConverter(studyMetadata, sampleIdsMap, multiFileSampleIds);
        StudyEntryToDocumentConverter studyConverter = new StudyEntryToDocumentConverter(samplesConverter, false);
        Variant variant = new Variant("1:200:C:G");

        // Only file1 is present for this variant (sampleC appears only in file1)
        StudyEntry studyEntry1 = buildStudyEntry(fileId1, SAMPLES_FILE1, Arrays.asList("0/1", "0/1"));
        Document file1Doc = getFileDocument(studyConverter, variant, studyEntry1, fileId1, SAMPLES_FILE1);

        Document gts = new Document()
                .append(DocumentToSamplesConverter.genotypeToStorageType("0/1"),
                        Arrays.asList(sampleIdA, sampleIdC));
        Document studyDoc = new Document(STUDYID_FIELD, STUDY_ID)
                .append(FILES_FIELD, Collections.singletonList(file1Doc))
                .append(GENOTYPES_FIELD, gts);

        VariantQueryProjection projection = buildProjection(
                Arrays.asList(sampleIdA, sampleIdC),
                Collections.singletonList(fileId1),
                Collections.singleton(sampleIdC));

        DocumentToSamplesConverter readConverter = new DocumentToSamplesConverter(metadataManager, projection);
        StudyEntry result = new StudyEntry(STUDY_NAME);
        readConverter.convertToDataModelType(studyDoc, result, STUDY_ID);

        List<IssueEntry> issues = result.getIssues();
        assertTrue("No IssueEntry when sampleC appears in only one file",
                issues == null || issues.isEmpty());
    }

    /**
     * Regression test: when the first file was loaded without LOAD_SPLIT_DATA=MULTI (no mgt on its
     * file document), but the second file was loaded with the flag (has mgt), the read path must
     * still create an IssueEntry for the discrepancy.
     *
     * <p>Layout:
     * <ul>
     *   <li>file1 (no mgt): sampleC=0/1 — stored only in study-level gt</li>
     *   <li>file2 (has mgt): sampleC=0/0 — stored in file2.mgt</li>
     * </ul>
     * Expected: primary GT = 0/1 (MAIN_ALT wins), IssueEntry(GT=0/0) for file2.
     */
    @Test
    public void testReadPath_issueEntryWhenFirstFileHasNoMgt() throws Exception {
        Set<Integer> multiFileSampleIds = Collections.singleton(sampleIdC);
        SampleToDocumentConverter samplesConverter = new SampleToDocumentConverter(studyMetadata, sampleIdsMap, multiFileSampleIds);
        StudyEntryToDocumentConverter studyConverter = new StudyEntryToDocumentConverter(samplesConverter, false);
        Variant variant = new Variant("1:300:G:C");

        // file2 is the one loaded with MULTI flag — build its file document the normal way (will have mgt)
        StudyEntry studyEntry2 = buildStudyEntry(fileId2, SAMPLES_FILE2, Arrays.asList("1/1", "0/0"));
        Document file2Doc = getFileDocument(studyConverter, variant, studyEntry2, fileId2, SAMPLES_FILE2);

        // file1 was loaded WITHOUT the flag: build a plain file document with no mgt field
        Document file1Doc = new Document(DocumentToStudyEntryConverter.FILEID_FIELD, fileId1);

        // Study-level gt: sampleC=0/1 (written during file1 load, no filtering since MULTI wasn't set),
        //                 sampleB=1/1 (from file2)
        Document gts = new Document()
                .append(DocumentToSamplesConverter.genotypeToStorageType("0/1"), Collections.singletonList(sampleIdC))
                .append(DocumentToSamplesConverter.genotypeToStorageType("1/1"), Collections.singletonList(sampleIdB));
        Document studyDoc = new Document(DocumentToStudyEntryConverter.STUDYID_FIELD, STUDY_ID)
                .append(DocumentToStudyEntryConverter.FILES_FIELD, Arrays.asList(file1Doc, file2Doc))
                .append(DocumentToStudyEntryConverter.GENOTYPES_FIELD, gts);

        VariantQueryProjection projection = buildProjection(
                Arrays.asList(sampleIdA, sampleIdB, sampleIdC),
                Arrays.asList(fileId1, fileId2),
                Collections.singleton(sampleIdC));

        DocumentToSamplesConverter readConverter = new DocumentToSamplesConverter(metadataManager, projection);
        StudyEntry result = new StudyEntry(STUDY_NAME);
        readConverter.convertToDataModelType(studyDoc, result, STUDY_ID);

        // sampleC primary GT should be 0/1 (MAIN_ALT; came from study-level gt / file1)
        SampleEntry sampleCEntry = result.getSamples().get(result.getSamplesPosition().get(SAMPLE_C));
        assertEquals("sampleC primary GT should be 0/1 (MAIN_ALT)", "0/1", sampleCEntry.getData().get(0));

        // There must be exactly one IssueEntry for sampleC (from file2 with GT 0/0)
        List<IssueEntry> issues = result.getIssues();
        assertNotNull("Issues list should not be null", issues);
        assertEquals("Exactly one IssueEntry expected", 1, issues.size());

        IssueEntry issue = issues.get(0);
        assertEquals(IssueType.DISCREPANCY, issue.getType());
        assertEquals(SAMPLE_C, issue.getSample().getSampleId());
        assertEquals("0/0", issue.getSample().getData().get(0));
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
        Document studyDoc = studyConverter.convertToStorageType(variant, studyEntry,
                studyEntry.getFile(String.valueOf(fileId)),
                new LinkedHashSet<>(sampleNames));
        List<Document> files = studyDoc.get(FILES_FIELD, List.class);
        return files.get(0);
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
