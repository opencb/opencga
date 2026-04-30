package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchemaFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class LocalSampleIndexDBAdaptorTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private LocalSampleIndexDBAdaptor adaptor;
    private VariantStorageMetadataManager metadataManager;
    private int studyId;
    private int sampleId1;
    private int sampleId2;
    private SampleIndexSchema schema;

    @Before
    public void setUp() throws Exception {
        // Create temporary directory for test data
        Path tempDir = temporaryFolder.newFolder("sample_index").toPath();

        // Set up metadata manager
        DummyVariantStorageEngine.clear();
        metadataManager = new VariantStorageMetadataManager(
                new DummyVariantStorageMetadataDBAdaptorFactory());
        studyId = metadataManager.createStudy("TEST_STUDY").getId();
        metadataManager.registerFile(studyId, "file1.vcf", Arrays.asList("SAMPLE1", "SAMPLE2"));
        sampleId1 = metadataManager.getSampleId(studyId, "SAMPLE1");
        sampleId2 = metadataManager.getSampleId(studyId, "SAMPLE2");

        // Register a sample-index configuration on the study (version 1, ACTIVE)
        metadataManager.updateStudyMetadata(studyId, sm -> {
            sm.setSampleIndexConfigurations(Collections.singletonList(
                    new StudyMetadata.SampleIndexConfigurationVersioned(
                            SampleIndexConfiguration.defaultConfiguration(),
                            1,
                            new Date(),
                            StudyMetadata.SampleIndexConfigurationVersioned.Status.ACTIVE)));
        });
        // Mark each sample as having a ready sample-index (and annotation) at version 1
        for (int sampleId : Arrays.asList(sampleId1, sampleId2)) {
            metadataManager.updateSampleMetadata(studyId, sampleId, sm -> {
                sm.setSampleIndexStatus(TaskMetadata.Status.READY, 1);
                sm.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, 1);
            });
        }

        // Create schema factory
        SampleIndexSchemaFactory schemaFactory = new SampleIndexSchemaFactory(metadataManager);
        schema = schemaFactory.getSchemaForVersion(studyId, 1);

        // Create adaptor
        adaptor = new LocalSampleIndexDBAdaptor(metadataManager, tempDir);
    }

    @After
    public void tearDown() {
        // Cleanup is handled by TemporaryFolder rule
    }

    @Test
    public void testWriteAndReadEntry() throws Exception {
        // Create a sample entry
        SampleIndexEntry entry = new SampleIndexEntry(sampleId1, "1", 0);
        entry.getGtEntry("0/1");  // Add a genotype entry

        // Write entry
        adaptor.writeEntry(studyId, 1, entry);

        // Read entry back
        SampleIndexEntry readEntry = adaptor.readEntry(studyId, 1, sampleId1, "1", 0);

        // Verify
        assertNotNull(readEntry);
        assertEquals("1", readEntry.getChromosome());
        assertEquals(0, readEntry.getBatchStart());
        assertEquals(sampleId1, readEntry.getSampleId());
        assertTrue(readEntry.getGts().containsKey("0/1"));
    }

    @Test
    public void testReadNonExistentEntry() throws Exception {
        // Try to read entry that doesn't exist
        SampleIndexEntry entry = adaptor.readEntry(studyId, 1, sampleId1, "1", 0);

        // Should return null
        assertNull(entry);
    }

    @Test
    public void testWriteMultipleEntries() throws Exception {
        // Create entries for different regions
        SampleIndexEntry entry1 = new SampleIndexEntry(sampleId1, "1", 0);
        entry1.getGtEntry("0/1");

        SampleIndexEntry entry2 = new SampleIndexEntry(sampleId1, "1", 100000);
        entry2.getGtEntry("1/1");

        SampleIndexEntry entry3 = new SampleIndexEntry(sampleId1, "2", 0);
        entry3.getGtEntry("0/1");

        // Write entries
        adaptor.writeEntry(studyId, 1, entry1);
        adaptor.writeEntry(studyId, 1, entry2);
        adaptor.writeEntry(studyId, 1, entry3);

        // Read back and verify
        assertNotNull(adaptor.readEntry(studyId, 1, sampleId1, "1", 0));
        assertNotNull(adaptor.readEntry(studyId, 1, sampleId1, "1", 100000));
        assertNotNull(adaptor.readEntry(studyId, 1, sampleId1, "2", 0));
    }

    @Test
    public void testWriteMultipleSamples() throws Exception {
        // Create entries for different samples
        SampleIndexEntry entry1 = new SampleIndexEntry(sampleId1, "1", 0);
        entry1.getGtEntry("0/1");

        SampleIndexEntry entry2 = new SampleIndexEntry(sampleId2, "1", 0);
        entry2.getGtEntry("1/1");

        // Write entries
        adaptor.writeEntry(studyId, 1, entry1);
        adaptor.writeEntry(studyId, 1, entry2);

        // Read back and verify
        SampleIndexEntry read1 = adaptor.readEntry(studyId, 1, sampleId1, "1", 0);
        SampleIndexEntry read2 = adaptor.readEntry(studyId, 1, sampleId2, "1", 0);

        assertNotNull(read1);
        assertNotNull(read2);
        assertEquals(sampleId1, read1.getSampleId());
        assertEquals(sampleId2, read2.getSampleId());
    }

    @Test
    public void testOverwriteEntry() throws Exception {
        // Create and write initial entry
        SampleIndexEntry entry1 = new SampleIndexEntry(sampleId1, "1", 0);
        entry1.getGtEntry("0/1");
        adaptor.writeEntry(studyId, 1, entry1);

        // Overwrite with new entry
        SampleIndexEntry entry2 = new SampleIndexEntry(sampleId1, "1", 0);
        entry2.getGtEntry("1/1");
        adaptor.writeEntry(studyId, 1, entry2);

        // Read back
        SampleIndexEntry readEntry = adaptor.readEntry(studyId, 1, sampleId1, "1", 0);

        // Should have the new genotype
        assertNotNull(readEntry);
        assertTrue(readEntry.getGts().containsKey("1/1"));
        assertFalse(readEntry.getGts().containsKey("0/1"));
    }

    @Test
    public void testGetRegionBounds() throws Exception {
        // Create entries for different regions
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 0));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 100000));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "2", 0));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId2, "2", 100000));

        // Get region bounds
        List<Region> regions = adaptor.getRegionBounds(studyId, 1, Arrays.asList(sampleId1, sampleId2));

        // Verify we got all unique regions
        assertNotNull(regions);
        assertEquals(4, regions.size());

        // Check that regions are correct
        assertTrue(regions.stream().anyMatch(r -> r.getChromosome().equals("1") && r.getStart() == 0));
        assertTrue(regions.stream().anyMatch(r -> r.getChromosome().equals("1") && r.getStart() == 100000));
        assertTrue(regions.stream().anyMatch(r -> r.getChromosome().equals("2") && r.getStart() == 0));
        assertTrue(regions.stream().anyMatch(r -> r.getChromosome().equals("2") && r.getStart() == 100000));
    }

    @Test
    public void testGetRegionBoundsNoDuplicates() throws Exception {
        // Create same region for multiple samples
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 0));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId2, "1", 0));

        // Get region bounds
        List<Region> regions = adaptor.getRegionBounds(studyId, 1, Arrays.asList(sampleId1, sampleId2));

        // Should only have one region (no duplicates)
        assertNotNull(regions);
        assertEquals(1, regions.size());
        assertEquals("1", regions.get(0).getChromosome());
        assertEquals(0, regions.get(0).getStart());
    }

    @Test
    public void testGetRegionBoundsEmptyWhenNoFiles() throws Exception {
        // Get region bounds when no files exist
        List<Region> regions = adaptor.getRegionBounds(studyId, 1, Arrays.asList(sampleId1));

        // Should return empty list
        assertNotNull(regions);
        assertTrue(regions.isEmpty());
    }

    @Test
    public void testChromosomeSanitization() throws Exception {
        // Test with special characters in chromosome name
        SampleIndexEntry entry = new SampleIndexEntry(sampleId1, "chr1:special", 0);
        entry.getGtEntry("0/1");

        // Write and read
        adaptor.writeEntry(studyId, 1, entry);
        SampleIndexEntry readEntry = adaptor.readEntry(studyId, 1, sampleId1, "chr1:special", 0);

        // Verify
        assertNotNull(readEntry);
        assertEquals("chr1:special", readEntry.getChromosome());
    }

    @Test
    public void testRawIterator() throws Exception {
        // Create multiple entries for one sample
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 0));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 100000));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "2", 0));

        // Get iterator
        Iterator<SampleIndexEntry> iterator = adaptor.indexEntryIterator(studyId, sampleId1);

        // Count entries
        int count = 0;
        while (iterator.hasNext()) {
            SampleIndexEntry entry = iterator.next();
            assertNotNull(entry);
            assertEquals(sampleId1, entry.getSampleId());
            count++;
        }

        // Should have 3 entries
        assertEquals(3, count);
    }

    @Test
    public void testCount() throws Exception {
        // Smoke test: count() over empty chunk entries returns 0 variants without crashing.
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 0));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 100000));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "2", 0));

        long count = adaptor.count(adaptor.parseSampleIndexQuery(new VariantQuery().sample("SAMPLE1")));

        assertEquals(0, count);
    }

    @Test
    public void testEntryWriterBatch() throws Exception {
        // The batch writer (used by indexers via SampleIndexDBAdaptor.newSampleIndexEntryWriter)
        // delegates to writeEntry per element. Verify a list of entries lands on disk.
        SampleIndexEntry e1 = new SampleIndexEntry(sampleId1, "1", 0);
        SampleIndexEntry e2 = new SampleIndexEntry(sampleId1, "1", 100000);
        SampleIndexEntry e3 = new SampleIndexEntry(sampleId2, "2", 0);

        LocalSampleIndexEntryWriter writer = adaptor.newSampleIndexEntryWriter(
                studyId, /*fileId*/ 0, schema, /*options*/ null);

        boolean ok = writer.write(Arrays.asList(e1, e2, e3));

        assertTrue(ok);
        assertNotNull(adaptor.readEntry(studyId, 1, sampleId1, "1", 0));
        assertNotNull(adaptor.readEntry(studyId, 1, sampleId1, "1", 100000));
        assertNotNull(adaptor.readEntry(studyId, 1, sampleId2, "2", 0));
    }

    @Test
    public void testIteratorByGt() throws Exception {
        // Three chunks for SAMPLE1, each with empty GT entries for "0/1" and "1/1"
        SampleIndexEntry e1 = new SampleIndexEntry(sampleId1, "1", 0);
        e1.getGtEntry("0/1");
        e1.getGtEntry("1/1");
        SampleIndexEntry e2 = new SampleIndexEntry(sampleId1, "1", 100000);
        e2.getGtEntry("0/1");
        SampleIndexEntry e3 = new SampleIndexEntry(sampleId1, "2", 0);
        e3.getGtEntry("1/1");
        adaptor.writeEntry(studyId, 1, e1);
        adaptor.writeEntry(studyId, 1, e2);
        adaptor.writeEntry(studyId, 1, e3);

        Iterator<Map<String, List<Variant>>> it = adaptor.iteratorByGt(studyId, sampleId1, schema);

        int chunks = 0;
        while (it.hasNext()) {
            Map<String, List<Variant>> byGt = it.next();
            assertNotNull(byGt);
            for (List<Variant> variants : byGt.values()) {
                // Empty entries decode to empty variant lists
                assertTrue(variants.isEmpty());
            }
            chunks++;
        }
        assertEquals(3, chunks);
    }

    @Test
    public void testFileStructure() throws Exception, IOException {
        // Write an entry
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 0));

        // Check file structure
        Path samplePath = adaptor.getSamplePath(studyId, 1, sampleId1);
        assertTrue(Files.exists(samplePath));
        assertTrue(Files.isDirectory(samplePath));

        // Check file exists
        Path filePath = samplePath.resolve("1_0.json");
        assertTrue(Files.exists(filePath));
        assertTrue(Files.isRegularFile(filePath));
    }

    @Test
    public void testRegionBoundsOrder() throws Exception {
        // Write entries out of order
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "2", 100000));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 0));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "2", 0));
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 100000));

        List<Region> regions = adaptor.getRegionBounds(studyId, 1, Arrays.asList(sampleId1));

        // Returned in deterministic (chromosome, batchStart) order regardless of write order
        assertNotNull(regions);
        assertEquals(4, regions.size());
        assertEquals("1", regions.get(0).getChromosome());
        assertEquals(0, regions.get(0).getStart());
        assertEquals("1", regions.get(1).getChromosome());
        assertEquals(100000, regions.get(1).getStart());
        assertEquals("2", regions.get(2).getChromosome());
        assertEquals(0, regions.get(2).getStart());
        assertEquals("2", regions.get(3).getChromosome());
        assertEquals(100000, regions.get(3).getStart());
    }

    @Test
    public void testRegionEndPosition() throws Exception {
        // Create entry
        adaptor.writeEntry(studyId, 1, new SampleIndexEntry(sampleId1, "1", 0));

        // Get region bounds
        List<Region> regions = adaptor.getRegionBounds(studyId, 1, Arrays.asList(sampleId1));

        // Verify end position is batchStart + BATCH_SIZE - 1
        assertEquals(1, regions.size());
        Region region = regions.get(0);
        assertEquals(0, region.getStart());
        assertEquals(SampleIndexSchema.BATCH_SIZE - 1, region.getEnd());
    }
}
