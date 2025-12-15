package org.opencb.opencga.storage.core.variant.index.sample.local;

//import org.junit.After;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
//import org.opencb.biodata.models.core.Region;
//import org.opencb.biodata.models.variant.Variant;
//import org.opencb.commons.datastore.core.ObjectMap;
//import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
//import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
//import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
//import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
//import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
//import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchemaFactory;
//
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.Arrays;
//import java.util.Iterator;
//import java.util.List;
//
//import static org.junit.Assert.*;

/**
 * Test class for LocalSampleIndexDBAdaptor.
 * Tests basic read/write operations, region bounds extraction, and file structure.
 */
public class LocalSampleIndexDBAdaptorTest {

//    @Rule
//    public TemporaryFolder temporaryFolder = new TemporaryFolder();
//
//    private LocalSampleIndexDBAdaptor adaptor;
//    private VariantStorageMetadataManager metadataManager;
//    private int studyId;
//    private int sampleId1;
//    private int sampleId2;
//    private SampleIndexSchema schema;
//
//    @Before
//    public void setUp() throws Exception {
//        // Create temporary directory for test data
//        Path tempDir = temporaryFolder.newFolder("sample_index").toPath();
//
//        // Set up metadata manager
//        DummyVariantStorageMetadataDBAdaptorFactory.clear();
//        metadataManager = new VariantStorageMetadataManager(
//                new DummyVariantStorageMetadataDBAdaptorFactory());
//        studyId = metadataManager.createStudy("TEST_STUDY").getId();
//        sampleId1 = metadataManager.registerSample(studyId, "SAMPLE1", null);
//        sampleId2 = metadataManager.registerSample(studyId, "SAMPLE2", null);
//
//        // Create schema factory
//        SampleIndexSchemaFactory schemaFactory = new SampleIndexSchemaFactory(metadataManager);
//        schema = schemaFactory.getSchema(studyId, 1);
//
//        // Create adaptor
//        adaptor = new LocalSampleIndexDBAdaptor(tempDir, schemaFactory, true);
//    }
//
//    @After
//    public void tearDown() {
//        // Cleanup is handled by TemporaryFolder rule
//    }
//
//    @Test
//    public void testWriteAndReadEntry() throws StorageEngineException {
//        // Create a sample entry
//        SampleIndexEntry entry = new SampleIndexEntry("1", 0, sampleId1);
//        entry.getGtEntry("0/1");  // Add a genotype entry
//
//        // Write entry
//        adaptor.writeEntry(studyId, 1, entry);
//
//        // Read entry back
//        SampleIndexEntry readEntry = adaptor.readEntry(studyId, 1, sampleId1, "1", 0);
//
//        // Verify
//        assertNotNull(readEntry);
//        assertEquals("1", readEntry.getChromosome());
//        assertEquals(0, readEntry.getBatchStart());
//        assertEquals(sampleId1, readEntry.getSampleId());
//        assertTrue(readEntry.getGts().containsKey("0/1"));
//    }
//
//    @Test
//    public void testReadNonExistentEntry() throws StorageEngineException {
//        // Try to read entry that doesn't exist
//        SampleIndexEntry entry = adaptor.readEntry(studyId, 1, sampleId1, "1", 0);
//
//        // Should return null
//        assertNull(entry);
//    }
//
//    @Test
//    public void testWriteMultipleEntries() throws StorageEngineException {
//        // Create entries for different regions
//        SampleIndexEntry entry1 = new SampleIndexEntry("1", 0, sampleId1);
//        entry1.getGtEntry("0/1");
//
//        SampleIndexEntry entry2 = new SampleIndexEntry("1", 100000, sampleId1);
//        entry2.getGtEntry("1/1");
//
//        SampleIndexEntry entry3 = new SampleIndexEntry("2", 0, sampleId1);
//        entry3.getGtEntry("0/1");
//
//        // Write entries
//        adaptor.writeEntry(studyId, 1, entry1);
//        adaptor.writeEntry(studyId, 1, entry2);
//        adaptor.writeEntry(studyId, 1, entry3);
//
//        // Read back and verify
//        assertNotNull(adaptor.readEntry(studyId, 1, sampleId1, "1", 0));
//        assertNotNull(adaptor.readEntry(studyId, 1, sampleId1, "1", 100000));
//        assertNotNull(adaptor.readEntry(studyId, 1, sampleId1, "2", 0));
//    }
//
//    @Test
//    public void testWriteMultipleSamples() throws StorageEngineException {
//        // Create entries for different samples
//        SampleIndexEntry entry1 = new SampleIndexEntry("1", 0, sampleId1);
//        entry1.getGtEntry("0/1");
//
//        SampleIndexEntry entry2 = new SampleIndexEntry("1", 0, sampleId2);
//        entry2.getGtEntry("1/1");
//
//        // Write entries
//        adaptor.writeEntry(studyId, 1, entry1);
//        adaptor.writeEntry(studyId, 1, entry2);
//
//        // Read back and verify
//        SampleIndexEntry read1 = adaptor.readEntry(studyId, 1, sampleId1, "1", 0);
//        SampleIndexEntry read2 = adaptor.readEntry(studyId, 1, sampleId2, "1", 0);
//
//        assertNotNull(read1);
//        assertNotNull(read2);
//        assertEquals(sampleId1, read1.getSampleId());
//        assertEquals(sampleId2, read2.getSampleId());
//    }
//
//    @Test
//    public void testOverwriteEntry() throws StorageEngineException {
//        // Create and write initial entry
//        SampleIndexEntry entry1 = new SampleIndexEntry("1", 0, sampleId1);
//        entry1.getGtEntry("0/1");
//        adaptor.writeEntry(studyId, 1, entry1);
//
//        // Overwrite with new entry
//        SampleIndexEntry entry2 = new SampleIndexEntry("1", 0, sampleId1);
//        entry2.getGtEntry("1/1");
//        adaptor.writeEntry(studyId, 1, entry2);
//
//        // Read back
//        SampleIndexEntry readEntry = adaptor.readEntry(studyId, 1, sampleId1, "1", 0);
//
//        // Should have the new genotype
//        assertNotNull(readEntry);
//        assertTrue(readEntry.getGts().containsKey("1/1"));
//        assertFalse(readEntry.getGts().containsKey("0/1"));
//    }
//
//    @Test
//    public void testGetRegionBounds() throws StorageEngineException {
//        // Create entries for different regions
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 0, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 100000, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("2", 0, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("2", 100000, sampleId2));
//
//        // Get region bounds
//        List<Region> regions = adaptor.getRegionBounds(studyId, 1, Arrays.asList(sampleId1, sampleId2));
//
//        // Verify we got all unique regions
//        assertNotNull(regions);
//        assertEquals(4, regions.size());
//
//        // Check that regions are correct
//        assertTrue(regions.stream().anyMatch(r -> r.getChromosome().equals("1") && r.getStart() == 0));
//        assertTrue(regions.stream().anyMatch(r -> r.getChromosome().equals("1") && r.getStart() == 100000));
//        assertTrue(regions.stream().anyMatch(r -> r.getChromosome().equals("2") && r.getStart() == 0));
//        assertTrue(regions.stream().anyMatch(r -> r.getChromosome().equals("2") && r.getStart() == 100000));
//    }
//
//    @Test
//    public void testGetRegionBoundsNoDuplicates() throws StorageEngineException {
//        // Create same region for multiple samples
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 0, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 0, sampleId2));
//
//        // Get region bounds
//        List<Region> regions = adaptor.getRegionBounds(studyId, 1, Arrays.asList(sampleId1, sampleId2));
//
//        // Should only have one region (no duplicates)
//        assertNotNull(regions);
//        assertEquals(1, regions.size());
//        assertEquals("1", regions.get(0).getChromosome());
//        assertEquals(0, regions.get(0).getStart());
//    }
//
//    @Test
//    public void testGetRegionBoundsEmptyWhenNoFiles() throws StorageEngineException {
//        // Get region bounds when no files exist
//        List<Region> regions = adaptor.getRegionBounds(studyId, 1, Arrays.asList(sampleId1));
//
//        // Should return empty list
//        assertNotNull(regions);
//        assertTrue(regions.isEmpty());
//    }
//
//    @Test
//    public void testChromosomeSanitization() throws StorageEngineException {
//        // Test with special characters in chromosome name
//        SampleIndexEntry entry = new SampleIndexEntry("chr1:special", 0, sampleId1);
//        entry.getGtEntry("0/1");
//
//        // Write and read
//        adaptor.writeEntry(studyId, 1, entry);
//        SampleIndexEntry readEntry = adaptor.readEntry(studyId, 1, sampleId1, "chr1:special", 0);
//
//        // Verify
//        assertNotNull(readEntry);
//        assertEquals("chr1:special", readEntry.getChromosome());
//    }
//
//    @Test
//    public void testRawIterator() throws StorageEngineException {
//        // Create multiple entries for one sample
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 0, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 100000, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("2", 0, sampleId1));
//
//        // Get iterator
//        Iterator<SampleIndexEntry> iterator = adaptor.rawIterator(studyId, sampleId1);
//
//        // Count entries
//        int count = 0;
//        while (iterator.hasNext()) {
//            SampleIndexEntry entry = iterator.next();
//            assertNotNull(entry);
//            assertEquals(sampleId1, entry.getSampleId());
//            count++;
//        }
//
//        // Should have 3 entries
//        assertEquals(3, count);
//    }
//
//    @Test
//    public void testCount() throws StorageEngineException {
//        // Create entries
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 0, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 100000, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("2", 0, sampleId1));
//
//        // Count
//        long count = adaptor.count(studyId, sampleId1);
//
//        // Should have 3 entries
//        assertEquals(3, count);
//    }
//
//    @Test
//    public void testFileStructure() throws StorageEngineException, IOException {
//        // Write an entry
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 0, sampleId1));
//
//        // Check file structure
//        Path samplePath = adaptor.getSamplePath(studyId, 1, sampleId1);
//        assertTrue(Files.exists(samplePath));
//        assertTrue(Files.isDirectory(samplePath));
//
//        // Check file exists
//        Path filePath = samplePath.resolve("1_0.json");
//        assertTrue(Files.exists(filePath));
//        assertTrue(Files.isRegularFile(filePath));
//    }
//
//    @Test
//    public void testRegionBoundsOrder() throws StorageEngineException {
//        // Create entries in specific order
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("2", 100000, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 0, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("2", 0, sampleId1));
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 100000, sampleId1));
//
//        // Get region bounds
//        List<Region> regions = adaptor.getRegionBounds(studyId, 1, Arrays.asList(sampleId1));
//
//        // Verify order is maintained (insertion order via LinkedHashSet)
//        assertNotNull(regions);
//        assertEquals(4, regions.size());
//
//        // Regions should be in the order they were encountered
//        assertEquals("2", regions.get(0).getChromosome());
//        assertEquals(100000, regions.get(0).getStart());
//        assertEquals("1", regions.get(1).getChromosome());
//        assertEquals(0, regions.get(1).getStart());
//        assertEquals("2", regions.get(2).getChromosome());
//        assertEquals(0, regions.get(2).getStart());
//        assertEquals("1", regions.get(3).getChromosome());
//        assertEquals(100000, regions.get(3).getStart());
//    }
//
//    @Test
//    public void testRegionEndPosition() throws StorageEngineException {
//        // Create entry
//        adaptor.writeEntry(studyId, 1, new SampleIndexEntry("1", 0, sampleId1));
//
//        // Get region bounds
//        List<Region> regions = adaptor.getRegionBounds(studyId, 1, Arrays.asList(sampleId1));
//
//        // Verify end position is batchStart + BATCH_SIZE - 1
//        assertEquals(1, regions.size());
//        Region region = regions.get(0);
//        assertEquals(0, region.getStart());
//        assertEquals(SampleIndexSchema.BATCH_SIZE - 1, region.getEnd());
//    }
}
