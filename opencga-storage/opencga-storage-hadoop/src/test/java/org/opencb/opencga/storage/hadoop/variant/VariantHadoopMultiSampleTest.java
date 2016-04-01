package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableMapper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;

import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created on 21/01/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopMultiSampleTest extends VariantStorageManagerTestUtils implements HadoopVariantStorageManagerTestUtils {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    public static final List<VariantType> VARIANT_TYPES = Arrays.asList(VariantTableMapper.TARGET_VARIANT_TYPE);

    // Variants that are wrong in the platinum files that should not be included
    private static final HashSet<String> PLATINUM_SKIP_VARIANTS = new HashSet<>(Arrays.asList("M:515:G:A", "1:10352:T:A"));

    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        //Force HBaseConverter to fail if something goes wrong
        HBaseToVariantConverter.setFailOnWrongVariants(true);

    }

    public VariantSource loadFile(String resourceName, int fileId, StudyConfiguration studyConfiguration) throws Exception {
        return loadFile(resourceName, fileId, studyConfiguration, null);
    }

    public VariantSource loadFile(String resourceName, StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams) throws Exception {
        return loadFile(resourceName, -1, studyConfiguration, otherParams);
    }

    public VariantSource loadFile(String resourceName, int fileId, StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams) throws Exception {
        return loadFile(resourceName, fileId, studyConfiguration, otherParams, true, true, true);
    }

    public VariantSource loadFile(String resourceName, int fileId, StudyConfiguration studyConfiguration,
                                  Map<? extends String, ?> otherParams, boolean doTransform, boolean loadArchive, boolean loadVariant)
            throws Exception {
        return VariantHbaseTestUtils.loadFile(getVariantStorageManager(), DB_NAME, outputUri, resourceName, fileId, studyConfiguration,
                otherParams, doTransform, loadArchive, loadVariant);
    }

    @Test
    public void testTwoFiles() throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageManagerTestUtils.newStudyConfiguration();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
        VariantSource source1 = loadFile("s1.genome.vcf", studyConfiguration, Collections.emptyMap());
        checkArchiveTableTimeStamp(dbAdaptor);
        VariantSource source2 = loadFile("s2.genome.vcf", studyConfiguration, Collections.emptyMap());
        checkArchiveTableTimeStamp(dbAdaptor);
        printVariantsFromArchiveTable(studyConfiguration);


        checkLoadedFilesS1S2(studyConfiguration, dbAdaptor);

    }

    @Test
    public void testTwoFilesFailOne() throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageManagerTestUtils.newStudyConfiguration();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
        try {
            VariantSource source1 = loadFile("s1.genome.vcf", studyConfiguration,
                    Collections.singletonMap(TestMRExecutor.VariantTableMapperFail.SLICE_TO_FAIL, "1_000000000011"));
            fail();
        } catch (Exception e) {
            HBaseStudyConfigurationManager scm = (HBaseStudyConfigurationManager) dbAdaptor.getStudyConfigurationManager();
            studyConfiguration = scm.toHBaseStudyConfiguration(scm.getStudyConfiguration(STUDY_ID, new QueryOptions()).first());
            System.out.println(studyConfiguration.getIndexedFiles());
            e.printStackTrace();
        }
        Integer fileId = studyConfiguration.getFileIds().get("s1.genome.vcf");
        System.out.println("fileId = " + fileId);
        VariantSource source1 = loadFile("s1.genome.vcf.variants.avro.gz", 1, studyConfiguration,
                Collections.singletonMap(TestMRExecutor.VariantTableMapperFail.SLICE_TO_FAIL, "_"), false, false, true);
        checkArchiveTableTimeStamp(dbAdaptor);
        VariantSource source2 = loadFile("s2.genome.vcf", studyConfiguration, Collections.emptyMap());
        checkArchiveTableTimeStamp(dbAdaptor);
        printVariantsFromArchiveTable(studyConfiguration);


        checkLoadedFilesS1S2(studyConfiguration, dbAdaptor);

    }

    public void checkLoadedFilesS1S2(StudyConfiguration studyConfiguration, VariantHadoopDBAdaptor dbAdaptor) {
        System.out.println("studyConfiguration = " + studyConfiguration);
        Map<String, Variant> variants = new HashMap<>();
        for (Variant variant : dbAdaptor) {
            String v = variant.toString();
            assertFalse(variants.containsKey(v));
            variants.put(v, variant);
            VariantAnnotation a = variant.getAnnotation();
            variant.setAnnotation(null);
            System.out.println(variant.toJson());
            variant.setAnnotation(a);
        }
        String studyName = Integer.toString(studyConfiguration.getStudyId());

        // TODO: Add more asserts
        /*                      s1  s2
        1	10013	T	C   0/1 0/0
        1	10014	A	T   0/1 0/2
        1	10014	A	G   0/2 0/1
        1	10030	T	G   0/0 0/1
        1	10031	T	G   0/1 0/1
        1	10032	A	G   0/1 0/0
        1   11000   T   G   1/1 0/1
        1   12000   T   G   1/1 0/0
        1   13000   T   G   0/0 0/1
        */

        assertEquals(9, variants.size());
        assertTrue(variants.containsKey("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/0", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        assertTrue(variants.containsKey("1:10014:A:T"));
        assertEquals("0/1", variants.get("1:10014:A:T").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/2", variants.get("1:10014:A:T").getStudy(studyName).getSampleData("s2", "GT"));

        assertTrue(variants.containsKey("1:10014:A:G"));
        assertEquals("0/2", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertTrue(variants.containsKey("1:10030:T:G"));
        assertEquals("0/0", variants.get("1:10030:T:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:10030:T:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertTrue(variants.containsKey("1:10031:T:G"));
        assertEquals("0/1", variants.get("1:10031:T:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:10031:T:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertTrue(variants.containsKey("1:10032:A:G"));
        assertEquals("1", variants.get("1:10032:A:G").getStudy(studyName).getFiles().get(0).getAttributes().get("PASS"));
        assertEquals("0/1", variants.get("1:10032:A:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("PASS", variants.get("1:10032:A:G").getStudy(studyName).getSampleData("s1", VariantMerger.GENOTYPE_FILTER_KEY));
        assertEquals("0/0", variants.get("1:10032:A:G").getStudy(studyName).getSampleData("s2", "GT"));
        assertEquals("LowGQX", variants.get("1:10032:A:G").getStudy(studyName).getSampleData("s2", VariantMerger.GENOTYPE_FILTER_KEY));

        assertTrue(variants.containsKey("1:11000:T:G"));
        assertEquals("1/1", variants.get("1:11000:T:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:11000:T:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertTrue(variants.containsKey("1:12000:T:G"));
        assertEquals("1/1", variants.get("1:12000:T:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(".", variants.get("1:12000:T:G").getStudy(studyName).getSampleData("s1", VariantMerger.GENOTYPE_FILTER_KEY));
        assertEquals("0/0", variants.get("1:12000:T:G").getStudy(studyName).getSampleData("s2", "GT"));
        assertEquals("HighDPFRatio;LowGQX", variants.get("1:12000:T:G").getStudy(studyName).getSampleData("s2", VariantMerger.GENOTYPE_FILTER_KEY));

        assertTrue(variants.containsKey("1:13000:T:G"));
        assertEquals("0/0", variants.get("1:13000:T:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:13000:T:G").getStudy(studyName).getSampleData("s2", "GT"));
    }

    public VariantHadoopDBAdaptor printVariantsFromArchiveTable(StudyConfiguration studyConfiguration) throws Exception {
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);

        GenomeHelper helper = dbAdaptor.getGenomeHelper();
        helper.getHBaseManager().act(HadoopVariantStorageManager.getTableName(studyConfiguration.getStudyId()), table -> {
            for (Result result : table.getScanner(helper.getColumnFamily())) {
                try {
                    byte[] value = result.getValue(helper.getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
                    if (value != null) {
                        System.out.println(VariantTableStudyRowsProto.parseFrom(value));
                    }
                } catch (Exception e) {
                    System.out.println("e.getMessage() = " + e.getMessage());
                }
            }
            return 0;
        });
        return dbAdaptor;
    }


    @Test
    public void testPlatinumFilesOneByOne() throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageManagerTestUtils.newStudyConfiguration();
        List<VariantSource> sources = new LinkedList<>();
        Set<String> expectedVariants = new HashSet<>();

        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
        HBaseStudyConfigurationManager scm = (HBaseStudyConfigurationManager) dbAdaptor.getStudyConfigurationManager();


        for (int fileId = 12877; fileId <= 12893; fileId++) {
            VariantSource source = loadFile("1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz", fileId, studyConfiguration);

            studyConfiguration = scm.getStudyConfiguration(studyConfiguration.getStudyId(), new QueryOptions()).first();
            System.out.println(studyConfiguration);

            Set<String> variants = checkArchiveTableLoadedVariants(studyConfiguration, dbAdaptor, source);
            sources.add(source);
            expectedVariants.addAll(variants);
            assertTrue(studyConfiguration.getIndexedFiles().contains(fileId));

//            checkLoadedVariants(expectedVariants, dbAdaptor, PLATINUM_SKIP_VARIANTS);
            checkArchiveTableTimeStamp(dbAdaptor);
        }


        checkLoadedVariants(expectedVariants, dbAdaptor, PLATINUM_SKIP_VARIANTS);

        printVariantsFromArchiveTable(studyConfiguration);

        for (Variant variant : dbAdaptor) {
            System.out.println("variant = " + variant);
        }

        System.out.println(studyConfiguration);

    }

    @Test
    public void testPlatinumFilesBatchLoad() throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageManagerTestUtils.newStudyConfiguration();
        List<VariantSource> sources = new LinkedList<>();
        Set<String> expectedVariants = new HashSet<>();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);

        int fileId;
        String pending = "";
        for (fileId = 12877; fileId < 12893; fileId++) {
            VariantSource source = loadFile("1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz", fileId, studyConfiguration,
                    new ObjectMap(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT, false));
            sources.add(source);
            expectedVariants.addAll(checkArchiveTableLoadedVariants(studyConfiguration, dbAdaptor, source));
            pending += fileId + ",";
            assertFalse(studyConfiguration.getIndexedFiles().contains(fileId));
        }
        pending += fileId + ",";
        VariantSource source = loadFile("1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz", fileId, studyConfiguration,
                new ObjectMap(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT, true)
                .append(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT_PENDING_FILES, pending)
        );
        sources.add(source);
        expectedVariants.addAll(checkArchiveTableLoadedVariants(studyConfiguration, dbAdaptor, source));

        HBaseStudyConfigurationManager scm = (HBaseStudyConfigurationManager) dbAdaptor.getStudyConfigurationManager();
        studyConfiguration = scm.getStudyConfiguration(studyConfiguration.getStudyId(), new QueryOptions()).first();

        System.out.println("studyConfiguration = " + studyConfiguration.getAttributes().toJson());
        System.out.println("HBaseStudyConfiguration = " + scm.toHBaseStudyConfiguration(studyConfiguration));

        for (fileId = 12877; fileId <= 12893; fileId++) {
            assertTrue(studyConfiguration.getIndexedFiles().contains(fileId));
        }

        checkArchiveTableTimeStamp(dbAdaptor);
        checkLoadedVariants(expectedVariants, dbAdaptor, PLATINUM_SKIP_VARIANTS);
    }

    public void checkLoadedVariants(Set<String> expectedVariants, VariantHadoopDBAdaptor dbAdaptor, HashSet<String> platinumSkipVariants) {
        long count = dbAdaptor.count(null).first();
        expectedVariants.removeAll(platinumSkipVariants);
        System.out.println("count = " + count);
        System.out.println("expectedVariants = " + expectedVariants.size());
        if (expectedVariants.size() != count) {
            Set<String> loadedVariants = new HashSet<>();
            for (Variant variant : dbAdaptor) {
                loadedVariants.add(variant.toString());
                if (!expectedVariants.contains(variant.toString())) {
                    System.out.println("unexpectedVariant: " + variant);
                }
            }
            for (String expectedVariant : expectedVariants) {
                if (!loadedVariants.contains(expectedVariant)) {
                    System.out.println("Missing variant: " + expectedVariant);
                }
            }

        }
        assertEquals(expectedVariants.size(), count);
        count = 0;
        for (Variant variant : dbAdaptor) {
            count++;
            assertTrue(expectedVariants.contains(variant.toString()));
        }
        assertEquals(expectedVariants.size(), count);
    }

    public Set<String> checkArchiveTableLoadedVariants(StudyConfiguration studyConfiguration, VariantHadoopDBAdaptor dbAdaptor,
                                                       VariantSource source) {
        int fileId = Integer.valueOf(source.getFileId());
        Set<String> variants = getVariants(dbAdaptor, studyConfiguration, fileId);
        assertEquals(source.getStats().getVariantTypeCounts().entrySet().stream()
                .filter(entry -> VARIANT_TYPES.contains(VariantType.valueOf(entry.getKey())))
                .map(Map.Entry::getValue).reduce((i1, i2) -> i1 + i2).orElse(0).intValue(), variants.size());
        return variants;
    }


    protected Set<String> getVariants(VariantHadoopDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration, int fileId){
//        Map<String, Integer> variantCounts = new HashMap<>();
        Set<String> variants = new HashSet<>();

        System.out.println("Query from Archive table");
        dbAdaptor.iterator(
                new Query()
                        .append(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId())
                        .append(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileId),
                new QueryOptions("archive", true))
                .forEachRemaining(variant -> {
                    if (VARIANT_TYPES.contains(variant.getType())) {
                        variants.add(variant.toString());
                    }
//                    variantCounts.compute(variant.getType().toString(), (s, integer) -> integer == null ? 1 : (integer + 1));
                });
        return variants;
    }

    protected void checkArchiveTableTimeStamp(VariantHadoopDBAdaptor dbAdaptor) throws Exception {
        HBaseStudyConfigurationManager scm = (HBaseStudyConfigurationManager) dbAdaptor.getStudyConfigurationManager();
        HBaseVariantStudyConfiguration studyConfiguration = scm.toHBaseStudyConfiguration(scm.getStudyConfiguration(STUDY_ID, new QueryOptions()).first());

        String tableName = HadoopVariantStorageManager.getTableName(STUDY_ID);
        System.out.println("Query from archive HBase " + tableName);
        HBaseManager hm = new HBaseManager(configuration.get());

        GenomeHelper helper = dbAdaptor.getGenomeHelper();

        long ts = studyConfiguration.getBatches().get(studyConfiguration.getBatches().size() - 1).getTimestamp();

        hm.act(tableName, table -> {
            Scan scan = new Scan();
            scan.addColumn(helper.getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                Cell cell = result.getColumnLatestCell(helper.getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
                assertNotNull(cell);
                assertEquals(ts, cell.getTimestamp());
            }
            resultScanner.close();
            return null;
        });


    }

}