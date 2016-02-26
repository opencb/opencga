package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.hadoop.variant.index.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableMapper;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;

import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created on 21/01/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopMultiSampleTest extends HadoopVariantStorageManagerTestUtils {

    public static final List<VariantType> VARIANT_TYPES = Arrays.asList(VariantTableMapper.TARGET_VARIANT_TYPE);

    @Before
    public void loadSingleVcf() throws Exception {
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
        HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();
        URI fileInputUri = VariantStorageManagerTestUtils.getResourceUri(resourceName);

        ObjectMap params = new ObjectMap(VariantStorageManager.Options.TRANSFORM_FORMAT.key(), "avro")
                .append(VariantStorageManager.Options.STUDY_CONFIGURATION.key(), studyConfiguration)
                .append(VariantStorageManager.Options.STUDY_ID.key(), studyConfiguration.getStudyId())
                .append(VariantStorageManager.Options.DB_NAME.key(), DB_NAME)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantAnnotationManager.SPECIES, "hsapiens")
                .append(VariantAnnotationManager.ASSEMBLY, "GRc37")
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(HadoopVariantStorageManager.HADOOP_LOAD_ARCHIVE, true)
                .append(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT, true);
        if (otherParams != null) {
            params.putAll(otherParams);
        }

        if (fileId > 0) {
            params.append(VariantStorageManager.Options.FILE_ID.key(), fileId);
        }

        ETLResult etlResult = runETL(variantStorageManager, fileInputUri, outputUri, params, params, params,
                params, params, params, params, true, true, true);

        return variantStorageManager.readVariantSource(etlResult.transformResult, new ObjectMap());
    }

    @Test
    public void testTwoFiles() throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageManagerTestUtils.newStudyConfiguration();
        VariantSource source1 = loadFile("s1.genome.vcf", studyConfiguration, Collections.emptyMap());
        VariantSource source2 = loadFile("s2.genome.vcf", studyConfiguration, Collections.emptyMap());
        printVariantsFromArchiveTable(studyConfiguration);

        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);

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
        assertEquals("0/1", variants.get("1:10032:A:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("1", variants.get("1:10032:A:G").getStudy(studyName).getAttributes().get("FILTER"));
        assertEquals("0/0", variants.get("1:10032:A:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertTrue(variants.containsKey("1:11000:T:G"));
        assertEquals("1/1", variants.get("1:11000:T:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:11000:T:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertTrue(variants.containsKey("1:12000:T:G"));
        assertEquals("1/1", variants.get("1:12000:T:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/0", variants.get("1:12000:T:G").getStudy(studyName).getSampleData("s2", "GT"));

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


        for (int fileId = 12877; fileId <= 12893; fileId++) {
            VariantSource source = loadFile("1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz", fileId, studyConfiguration);
            Set<String> variants = checkArchiveTableLoadedVariants(studyConfiguration, dbAdaptor, source);
            sources.add(source);
            expectedVariants.addAll(variants);
            assertTrue(studyConfiguration.getIndexedFiles().contains(fileId));
        }


        checkLoadedVariants(expectedVariants, dbAdaptor);

        printVariantsFromArchiveTable(studyConfiguration);

        for (Variant variant : dbAdaptor) {
            System.out.println("variant = " + variant);
        }


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

        for (fileId = 12877; fileId <= 12893; fileId++) {
            assertTrue(studyConfiguration.getIndexedFiles().contains(fileId));
        }

        checkLoadedVariants(expectedVariants, dbAdaptor);
    }

    public void checkLoadedVariants(Set<String> expectedVariants, VariantHadoopDBAdaptor dbAdaptor) {
        long count = dbAdaptor.count(null).first();
        System.out.println("count = " + count);
        System.out.println("expectedVariants = " + expectedVariants.size());
        assertEquals(expectedVariants.size(), count);
        count = 0;
        for (Variant variant : dbAdaptor) {
            count++;
            assertTrue(expectedVariants.contains(variant.toString()));
        }
        assertEquals(expectedVariants.size(), count);
    }

    public Set<String> checkArchiveTableLoadedVariants(StudyConfiguration studyConfiguration, VariantHadoopDBAdaptor dbAdaptor, VariantSource source) {
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


}