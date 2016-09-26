package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.biodata.tools.variant.converter.VcfSliceToVariantListConverter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager.Options;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableMapper;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created on 15/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopManagerTest extends VariantStorageManagerTestUtils implements HadoopVariantStorageManagerTestUtils {

    private VariantHadoopDBAdaptor dbAdaptor;
    private static StudyConfiguration studyConfiguration;
    private static VariantSource source;
    private static StorageETLResult etlResult = null;

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();
    private QueryResult<Variant> allVariantsQueryResult;

    @Before
    public void before() throws Exception {
        if (etlResult == null) {
            HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();
            clearDB(variantStorageManager.getVariantTableName(DB_NAME));
            clearDB(variantStorageManager.getArchiveTableName(STUDY_ID));

            URI inputUri = VariantStorageManagerTestUtils.getResourceUri("sample1.genome.vcf");
//            URI inputUri = VariantStorageManagerTestUtils.getResourceUri("variant-test-file.vcf.gz");

            studyConfiguration = VariantStorageManagerTestUtils.newStudyConfiguration();
            etlResult = VariantStorageManagerTestUtils.runDefaultETL(inputUri, variantStorageManager, studyConfiguration,
                    new ObjectMap(Options.TRANSFORM_FORMAT.key(), "avro")
                            .append(Options.FILE_ID.key(), FILE_ID)
                            .append(Options.ANNOTATE.key(), true)
                            .append(Options.CALCULATE_STATS.key(), false)
                            .append(HadoopVariantStorageManager.HADOOP_LOAD_DIRECT, false)
                            .append(HadoopVariantStorageManager.HADOOP_LOAD_ARCHIVE, true)
                            .append(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT, true)
            );

            source = variantStorageManager.readVariantSource(etlResult.getTransformResult());
            VariantGlobalStats stats = source.getStats();
            Assert.assertNotNull(stats);

            allVariantsQueryResult = null;
            dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);
            VariantHbaseTestUtils.printVariantsFromVariantsTable(dbAdaptor);
            VariantHbaseTestUtils.printVariantsFromArchiveTable(dbAdaptor, studyConfiguration);
        }
        HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();
        dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);

        if (allVariantsQueryResult == null) {
            allVariantsQueryResult = dbAdaptor.get(new Query(), new QueryOptions());
        }

    }

    @Test
    public void queryVariantTable() {
        System.out.println("Query from Variant table");
        VariantDBIterator iterator = dbAdaptor.iterator(
                new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId()),
                new QueryOptions());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            System.out.println("Phoenix variant = " + variant);
        }
        System.out.println("End query from Analysis table");
    }

    @Test
    public void countVariants() {
        long totalCount = dbAdaptor.count(new Query()).first();
        long partialCount1 = dbAdaptor.count(new Query(VariantDBAdaptor.VariantQueryParams.REGION.key(), "1:1-15030")).first();
        long partialCount2 = dbAdaptor.count(new Query(VariantDBAdaptor.VariantQueryParams.REGION.key(), "1:15030-60000")).first();


        long count = Arrays.stream(VariantTableMapper.getTargetVariantType())
                .map(type -> source.getStats().getVariantTypeCount(type))
                .reduce((a, b) -> a + b)
                .orElse(0).longValue();
        count  -= 1; // Deletion is in conflict with other variant: 1:10403:ACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAAC:A
        assertEquals(count, totalCount);
        assertEquals(totalCount, partialCount1 + partialCount2);
    }

    @Test
    public void getVariantByGene() {

        // Group by Gene
        HashMap<String, Long> genesCount = new HashMap<>();
        for (Variant variant : allVariantsQueryResult.getResult()) {
            HashSet<String> genesInVariant = new HashSet<>();
            for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                String geneName = consequenceType.getGeneName();
                if (geneName != null) {
                    genesInVariant.add(geneName);
                }
                geneName = consequenceType.getEnsemblGeneId();
                if (geneName != null) {
                    genesInVariant.add(geneName);
                }
            }
            for (String geneName : genesInVariant) {
                genesCount.put(geneName, genesCount.getOrDefault(geneName, 0L) + 1);
            }
        }
        System.out.println("genesCount = " + genesCount);

        //Count for each gene
        for (Map.Entry<String, Long> entry : genesCount.entrySet()) {
            System.out.println("Gene " + entry.getKey() + " in " + entry.getValue() + " variants");
            QueryResult<Long> queryResult = dbAdaptor.count(new Query(VariantDBAdaptor.VariantQueryParams.GENE.key(), entry.getKey()));
            System.out.println("queryResult.getDbTime() = " + queryResult.getDbTime());
            long count = queryResult.first();
            assertEquals(entry.getValue().longValue(), count);
        }

    }

    @Test
    public void queryArchiveTable() {
        final int[] numVariants = {0};
        Map<String, Integer> variantCounts = new HashMap<>();
        System.out.println("Query from Archive table");
        dbAdaptor.iterator(
                new Query()
                        .append(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId())
                        .append(VariantDBAdaptor.VariantQueryParams.FILES.key(), FILE_ID),
                new QueryOptions("archive", true)).forEachRemaining(variant -> {
            System.out.println("Variant from archive = " + variant.toJson());
            numVariants[0]++;
            variantCounts.compute(variant.getType().toString(), (s, integer) -> integer == null ? 1 : (integer + 1));
        });
        System.out.println("End query from Archive table");
        source.getStats().getVariantTypeCounts().forEach((s, integer) -> assertEquals(integer, variantCounts.getOrDefault(s, 0)));
        assertEquals(source.getStats().getNumRecords(), numVariants[0]);
    }

    @Test
    public void checkVariantTable() throws IOException {
        System.out.println("Query from HBase : " + DB_NAME);
        HBaseManager hm = new HBaseManager(configuration.get());
        GenomeHelper genomeHelper = dbAdaptor.getGenomeHelper();
        int numVariants = hm.act(DB_NAME, table -> {
            int num = 0;
            ResultScanner resultScanner = table.getScanner(genomeHelper.getColumnFamily());
            for (Result result : resultScanner) {
                if (Bytes.toString(result.getRow()).startsWith(genomeHelper.getMetaRowKeyString())) {
                    continue;
                }
                Variant variant = genomeHelper.extractVariantFromVariantRowKey(result.getRow());
                System.out.println("Variant = " + variant);
                if (!variant.getChromosome().equals(genomeHelper.getMetaRowKeyString())) {
                    num++;
                }
            }
            resultScanner.close();
            return num;
        });
        System.out.println("End query from HBase : " + DB_NAME);
        System.out.println(source.getStats().getVariantTypeCounts());
        long count = Arrays.stream(VariantTableMapper.getTargetVariantType())
                .map(type -> source.getStats().getVariantTypeCount(type))
                .reduce((a, b) -> a + b).orElse(0).longValue();
        count  -= 1; // Deletion is in conflict with other variant: 1:10403:ACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAAC:A
        assertEquals(count, numVariants);
    }

    @Test
    public void checkArchiveTable() throws Exception {
        String tableName = getVariantStorageManager().getArchiveTableName(STUDY_ID);
        System.out.println("Query from archive HBase " + tableName);
        HBaseManager hm = new HBaseManager(configuration.get());
        GenomeHelper genomeHelper = dbAdaptor.getGenomeHelper();
        ArchiveHelper archiveHelper = dbAdaptor.getArchiveHelper(studyConfiguration.getStudyId(), FILE_ID);
        VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(archiveHelper.getMeta());
        hm.act(tableName, table -> {
            ResultScanner resultScanner = table.getScanner(genomeHelper.getColumnFamily());
            for (Result result : resultScanner) {
                System.out.println("VcfSlice = " + Bytes.toString(result.getRow()));
                if (Arrays.equals(result.getRow(), archiveHelper.getMetaRowKey())) {
                    continue;
                }
                byte[] value = result.getValue(archiveHelper.getColumnFamily(), archiveHelper.getColumn());
                VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(
                        value);
                System.out.println(vcfSlice);
                List<Variant> variants = converter.convert(vcfSlice);
                for (Variant variant : variants) {
                    System.out.println(variant.toJson());
                }

                Cell cell = result.getColumnLatestCell(archiveHelper.getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
                if (cell != null) {
                    value = result.getValue(archiveHelper.getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
                    VariantTableStudyRowsProto proto = VariantTableStudyRowsProto.parseFrom(value);
                    System.out.println(GenomeHelper.VARIANT_COLUMN + " ts:" + cell.getTimestamp() + " value: " + proto);
                }
            }
            resultScanner.close();
            return null;
        });
        System.out.println("End query from archive HBase " + tableName);
    }

    @Test
    public void checkMeta() throws Exception {
        System.out.println("Get studies");
        List<String> studyNames = dbAdaptor.getStudyConfigurationManager().getStudyNames(new QueryOptions());
        assertEquals(1, studyNames.size());
        for (String studyName : studyNames) {
            System.out.println("studyName = " + studyName);
            StudyConfiguration sc = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyName, new QueryOptions()).first();
            assertEquals(sc.getStudyId(), STUDY_ID);
            assertEquals(sc.getStudyName(), STUDY_NAME);
            assertEquals(Collections.singleton(FILE_ID), sc.getIndexedFiles());
            System.out.println("sc = " + sc);
        }
    }
}
