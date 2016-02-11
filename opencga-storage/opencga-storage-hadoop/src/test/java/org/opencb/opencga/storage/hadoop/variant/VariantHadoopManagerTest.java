package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager.Options;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created on 15/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopManagerTest extends HadoopVariantStorageManagerTestUtils {

    private VariantHadoopDBAdaptor dbAdaptor;
    private static StudyConfiguration studyConfiguration;
    private static VariantSource source;
    private static ETLResult etlResult = null;

    @Before
    public void before() throws Exception {
        if (etlResult == null) {
            clearDB(DB_NAME);
            HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();

            URI inputUri = VariantStorageManagerTestUtils.getResourceUri("sample1.genome.vcf");

            studyConfiguration = VariantStorageManagerTestUtils.newStudyConfiguration();
            etlResult = VariantStorageManagerTestUtils.runDefaultETL(inputUri, variantStorageManager, studyConfiguration,
                    new ObjectMap(Options.TRANSFORM_FORMAT.key(), "avro")
                            .append(Options.ANNOTATE.key(), true)
                            .append(Options.CALCULATE_STATS.key(), false)
                            .append(HadoopVariantStorageManager.HADOOP_LOAD_ARCHIVE, true)
                            .append(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT, true)
            );


            source = variantStorageManager.readVariantSource(etlResult.transformResult, new ObjectMap());
            VariantGlobalStats stats = source.getStats();
            Assert.assertNotNull(stats);
        }
        HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();
        dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);

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
        assertEquals(totalCount, partialCount1 + partialCount2);
    }

    @Test
    public void getVariantByGene() {

        // Group by Gene
        HashMap<String, Long> genesCount = new HashMap<>();
        for (Variant variant : dbAdaptor) {
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
        System.out.println("Query from Archive table");
        dbAdaptor.iterator(
                new Query("archive", true)
                        .append(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId())
                        .append(VariantDBAdaptor.VariantQueryParams.FILES.key(), FILE_ID),
                new QueryOptions()).forEachRemaining(variant -> {
            System.out.println("Variant from archive = " + variant);
            numVariants[0]++;
        });
        System.out.println("End query from Archive table");
        assertEquals(source.getStats().getNumRecords(), numVariants[0]);
    }

    @Test
    public void checkVariantTable() throws java.io.IOException {
        System.out.println("Query from HBase : " + DB_NAME);
        HBaseManager hm = new HBaseManager(configuration.get());
        GenomeHelper genomeHelper = dbAdaptor.getGenomeHelper();
        int numVariants = hm.act(DB_NAME, table -> {
            int num = 0;
            ResultScanner resultScanner = table.getScanner(genomeHelper.getColumnFamily());
            for (Result result : resultScanner) {
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
        assertEquals(source.getStats().getVariantTypeCount(VariantType.SNP) + source.getStats().getVariantTypeCount(VariantType.SNV), numVariants);
    }

    @Test
    public void checkArchiveTable() throws java.io.IOException {
        String tableName = ArchiveHelper.getTableName(STUDY_ID);
        System.out.println("Query from archive HBase " + tableName);
        HBaseManager hm = new HBaseManager(configuration.get());
        GenomeHelper genomeHelper = dbAdaptor.getGenomeHelper();
        hm.act(tableName, table -> {
            ResultScanner resultScanner = table.getScanner(genomeHelper.getColumnFamily());
            for (Result result : resultScanner) {
                System.out.println("VcfSlice = " + Bytes.toString(result.getRow()));
            }
            resultScanner.close();
            return null;
        });
        System.out.println("End query from archive HBase " + tableName);
    }

    @Test
    public void checkMeta() {
        System.out.println("Get studies");
        for (String studyName : dbAdaptor.getStudyConfigurationManager().getStudyNames(new QueryOptions())) {
            System.out.println("studyName = " + studyName);
            StudyConfiguration sc = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyName, new QueryOptions()).first();
            System.out.println("sc = " + sc);
        }
    }
}
