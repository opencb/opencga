package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager.Options;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;

import java.net.URI;

/**
 * Created on 15/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopManagerTest extends HadoopVariantStorageManagerTestUtils {

    @Test
    public void testLoadGvcf() throws Exception {
        clearDB(DB_NAME);
        HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();

        URI inputUri = VariantStorageManagerTestUtils.getResourceUri("sample1.genome.vcf");

        StudyConfiguration studyConfiguration = VariantStorageManagerTestUtils.newStudyConfiguration();
        ETLResult etlResult = VariantStorageManagerTestUtils.runDefaultETL(inputUri, variantStorageManager, studyConfiguration,
                new ObjectMap(Options.TRANSFORM_FORMAT.key(), "avro")
                        .append(Options.ANNOTATE.key(), true)
                        .append(Options.CALCULATE_STATS.key(), false)
                        .append(HadoopVariantStorageManager.HADOOP_LOAD_ARCHIVE, true)
                        .append(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT, true)
        );

        VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);

        VariantSource source = variantStorageManager.readVariantSource(etlResult.transformResult, new ObjectMap());
        VariantGlobalStats stats = source.getStats();
        Assert.assertNotNull(stats);

        checkMeta(dbAdaptor);

        checkArchiveTable(dbAdaptor);
        checkVariantTable(dbAdaptor, stats);

        queryArchiveTable(studyConfiguration, dbAdaptor, stats);
        queryVariantTable(studyConfiguration, dbAdaptor);

    }

    public void queryVariantTable(StudyConfiguration studyConfiguration, VariantHadoopDBAdaptor dbAdaptor) {
        System.out.println("Query from Variant table");
        for (VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId()),
                new QueryOptions()); iterator.hasNext(); ) {
            Variant variant = iterator.next();
            System.out.println("Phoenix variant = " + variant);
        }
        System.out.println("End query from Analysis table");
    }

    public void queryArchiveTable(StudyConfiguration studyConfiguration, VariantHadoopDBAdaptor dbAdaptor, VariantGlobalStats stats) {
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
        Assert.assertEquals(stats.getNumRecords(), numVariants[0]);
    }

    public void checkVariantTable(VariantHadoopDBAdaptor dbAdaptor, VariantGlobalStats stats) throws java.io.IOException {
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
        Assert.assertEquals(stats.getVariantTypeCount(VariantType.SNP) + stats.getVariantTypeCount(VariantType.SNV), numVariants);
    }

    public void checkArchiveTable(VariantHadoopDBAdaptor dbAdaptor) throws java.io.IOException {
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

    public void checkMeta(VariantHadoopDBAdaptor dbAdaptor) {
        System.out.println("Get studies");
        for (String studyName : dbAdaptor.getStudyConfigurationManager().getStudyNames(new QueryOptions())) {
            System.out.println("studyName = " + studyName);
            StudyConfiguration sc = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyName, new QueryOptions()).first();
            System.out.println("sc = " + sc);
        }
    }
}
