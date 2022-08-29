package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils.printVariants;

/**
 * Created on 26/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStorageEngineArchiveTableTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();
    private static VariantHadoopDBAdaptor dbAdaptor;

    @BeforeClass
    public static void beforeClass() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = externalResource.getVariantStorageEngine();

        ObjectMap extraParams = new ObjectMap(VariantStorageOptions.LOAD_HOM_REF.key(), true);
//        extraParams.append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "proto");
//        extraParams.append(VariantStorageOptions.GVCF.key(), true);

        runETL(variantStorageManager, getResourceUri("impact/HG005_GRCh38_1_22_v4.2.1_benchmark.tuned.chr6-31.vcf.gz"), STUDY_NAME, extraParams);
        runETL(variantStorageManager, getResourceUri("impact/HG006_GRCh38_1_22_v4.2.1_benchmark.tuned.chr6-31.vcf.gz"), STUDY_NAME, extraParams);
        runETL(variantStorageManager, getResourceUri("impact/HG007_GRCh38_1_22_v4.2.1_benchmark.tuned.chr6-31.vcf.gz"), STUDY_NAME, extraParams);
        runETL(variantStorageManager, getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"), STUDY_NAME, extraParams);
        runETL(variantStorageManager, getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.vcf.gz"), STUDY_NAME, extraParams);
        runETL(variantStorageManager, getResourceUri("variant-test-sv.vcf"), STUDY_NAME, extraParams);

        dbAdaptor = variantStorageManager.getDBAdaptor();
        printVariants(dbAdaptor, newOutputUri());

//        variantStorageManager.aggregateFamily(STUDY_NAME, new VariantAggregateFamilyParams()
//                .setSamples(Arrays.asList("HG005", "HG006", "HG007")), new ObjectMap());
//        printVariants(dbAdaptor, newOutputUri());
    }


    @Before
    @Override
    public void before() throws Exception {
        dbAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getDBAdaptor();
    }

    @Test
    public void queryArchiveTable() throws StorageEngineException {
        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();
        int studyId = mm.getStudyId(STUDY_NAME);
        Iterator<FileMetadata> it = mm.fileMetadataIterator(studyId);
        while (it.hasNext()) {
            FileMetadata fileMetadata = it.next();
            final int[] numVariants = {0};
            Map<VariantType, Long> variantCounts = new HashMap<>();
            Map<String, Long> variantChromosomeCounts = new HashMap<>();
            System.out.println("Query from Archive table " + fileMetadata.getName());
            dbAdaptor.archiveIterator(STUDY_NAME, fileMetadata.getName(), new Query(), new QueryOptions())
                    .forEachRemaining(variant -> {
//                        System.out.println("Variant from archive = " + variant);
                        numVariants[0]++;
                        variantCounts.merge(variant.getType(), 1L, Long::sum);
                        variantChromosomeCounts.merge(variant.getChromosome(), 1L, Long::sum);
                    });
            System.out.println("End query from Archive table");
            VariantSetStats stats = mm.getVariantFileMetadata(studyId, fileMetadata.getId(), null).first().getStats();
            stats.getChromosomeCount().forEach((s, l) -> assertEquals("chromosome : " + s, l, variantChromosomeCounts.getOrDefault(s, 0L)));
            stats.getTypeCount().forEach((s, l) -> assertEquals("variant type : " + s, l, variantCounts.getOrDefault(VariantType.valueOf(s), 0L)));
            assertEquals(stats.getVariantCount().intValue(), numVariants[0]);
            assertTrue(fileMetadata.getAttributes().getBoolean(VariantStorageOptions.LOAD_ARCHIVE.key()));
        }
    }
}
