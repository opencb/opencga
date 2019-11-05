package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.CellBaseRestVariantAnnotator;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.*;

/**
 * Created on 12/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    private static boolean loaded = false;
    private VariantHadoopDBAdaptor dbAdaptor;

    @Before
    public void before() throws Exception {
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        if (!loaded) {
            load();
            loaded = true;
        }
    }

    public void load() throws Exception {
        clearDB(DB_NAME);

        ObjectMap params = new ObjectMap(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.FAMILY)
                .append(VariantStorageEngine.Options.STUDY.key(), "study")
                .append(VariantStorageEngine.Options.ANNOTATE.key(), true)
                .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "DS,GL")
                .append(VariantAnnotationManager.VARIANT_ANNOTATOR_CLASSNAME, CellBaseRestVariantAnnotator.class.getName())
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false);

        runETL(getVariantStorageEngine(), smallInputUri, outputUri, params, true, true, true);

        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
    }

    @Test
    public void checkLoadedData() throws Exception {
        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        Iterator<SampleMetadata> it = variantStorageEngine.getMetadataManager().sampleMetadataIterator(1);
        while (it.hasNext()) {
            SampleMetadata sample = it.next();
            Iterator<SampleIndexEntry> indexIt = variantStorageEngine.getSampleIndexDBAdaptor().rawIterator(1, sample.getId());
            while (indexIt.hasNext()) {
                SampleIndexEntry record = indexIt.next();

                List<Variant> variants = variantStorageEngine.getDBAdaptor().get(new Query()
                        .append(VariantQueryParam.SAMPLE.key(), sample.getName())
                        .append(VariantQueryParam.REGION.key(),
                                record.getChromosome() + ":" + record.getBatchStart() + "-" + (record.getBatchStart() + SampleIndexSchema.BATCH_SIZE - 1)), null).getResults();

                Map<String, List<String>> gtsMap = variants.stream().collect(groupingBy(v -> v.getStudies().get(0).getSampleData(0).get(0), mapping(Variant::toString, toList())));
//                System.out.println("record = " + record);

                Assert.assertEquals(gtsMap.keySet(), record.getGts().keySet());
                for (Map.Entry<String, SampleIndexEntry.SampleIndexGtEntry> entry : record.getGts().entrySet()) {
                    String gt = entry.getKey();
                    List<String> expectedVariants = gtsMap.get(gt);
                    List<String> actualVariants;
                    if (entry.getValue().getVariants() == null) {
                        actualVariants = Collections.emptyList();
                    } else {
                        actualVariants = Lists.newArrayList(entry.getValue().getVariants())
                                .stream()
                                .map(Variant::toString)
                                .collect(toList());
                    }
                    Assert.assertEquals(gt, expectedVariants, actualVariants);
                }

            }
        }

    }

    @Test
    public void regenerateSampleIndex() throws Exception {

        String copy = dbAdaptor.getTableNameGenerator().getSampleIndexTableName(1) + "_copy";

        dbAdaptor.getHBaseManager().createTableIfNeeded(copy, Bytes.toBytes(GenomeHelper.COLUMN_FAMILY),
                Compression.Algorithm.NONE);

        ObjectMap options = new ObjectMap()
                .append(SampleIndexDriver.OUTPUT, copy)
                .append(SampleIndexDriver.SAMPLES, "all");
        new TestMRExecutor().run(SampleIndexDriver.class, SampleIndexDriver.buildArgs(
                dbAdaptor.getArchiveTableName(1),
                dbAdaptor.getVariantTable(),
                1,
                Collections.emptySet(), options), options);

        VariantHbaseTestUtils.printSampleIndexTable(dbAdaptor, Paths.get(newOutputUri()), copy);

    }
}