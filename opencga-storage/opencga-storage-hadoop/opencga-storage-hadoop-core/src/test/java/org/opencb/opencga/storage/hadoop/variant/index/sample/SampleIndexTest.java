package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.CellBaseRestVariantAnnotator;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.nio.file.Paths;
import java.util.Collections;

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
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);

        runETL(getVariantStorageEngine(), smallInputUri, outputUri, params, true, true, true);

        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
    }

    @Test
    public void validateSampleIndexTest() throws Exception {

        HadoopVariantStorageEngine engine = getVariantStorageEngine();

        String copy = dbAdaptor.getTableNameGenerator().getSampleIndexTableName(1) + "_copy";

        dbAdaptor.getHBaseManager().createTableIfNeeded(copy, Bytes.toBytes(GenomeHelper.DEFAULT_COLUMN_FAMILY),
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