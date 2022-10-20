package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexOnlyVariantQueryExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.OVERWRITE;

/**
 * Created on 12/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexDuplicatedVariantsTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    private VariantHadoopDBAdaptor dbAdaptor;
    private SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private static boolean loaded = false;

    @Before
    public void before() throws Exception {
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        sampleIndexDBAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor();
        if (!loaded) {
            load();
            loaded = true;
        }
    }

    public void load() throws Exception {
        clearDB(DB_NAME);
        StudyMetadata.SampleIndexConfigurationVersioned versioned;
        HadoopVariantStorageEngine engine = getVariantStorageEngine();

        // Study 1 - single file
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), true)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, getResourceUri("s1_2.vcf"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("s1.vcf"), outputUri, params, true, true, true);

        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());

    }

    @Test
    public void regenerateSampleIndex() throws Exception {
        SampleIndexOnlyVariantQueryExecutor queryExecutor = new SampleIndexOnlyVariantQueryExecutor(dbAdaptor, sampleIndexDBAdaptor, "", new ObjectMap());
        List<Variant> expectedVariants = new ArrayList<>();
        queryExecutor.iterator(new VariantQuery().sample("s1"), new QueryOptions()).forEachRemaining(expectedVariants::add);

        getVariantStorageEngine().sampleIndex(STUDY_NAME, Arrays.asList("s1"), new ObjectMap(OVERWRITE, true));
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());

        List<Variant> actualVariants = new ArrayList<>();
        queryExecutor.iterator(new VariantQuery().sample("s1"), new QueryOptions()).forEachRemaining(actualVariants::add);

        Assert.assertEquals(expectedVariants, actualVariants);
    }

}
