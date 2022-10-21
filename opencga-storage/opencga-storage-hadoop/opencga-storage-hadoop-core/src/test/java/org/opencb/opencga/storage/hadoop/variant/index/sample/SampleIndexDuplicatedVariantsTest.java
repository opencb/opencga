package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.TableName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
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

    @Before
    public void before() throws Exception {
        clearDB(DB_NAME);
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        sampleIndexDBAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getSampleIndexDBAdaptor();
    }

    @Test
    public void test2FilesSampleIndex() throws Exception {
        HadoopVariantStorageEngine engine = getVariantStorageEngine();

        // Study 1 - single file
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), true)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, getResourceUri("duplicated/s1.vcf"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("duplicated/s1_2.vcf"), outputUri, params, true, true, true);
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());

        SampleIndexOnlyVariantQueryExecutor queryExecutor = new SampleIndexOnlyVariantQueryExecutor(dbAdaptor, sampleIndexDBAdaptor, "", new ObjectMap());
        List<Variant> expectedVariants = new ArrayList<>();
        queryExecutor.iterator(new VariantQuery().sample("s1"), new QueryOptions()).forEachRemaining(expectedVariants::add);

        int studyId = engine.getMetadataManager().getStudyId(STUDY_NAME);
        String actualSampleIndexTableName = sampleIndexDBAdaptor.getSampleIndexTableNameLatest(studyId);
        String expectedSampleIndexTableName = actualSampleIndexTableName + "_expected";
        dbAdaptor.getHBaseManager().act(actualSampleIndexTableName, (table, admin) -> {
            admin.snapshot(actualSampleIndexTableName + "_SNAPSHOT", TableName.valueOf(actualSampleIndexTableName));
            admin.cloneSnapshot(actualSampleIndexTableName + "_SNAPSHOT", TableName.valueOf(expectedSampleIndexTableName));
            admin.deleteSnapshot(actualSampleIndexTableName + "_SNAPSHOT");
            admin.disableTable(table.getName());
            admin.deleteTable(table.getName());
            return null;
        });

        engine.sampleIndex(STUDY_NAME, Arrays.asList("s1"), new ObjectMap(OVERWRITE, true));
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());

        List<Variant> actualVariants = new ArrayList<>();
        queryExecutor.iterator(new VariantQuery().sample("s1"), new QueryOptions()).forEachRemaining(actualVariants::add);

        Assert.assertEquals(expectedVariants, actualVariants);

        assertEqualTables(dbAdaptor.getConnection(), expectedSampleIndexTableName, actualSampleIndexTableName);
    }

    @Test
    public void test3FilesSampleIndex() throws Exception {
        HadoopVariantStorageEngine engine = getVariantStorageEngine();

        // Study 1 - single file
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), true)
                .append(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);
        runETL(engine, getResourceUri("duplicated/s1.vcf"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("duplicated/s1_2.vcf"), outputUri, params, true, true, true);
        runETL(engine, getResourceUri("duplicated/s1_3.vcf"), outputUri, params, true, true, true);
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());

        SampleIndexOnlyVariantQueryExecutor queryExecutor = new SampleIndexOnlyVariantQueryExecutor(dbAdaptor, sampleIndexDBAdaptor, "", new ObjectMap());
        List<Variant> expectedVariants = new ArrayList<>();
        queryExecutor.iterator(new VariantQuery().sample("s1"), new QueryOptions()).forEachRemaining(expectedVariants::add);

        int studyId = engine.getMetadataManager().getStudyId(STUDY_NAME);
        String actualSampleIndexTableName = sampleIndexDBAdaptor.getSampleIndexTableNameLatest(studyId);
        String expectedSampleIndexTableName = actualSampleIndexTableName + "_expected";
        dbAdaptor.getHBaseManager().act(actualSampleIndexTableName, (table, admin) -> {
            admin.snapshot(actualSampleIndexTableName + "_SNAPSHOT", TableName.valueOf(actualSampleIndexTableName));
            admin.cloneSnapshot(actualSampleIndexTableName + "_SNAPSHOT", TableName.valueOf(expectedSampleIndexTableName));
            admin.deleteSnapshot(actualSampleIndexTableName + "_SNAPSHOT");
            admin.disableTable(table.getName());
            admin.deleteTable(table.getName());
            return null;
        });

        engine.sampleIndex(STUDY_NAME, Arrays.asList("s1"), new ObjectMap(OVERWRITE, true));
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());

        List<Variant> actualVariants = new ArrayList<>();
        queryExecutor.iterator(new VariantQuery().sample("s1"), new QueryOptions()).forEachRemaining(actualVariants::add);

        Assert.assertEquals(expectedVariants, actualVariants);

        assertEqualTables(dbAdaptor.getConnection(), expectedSampleIndexTableName, actualSampleIndexTableName);
    }

}
