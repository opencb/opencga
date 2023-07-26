package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.query.LocusQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by jacobo on 21/03/19.
 */
@Category(ShortTests.class)
public class SampleIndexDBAdaptorTest {

    private VariantStorageMetadataManager metadataManager;
    private int studyId;

    @Before
    public void setUp() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        studyId = metadataManager.createStudy("ST").getId();
    }

    @Test
    public void testSampleIdFF() throws Exception {
        int sampleId = 0xFF;
        String sampleName = "FF";
        metadataManager.unsecureUpdateSampleMetadata(studyId, new SampleMetadata(studyId, sampleId, sampleName));

        SampleIndexQuery query = new SampleIndexQuery(SampleIndexSchema.defaultSampleIndexSchema(), Collections.emptyList(), 0, null, "ST",
                Collections.singletonMap(sampleName, Collections.singletonList("0/1")), Collections.emptySet(), null, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), new SampleAnnotationIndexQuery(SampleIndexSchema.defaultSampleIndexSchema()),
                Collections.emptySet(), null, false, VariantQueryUtils.QueryOperation.AND);
        new SampleIndexDBAdaptor(new HBaseManager(new Configuration()), null, metadataManager).parse(query.forSample(sampleName), null);
    }

    @Test
    public void testSplitRegion() {
        Region region = new Region("1", 11001000, 16400000);
        List<LocusQuery> split = SampleIndexDBAdaptor.splitLocusQuery(
                new LocusQuery(
                        SampleIndexSchema.getChunkRegion(region, 3000000),
                        Collections.singletonList(region),
                        Collections.emptyList()));
        // Check region is not modified
        Assert.assertEquals("1:11001000-16400000", region.toString());
        Assert.assertEquals(Arrays.asList(
                new LocusQuery(new Region("1", 8000000, 12000000), Collections.singletonList(new Region("1", 11001000, 11999999)), Collections.emptyList()),
                new LocusQuery(new Region("1", 12000000, 16000000), Collections.singletonList(new Region("1", 12000000, 15999999)), Collections.emptyList()),
                new LocusQuery(new Region("1", 16000000, 17000000), Collections.singletonList(new Region("1", 16000000, 16400000)), Collections.emptyList())),
                split);
        Assert.assertFalse(SampleIndexDBAdaptor.startsAtBatch(split.get(0).getRegions().get(0)));
        Assert.assertTrue(SampleIndexDBAdaptor.endsAtBatch(split.get(0).getRegions().get(0)));
        Assert.assertTrue(SampleIndexDBAdaptor.startsAtBatch(split.get(1).getRegions().get(0)));
        Assert.assertTrue(SampleIndexDBAdaptor.endsAtBatch(split.get(1).getRegions().get(0)));
        Assert.assertTrue(SampleIndexDBAdaptor.startsAtBatch(split.get(2).getRegions().get(0)));
        Assert.assertFalse(SampleIndexDBAdaptor.endsAtBatch(split.get(2).getRegions().get(0)));

        region = new Region("1", 1000000, 16400000);
        split = SampleIndexDBAdaptor.splitLocusQuery(new LocusQuery(SampleIndexSchema.getChunkRegion(region, 0), Collections.singletonList(region), Collections.emptyList()));
        Assert.assertEquals(Arrays.asList(
                new LocusQuery(new Region("1", 1000000, 16000000), Collections.singletonList(new Region("1", 1000000, 15999999)), Collections.emptyList()),
                new LocusQuery(new Region("1", 16000000, 17000000), Collections.singletonList(new Region("1", 16000000, 16400000)), Collections.emptyList())),
                split);
        Assert.assertTrue(SampleIndexDBAdaptor.startsAtBatch(split.get(0).getRegions().get(0)));
        Assert.assertTrue(SampleIndexDBAdaptor.endsAtBatch(split.get(0).getRegions().get(0)));
        Assert.assertTrue(SampleIndexDBAdaptor.startsAtBatch(split.get(1).getRegions().get(0)));
        Assert.assertFalse(SampleIndexDBAdaptor.endsAtBatch(split.get(1).getRegions().get(0)));
    }

}