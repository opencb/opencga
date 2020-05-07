package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by jacobo on 21/03/19.
 */
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

        SampleIndexQuery query = new SampleIndexQuery(Collections.emptyList(), "ST",
                Collections.singletonMap(sampleName, Collections.singletonList("0/1")), VariantQueryUtils.QueryOperation.AND);
        new SampleIndexDBAdaptor(new HBaseManager(new Configuration()), null, metadataManager).parse(query.forSample(sampleName), null);
    }

    @Test
    public void testSplitRegion() {
        Region region = new Region("1", 1000, 16400000);
        List<Region> split = SampleIndexDBAdaptor.splitRegion(region);
        // Check region is not modified
        Assert.assertEquals("1:1000-16400000", region.toString());
        Assert.assertEquals(Arrays.asList(
                new Region("1", 1000, 999999),
                new Region("1", 1000000, 15999999),
                new Region("1", 16000000, 16400000)),
                split);
        Assert.assertFalse(SampleIndexDBAdaptor.startsAtBatch(split.get(0)));
        Assert.assertTrue(SampleIndexDBAdaptor.endsAtBatch(split.get(0)));
        Assert.assertTrue(SampleIndexDBAdaptor.startsAtBatch(split.get(1)));
        Assert.assertTrue(SampleIndexDBAdaptor.endsAtBatch(split.get(1)));
        Assert.assertTrue(SampleIndexDBAdaptor.startsAtBatch(split.get(2)));
        Assert.assertFalse(SampleIndexDBAdaptor.endsAtBatch(split.get(2)));

        split = SampleIndexDBAdaptor.splitRegion(new Region("1", 1000000, 16400000));
        Assert.assertEquals(Arrays.asList(
                new Region("1", 1000000, 15999999),
                new Region("1", 16000000, 16400000)),
                split);
        Assert.assertTrue(SampleIndexDBAdaptor.startsAtBatch(split.get(0)));
        Assert.assertTrue(SampleIndexDBAdaptor.endsAtBatch(split.get(0)));
        Assert.assertTrue(SampleIndexDBAdaptor.startsAtBatch(split.get(1)));
        Assert.assertFalse(SampleIndexDBAdaptor.endsAtBatch(split.get(1)));
    }

}