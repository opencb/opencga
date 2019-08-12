package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexPutBuilder;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery.PopulationFrequencyQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleFileIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SingleSampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexConfiguration.PopulationFrequencyRange;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SampleIndexEntryFilterTest {

    private SampleIndexConfiguration configuration;

    @Before
    public void setUp() throws Exception {
        configuration = new SampleIndexConfiguration()
                .addPopulationRange(new PopulationFrequencyRange("s1", "ALL"))
                .addPopulationRange(new PopulationFrequencyRange("s2", "ALL"))
                .addPopulationRange(new PopulationFrequencyRange("s3", "ALL"))
                .addPopulationRange(new PopulationFrequencyRange("s4", "ALL"))
                .addPopulationRange(new PopulationFrequencyRange("s5", "ALL"));
    }

    @Test
    public void testPopFreqQueryOR() {
        List<String> result;
        SingleSampleIndexQuery query;

        // Partial OR query should always return ALL values
        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR, true, buildPopulationFrequencyQuery("s2", 0, 1));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T", "1:20:A:T", "1:30:A:T", "1:40:A:T", "1:50:A:T"), result);
        Assert.assertEquals(5, new SampleIndexEntryFilter(query, configuration).filterAndCount(getSampleIndexEntry()));

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR, false, buildPopulationFrequencyQuery("s2", 0, 1));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T"), result);
        Assert.assertEquals(1, new SampleIndexEntryFilter(query, configuration).filterAndCount(getSampleIndexEntry()));

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR, false,
                buildPopulationFrequencyQuery("s2", 0, 3),
                buildPopulationFrequencyQuery("s3", 0, 3));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T", "1:20:A:T", "1:30:A:T", "1:50:A:T"), result);
        Assert.assertEquals(4, new SampleIndexEntryFilter(query, configuration).filterAndCount(getSampleIndexEntry()));

        SampleIndexEntry e = getSampleIndexEntry();
        for (SampleIndexEntry.SampleIndexGtEntry value : e.getGts().values()) {
            value.setVariants(new SampleIndexVariantBiConverter().toVariantsCountIterator(value.getCount()));
        }
        Assert.assertEquals(4, new SampleIndexEntryFilter(query, configuration).filterAndCount(e));
    }

    @Test
    public void testPopFreqQueryAND() {
        SingleSampleIndexQuery query;
        List<String> result;
        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, true, buildPopulationFrequencyQuery("s2", 0, 1));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T"), result);


        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, false, buildPopulationFrequencyQuery("s2", 0, 1));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T"), result);


        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, false,
                buildPopulationFrequencyQuery("s2",  2, 4),
                buildPopulationFrequencyQuery("s3",  2, 4));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:30:A:T", "1:40:A:T"), result);

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, false,
                buildPopulationFrequencyQuery("s2", 0, 4),
                buildPopulationFrequencyQuery("s3", 1, 4),
                buildPopulationFrequencyQuery("s4", 2, 4));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:20:A:T", "1:40:A:T"), result);
    }

    private SampleIndexEntry getSampleIndexEntry() {
        byte[] pf = new AnnotationIndexPutBuilder()                                         // s1 s2 s3 s4 s5
                .add(new AnnotationIndexEntry((byte) 0, false, (short) 0, (byte) 0, new byte[]{ 0, 0, 0, 0, 3 }))  // 1:10:A:T
                .add(new AnnotationIndexEntry((byte) 0, false, (short) 0, (byte) 0, new byte[]{ 0, 1, 2, 3, 3 }))  // 1:20:A:T
                .add(new AnnotationIndexEntry((byte) 0, false, (short) 0, (byte) 0, new byte[]{ 0, 2, 3, 1, 3 }))  // 1:30:A:T
                .add(new AnnotationIndexEntry((byte) 0, false, (short) 0, (byte) 0, new byte[]{ 0, 3, 3, 3, 3 }))  // 1:40:A:T
                .add(new AnnotationIndexEntry((byte) 0, false, (short) 0, (byte) 0, new byte[]{ 0, 1, 0, 3, 3 }))  // 1:50:A:T
                .buildAndReset(new Put(new byte[1]), "0/1", new byte[1])
                .getFamilyCellMap().get(new byte[1])
                .stream()
                .collect(Collectors.toMap(cell -> Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil::cloneValue)).get("_PF_0/1");


        return new SampleIndexEntry(0, "1", 0, Collections.singletonMap("S1", new SampleIndexEntry.SampleIndexGtEntry("0/1")
                .setPopulationFrequencyIndexGt(pf)
                .setCount(5)
                .setVariants(buildIterator(
                        "1:10:A:T",
                        "1:20:A:T",
                        "1:30:A:T",
                        "1:40:A:T",
                        "1:50:A:T"
                        ))), null);
    }

    private SampleIndexVariantBiConverter.SampleIndexVariantIterator buildIterator(String... variants) {
        String str = String.join(",", variants);
        return new SampleIndexVariantBiConverter().toVariantsIterator("1", 0, Bytes.toBytes(str), 0, str.length());
    }

    private PopulationFrequencyQuery buildPopulationFrequencyQuery(String study, int minFreqInclusive, int maxFreqExclusive) {
        return new PopulationFrequencyQuery(Integer.valueOf(study.substring(1)) - 1, study, "ALL",
                -1, -1,
                (byte) minFreqInclusive,
                (byte) maxFreqExclusive);
    }

    private SingleSampleIndexQuery getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation op,
                                                             boolean partial,
                                                             PopulationFrequencyQuery... frequencyQuery) {
        SampleAnnotationIndexQuery annotationIndexQuery = new SampleAnnotationIndexQuery(new byte[2], (short) 0, (byte) 0,
                op,
                Arrays.asList(frequencyQuery),
                partial);
        return getSingleSampleIndexQuery(annotationIndexQuery);
    }

    private SingleSampleIndexQuery getSingleSampleIndexQuery(SampleAnnotationIndexQuery annotationIndexQuery) {
        return getSingleSampleIndexQuery(annotationIndexQuery, Collections.emptyMap());
    }

    private SingleSampleIndexQuery getSingleSampleIndexQuery(Map<String, SampleFileIndexQuery> fileFilterMap) {
        return getSingleSampleIndexQuery(null, fileFilterMap);
    }

    private SingleSampleIndexQuery getSingleSampleIndexQuery(SampleAnnotationIndexQuery annotationIndexQuery, Map<String, SampleFileIndexQuery> fileFilterMap) {
        return new SampleIndexQuery(
                Collections.emptyList(), null, "study", Collections.singletonMap("S1", Arrays.asList("0/1", "1/1")), null, Collections.emptyMap(), Collections.emptyMap(), fileFilterMap, annotationIndexQuery, Collections.emptySet(), false, VariantQueryUtils.QueryOperation.AND)
                .forSample("S1");
    }
}