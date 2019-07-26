package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexPutBuilder;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexConfiguration.PopulationFrequencyRange;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery.PopulationFrequencyQuery;

import java.util.*;
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
        SampleIndexQuery.SingleSampleIndexQuery query;

        // Partial OR query should always return ALL values
        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR, true, buildPopulationFrequencyQuery("s2", 0, 0.001));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T", "1:20:A:T", "1:30:A:T", "1:40:A:T", "1:50:A:T"), result);

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR, false, buildPopulationFrequencyQuery("s2", 0, 0.001));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T"), result);

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR, false,
                buildPopulationFrequencyQuery("s2", 0, 0.006),
                buildPopulationFrequencyQuery("s3", 0, 0.006));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T", "1:20:A:T", "1:30:A:T", "1:50:A:T"), result);
    }

    @Test
    public void testPopFreqQueryAND() {
        SampleIndexQuery.SingleSampleIndexQuery query;
        List<String> result;
        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, true, buildPopulationFrequencyQuery("s2", 0, 0.001));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T"), result);


        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, false, buildPopulationFrequencyQuery("s2", 0, 0.001));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T"), result);


        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, false,
                buildPopulationFrequencyQuery("s2", 0.006, 1),
                buildPopulationFrequencyQuery("s3", 0.006, 1));
        result = new SampleIndexEntryFilter(query, configuration).filter(getSampleIndexEntry()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:30:A:T", "1:40:A:T"), result);

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, false,
                buildPopulationFrequencyQuery("s2", 0, 1),
                buildPopulationFrequencyQuery("s3", 0.002, 1),
                buildPopulationFrequencyQuery("s4", 0.006, 1));
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


        return new SampleIndexEntry("1", 0, Collections.singletonMap("S1", new SampleIndexEntry.SampleIndexGtEntry("0/1")
                .setPopulationFrequencyIndexGt(pf)
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

    private PopulationFrequencyQuery buildPopulationFrequencyQuery(String study, double minFreqInclusive, double maxFreqExclusive) {
        return new PopulationFrequencyQuery(Integer.valueOf(study.substring(1)) - 1, study, "ALL",
                minFreqInclusive, maxFreqExclusive,
                IndexUtils.getRangeCode(minFreqInclusive, PopulationFrequencyRange.DEFAULT_RANGES),
                IndexUtils.getRangeCodeExclusive(maxFreqExclusive, PopulationFrequencyRange.DEFAULT_RANGES));
    }

    private SampleIndexQuery.SingleSampleIndexQuery getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation op,
                                                                              boolean partial,
                                                                              PopulationFrequencyQuery... frequencyQuery) {
        SampleAnnotationIndexQuery annotationIndexQuery = new SampleAnnotationIndexQuery(new byte[2], (short) 0, (byte) 0,
                op,
                Arrays.asList(frequencyQuery),
                partial);
        return getSingleSampleIndexQuery(annotationIndexQuery);
    }

    private SampleIndexQuery.SingleSampleIndexQuery getSingleSampleIndexQuery(SampleAnnotationIndexQuery annotationIndexQuery) {
        return getSingleSampleIndexQuery(annotationIndexQuery, Collections.emptyMap());
    }

    private SampleIndexQuery.SingleSampleIndexQuery getSingleSampleIndexQuery(Map<String, byte[]> fileFilterMap) {
        return getSingleSampleIndexQuery(null, fileFilterMap);
    }

    private SampleIndexQuery.SingleSampleIndexQuery getSingleSampleIndexQuery(SampleAnnotationIndexQuery annotationIndexQuery, Map<String, byte[]> fileFilterMap) {
        return new SampleIndexQuery(
                Collections.emptyList(), null, "study", Collections.singletonMap("S1", Arrays.asList("0/1", "1/1")), Collections.emptyMap(), Collections.emptyMap(), fileFilterMap, annotationIndexQuery, Collections.emptySet(), false, VariantQueryUtils.QueryOperation.AND)
                .forSample("S1");
    }
}