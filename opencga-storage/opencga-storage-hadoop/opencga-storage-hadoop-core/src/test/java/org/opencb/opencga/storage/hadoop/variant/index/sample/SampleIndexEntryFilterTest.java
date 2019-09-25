package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
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

import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverterTest.annot;
import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverterTest.ct;

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
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry1()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T", "1:20:A:T", "1:30:A:T", "1:40:A:T", "1:50:A:T"), result);
        Assert.assertEquals(5, new SampleIndexEntryFilter(query).filterAndCount(getSampleIndexEntry1()));

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR, false, buildPopulationFrequencyQuery("s2", 0, 1));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry1()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T"), result);
        Assert.assertEquals(1, new SampleIndexEntryFilter(query).filterAndCount(getSampleIndexEntry1()));

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR, false,
                buildPopulationFrequencyQuery("s2", 0, 3),
                buildPopulationFrequencyQuery("s3", 0, 3));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry1()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T", "1:20:A:T", "1:30:A:T", "1:50:A:T"), result);
        Assert.assertEquals(4, new SampleIndexEntryFilter(query).filterAndCount(getSampleIndexEntry1()));

        SampleIndexEntry e = getSampleIndexEntry1();
        for (SampleIndexEntry.SampleIndexGtEntry value : e.getGts().values()) {
//            value.setVariants(new SampleIndexVariantBiConverter().toVariantsCountIterator(value.getCount()));
            value.setCount(value.getCount());
        }
        Assert.assertEquals(4, new SampleIndexEntryFilter(query).filterAndCount(e));
    }

    @Test
    public void testPopFreqQueryAND() {
        SingleSampleIndexQuery query;
        List<String> result;
        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, true, buildPopulationFrequencyQuery("s2", 0, 1));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry1()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T"), result);


        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, false, buildPopulationFrequencyQuery("s2", 0, 1));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry1()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T"), result);


        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, false,
                buildPopulationFrequencyQuery("s2",  2, 4),
                buildPopulationFrequencyQuery("s3",  2, 4));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry1()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:30:A:T", "1:40:A:T"), result);

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND, false,
                buildPopulationFrequencyQuery("s2", 0, 4),
                buildPopulationFrequencyQuery("s3", 1, 4),
                buildPopulationFrequencyQuery("s4", 2, 4));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry1()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:20:A:T", "1:40:A:T"), result);
    }

    @Test
    public void testCtBtCombinationFilter() {
        SingleSampleIndexQuery query;
        List<String> result;

        query = getSingleSampleIndexQuery(new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding,nonsense_mediated_decay")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "stop_gained"));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry2()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:20:A:T"), result);

        query = getSingleSampleIndexQuery(new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "nonsense_mediated_decay")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost"));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry2()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T"), result);

        query = getSingleSampleIndexQuery(new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost"));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry2()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:20:A:T"), result);

        query = getSingleSampleIndexQuery(new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding,nonsense_mediated_decay")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "stop_lost"));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry2()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T", "1:20:A:T"), result);
    }

    private SampleIndexEntry getSampleIndexEntry1() {
        byte[] pf = new AnnotationIndexPutBuilder()                                         // s1 s2 s3 s4 s5
                .add(new AnnotationIndexEntry((byte) 0, false, (short) 0, (byte) 0, new byte[]{ 0, 0, 0, 0, 3 }, new byte[0]))  // 1:10:A:T
                .add(new AnnotationIndexEntry((byte) 0, false, (short) 0, (byte) 0, new byte[]{ 0, 1, 2, 3, 3 }, new byte[0]))  // 1:20:A:T
                .add(new AnnotationIndexEntry((byte) 0, false, (short) 0, (byte) 0, new byte[]{ 0, 2, 3, 1, 3 }, new byte[0]))  // 1:30:A:T
                .add(new AnnotationIndexEntry((byte) 0, false, (short) 0, (byte) 0, new byte[]{ 0, 3, 3, 3, 3 }, new byte[0]))  // 1:40:A:T
                .add(new AnnotationIndexEntry((byte) 0, false, (short) 0, (byte) 0, new byte[]{ 0, 1, 0, 3, 3 }, new byte[0]))  // 1:50:A:T
                .buildAndReset(new Put(new byte[1]), "0/1", new byte[1])
                .getFamilyCellMap().get(new byte[1])
                .stream()
                .collect(Collectors.toMap(cell -> Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil::cloneValue)).get("_PF_0/1");

        SampleIndexEntry entry = new SampleIndexEntry(0, "1", 0, configuration);
        entry.getGtEntry("0/1")
                .setPopulationFrequencyIndexGt(pf)
                .setCount(5)
                .setVariants(toBytes(
                        "1:10:A:T",
                        "1:20:A:T",
                        "1:30:A:T",
                        "1:40:A:T",
                        "1:50:A:T"
                        ));
        return entry;
    }

    private SampleIndexEntry getSampleIndexEntry2() {
        AnnotationIndexConverter converter = new AnnotationIndexConverter(SampleIndexConfiguration.defaultConfiguration());

        Map<String, byte[]> map = new AnnotationIndexPutBuilder()
                .add(converter.convert(annot(
                        ct("missense_variant", "protein_coding"),
                        ct("start_lost", "nonsense_mediated_decay"),
                        ct("start_lost", "protein_coding"),
                        ct("stop_lost", "nonsense_mediated_decay"),
                        ct("stop_gained", "other"))))
                .add(converter.convert(annot(
                        ct("missense_variant", "protein_coding"),
                        ct("start_lost", "nonsense_mediated_decay"),
                        ct("start_lost", "protein_coding"),
                        ct("stop_lost", "protein_coding"),
                        ct("stop_gained", "nonsense_mediated_decay"))))
                .buildAndReset(new Put(new byte[1]), "0/1", new byte[1])
                .getFamilyCellMap()
                .get(new byte[1])
                .stream()
                .collect(Collectors.toMap(cell -> Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil::cloneValue));


        SampleIndexEntry entry = new SampleIndexEntry(0, "1", 0, configuration);
        entry.getGtEntry("0/1")
                .setAnnotationIndexGt(map.get("_A_0/1"))
                .setCtBtIndexGt(map.get("_CB_0/1"))
                .setConsequenceTypeIndexGt(map.get("_CT_0/1"))
                .setBiotypeIndexGt(map.get("_BT_0/1"))
                .setCount(5)
                .setVariants(toBytes(
                        "1:10:A:T",
                        "1:20:A:T"
//                        "1:30:A:T",
//                        "1:40:A:T",
//                        "1:50:A:T"
                ));
        return entry;
    }

    private byte[] toBytes(String... variants) {
        String str = String.join(",", variants);
        return Bytes.toBytes(str);
    }

    private PopulationFrequencyQuery buildPopulationFrequencyQuery(String study, int minFreqInclusive, int maxFreqExclusive) {
        return new PopulationFrequencyQuery(Integer.valueOf(study.substring(1)) - 1, study, "ALL",
                -1, -1,
                (byte) minFreqInclusive,
                (byte) maxFreqExclusive);
    }

    private SingleSampleIndexQuery getSingleSampleIndexQuery(Query query) {
        SampleIndexQueryParser parser = new SampleIndexQueryParser(new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory()));
        SampleAnnotationIndexQuery annotQuery = parser.parseAnnotationIndexQuery(query);

        return getSingleSampleIndexQuery(annotQuery);
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