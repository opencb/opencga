package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration.Population;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.core.variant.query.Values;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexPutBuilder;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.RangeIndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleFileIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SingleSampleIndexQuery;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverterTest.*;

public class SampleIndexEntryFilterTest {

    private SampleIndexSchema schema;

    @Before
    public void setUp() throws Exception {
        SampleIndexConfiguration configuration = SampleIndexConfiguration.defaultConfiguration();
        configuration.getAnnotationIndexConfiguration().getPopulationFrequency().getPopulations().clear();
        schema = new SampleIndexSchema(configuration
                .addPopulation(new Population("s1", "ALL"))
                .addPopulation(new Population("s2", "ALL"))
                .addPopulation(new Population("s3", "ALL"))
                .addPopulation(new Population("s4", "ALL"))
                .addPopulation(new Population("s5", "ALL")));
    }

    @Test
    public void testPopFreqQuery() {
        List<String> result;
        SingleSampleIndexQuery query;

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR,
                buildPopulationFrequencyQuery("s2", "<", 0.001));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry1()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:10:A:T", "1:50:A:T"), result);
        Assert.assertEquals(2, new SampleIndexEntryFilter(query).filterAndCount(getSampleIndexEntry1()));

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR,
                buildPopulationFrequencyQuery("s3", "<", 0.01));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry1()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:20:A:T", "1:30:A:T", "1:50:A:T"), result);
        Assert.assertEquals(3, new SampleIndexEntryFilter(query).filterAndCount(getSampleIndexEntry1()));

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.AND,
                buildPopulationFrequencyQuery("s2", "<", 0.001),
                buildPopulationFrequencyQuery("s3", "<", 0.01));
        result = new SampleIndexEntryFilter(query).filter(getSampleIndexEntry1()).stream().map(Variant::toString).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList("1:50:A:T"), result);
        Assert.assertEquals(1, new SampleIndexEntryFilter(query).filterAndCount(getSampleIndexEntry1()));

        query = getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation.OR,
                buildPopulationFrequencyQuery("s2", "<", 0.001),
                buildPopulationFrequencyQuery("s3", "<", 0.01));
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
        AnnotationIndexConverter converter = new AnnotationIndexConverter(schema);
        //{0.001, 0.005, 0.01};
        Map<String, byte[]> map = new AnnotationIndexPutBuilder()
                .add(converter.convert(annot(
                        pf("s1", "ALL", 0.0),
                        pf("s2", "ALL", 0.0),
                        pf("s3", "ALL", 0.1),
                        pf("s4", "ALL", 0.0),
                        pf("s5", "ALL", 0.5)
                )))
                .add(converter.convert(annot(
                        pf("s1", "ALL", 0.0),
                        pf("s2", "ALL", 0.01),
                        pf("s3", "ALL", 0.005),
                        pf("s4", "ALL", 0.5),
                        pf("s5", "ALL", 0.5)
                )))
                .add(converter.convert(annot(
                        pf("s1", "ALL", 0.0),
                        pf("s2", "ALL", 0.01),
                        pf("s3", "ALL", 0.005),
                        pf("s4", "ALL", 0.1),
                        pf("s5", "ALL", 0.1)
                )))
                .add(converter.convert(annot(
                        pf("s1", "ALL", 0.0),
                        pf("s2", "ALL", 0.01),
                        pf("s3", "ALL", 0.1),
                        pf("s4", "ALL", 0.1),
                        pf("s5", "ALL", 0.1)
                )))
                .add(converter.convert(annot(
//                        pf("s1", "ALL", 0.0),
                        pf("s2", "ALL", 0.0001),
                        pf("s3", "ALL", 0.005),
                        pf("s4", "ALL", 0.1),
                        pf("s5", "ALL", 0.1)
                )))
                .buildAndReset(new Put(new byte[1]), "0/1", new byte[1])
                .getFamilyCellMap()
                .get(new byte[1])
                .stream()
                .collect(Collectors.toMap(cell -> Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil::cloneValue));



//        byte[] pf = new AnnotationIndexPutBuilder()                                              // s1 s2 s3 s4 s5
//                .add(new AnnotationIndexEntry((byte) 0, false, 0, 0, null, new BitBuffer(new byte[]{ 0, 0, 0, 0, 3 }), false, null))  // 1:10:A:T
//                .add(new AnnotationIndexEntry((byte) 0, false, 0, 0, null, new BitBuffer(new byte[]{ 0, 1, 2, 3, 3 }), false, null))  // 1:20:A:T
//                .add(new AnnotationIndexEntry((byte) 0, false, 0, 0, null, new BitBuffer(new byte[]{ 0, 2, 3, 1, 3 }), false, null))  // 1:30:A:T
//                .add(new AnnotationIndexEntry((byte) 0, false, 0, 0, null, new BitBuffer(new byte[]{ 0, 3, 3, 3, 3 }), false, null))  // 1:40:A:T
//                .add(new AnnotationIndexEntry((byte) 0, false, 0, 0, null, new BitBuffer(new byte[]{ 0, 1, 0, 3, 3 }), false, null))  // 1:50:A:T
//                .buildAndReset(new Put(new byte[1]), "0/1", new byte[1])
//                .getFamilyCellMap().get(new byte[1])
//                .stream()
//                .collect(Collectors.toMap(cell -> Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil::cloneValue)).get("_PF_0/1");

        SampleIndexEntry entry = new SampleIndexEntry(0, "1", 0);
        entry.getGtEntry("0/1")
//                .setAnnotationIndex(map.get("_A_0/1"))
//                .setCtBtIndex(map.get("_CB_0/1"))
//                .setConsequenceTypeIndex(map.get("_CT_0/1"))
//                .setBiotypeIndex(map.get("_BT_0/1"))
                .setPopulationFrequencyIndex(map.get("_PF_0/1"))
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
        AnnotationIndexConverter converter = new AnnotationIndexConverter(schema);

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


        SampleIndexEntry entry = new SampleIndexEntry(0, "1", 0);
        entry.getGtEntry("0/1")
                .setAnnotationIndex(map.get("_A_0/1"))
                .setCtBtIndex(map.get("_CB_0/1"))
                .setConsequenceTypeIndex(map.get("_CT_0/1"))
                .setBiotypeIndex(map.get("_BT_0/1"))
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
        return new SampleIndexVariantBiConverter(schema).toBytes(Arrays.stream(variants).map(Variant::new).collect(Collectors.toList()));
    }

    private RangeIndexFieldFilter buildPopulationFrequencyQuery(String study, String op, double value) {
        return (RangeIndexFieldFilter) schema.getPopFreqIndex().getField(study, "ALL").buildFilter(new OpValue<>(op, value));
    }

    private SingleSampleIndexQuery getSingleSampleIndexQuery(Query query) {
        VariantStorageMetadataManager metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        SampleIndexQueryParser parser = new SampleIndexQueryParser(metadataManager, SampleIndexSchema.defaultSampleIndexSchema());
        SampleAnnotationIndexQuery annotQuery = parser.parseAnnotationIndexQuery(query);

        return getSingleSampleIndexQuery(annotQuery);
    }

    private SingleSampleIndexQuery getSingleSampleIndexQuery(VariantQueryUtils.QueryOperation op, RangeIndexFieldFilter... frequencyQuery) {
        SampleAnnotationIndexQuery annotationIndexQuery = new SampleAnnotationIndexQuery(
                new byte[2],
                schema.getCtIndex().getField().noOpFilter(),
                schema.getBiotypeIndex().getField().noOpFilter(),
                schema.getCtBtIndex().getField().noOpFilter(),
                schema.getClinicalIndexSchema().noOpFilter(),
                schema.getPopFreqIndex().buildFilter(Arrays.asList(frequencyQuery), op));
        // TODO: what about Partial?
        return getSingleSampleIndexQuery(annotationIndexQuery);
    }

    private SingleSampleIndexQuery getSingleSampleIndexQuery(SampleAnnotationIndexQuery annotationIndexQuery) {
        return getSingleSampleIndexQuery(annotationIndexQuery, Collections.emptyMap());
    }

//    private SingleSampleIndexQuery getSingleSampleIndexQuery(Map<String, SampleFileIndexQuery> fileFilterMap) {
//        return getSingleSampleIndexQuery(null, fileFilterMap);
//    }

    private SingleSampleIndexQuery getSingleSampleIndexQuery(SampleAnnotationIndexQuery annotationIndexQuery, Map<String, Values<SampleFileIndexQuery>> fileFilterMap) {
        return new SampleIndexQuery(
                schema, Collections.emptyList(), null, "study", Collections.singletonMap("S1", Arrays.asList("0/1", "1/1")), Collections.emptySet(), null, Collections.emptyMap(), Collections.emptyMap(), fileFilterMap, annotationIndexQuery, Collections.emptySet(), false, VariantQueryUtils.QueryOperation.AND)
                .forSample("S1");
    }
}