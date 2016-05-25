package org.opencb.opencga.storage.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableMapper;

public class GenomeVariantTransformMapperTest {
    private VariantTableMapper mapper;
    private MapDriver<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put> mapDriver;

    public static class TestHelper extends VariantTableHelper {

        public TestHelper(Configuration conf) {
            super(conf);
        }

    }


//    @Before
//    public void setUp() throws Exception {
//        final VariantCallMeta variantCallMeta = new VariantCallMeta();
//        variantCallMeta.addSample("a", 1, HBaseZeroCopyByteString.copyFrom(Bytes.toBytes("a")));
//        mapper = new VariantTableMapper(){
//            @Override
//            protected VariantTableHelper loadHelper(
//                    Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) {
//                return new TestHelper(context.getConfiguration());
//            }
//
//            @Override
//            protected VariantCallMeta loadMeta() throws IOException {
//                return variantCallMeta;
//            }
//
//            @Override
//            protected void initVcfMetaMap(Configuration conf) throws IOException {
//                // do nothing
//            }
//
//        };
//        mapDriver = MapDriver.newMapDriver(mapper);
//        Configuration config = mapDriver.getConfiguration();
//        setUpConfig(config);
//
//    }
//
//    public void setUpConfig(Configuration conf) throws IOException {
////        GenomeVariantHelper.setChunkSize(conf, 1);
////        GenomeVariantHelper.setMetaProtoFile(conf, "VCF_To_Avro_Conversion/vcfmeta.proto");
//
////        AvroSerialization.addToConfiguration(conf);
////        AvroSerialization.setKeyReaderSchema(conf, VariantAvro.SCHEMA$);
////        AvroSerialization.setKeyWriterSchema(conf, VariantAvro.SCHEMA$);
//
//
////        String metaStr = metaConvert(new File("VCF_To_Avro_Conversion/out.avro.meta"), null);
////        GenomeVariantHelper.setMetaProtoString(conf, metaStr);
//
//        conf.setStrings("io.serializations", conf.get("io.serializations"), MutationSerialization.class.getName(),
//                ResultSerialization.class.getName());
//
//        conf.setStrings(VariantTableHelper.OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT, "TEST-out");
//        conf.setStrings(VariantTableHelper.OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT, "TEST-in");
//    }
//
//    @After
//    public void tearDown() throws Exception {
//    }
//
//    @Test
//    public void testMapImmutableBytesWritableResultContext() throws IOException {
//        ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes("1_123"));
//
//        VcfSample.Builder sampleBuilder = VcfSample.newBuilder()
//                .addSampleValues("0/1");
//
//        VcfRecord.Builder recordBuilder = VcfRecord.newBuilder()
//                .setRelativeStart(10)
//                .setReference("A")
//                .addAlternate("T")
//                .addSampleFormatNonDefault("GT")
//                .addSamples(sampleBuilder.build());
//
//        VcfSlice.Builder vcfSliceBuilder = VcfSlice.newBuilder()
//                .setChromosome("1")
//                .setPosition(12300)
//                .addRecords(recordBuilder.build());
//
//        byte[] vcf = vcfSliceBuilder.build().toByteArray();
//
//        KeyValue cell = new KeyValue(key.get(), Bytes.toBytes(VariantTableHelper.DEFAULT_COLUMN_FAMILY), Bytes.toBytes("a"), vcf);
//        List<Cell> cellLst = Arrays.asList(cell);
//        Result val = Result.create(cellLst);
//        mapDriver.setInput(key, val);
//
//        ImmutableBytesWritable oKey = new ImmutableBytesWritable(Bytes.toBytes("1_000000012310_A_T"));
//        Put oVal = new Put(Bytes.toBytes("1_000000012310_A_T"));
//
//        mapDriver.withOutput(oKey, oVal);
//        mapDriver.setValueComparator((o1, o2) -> Bytes.compareTo(o1.getRow(), o2.getRow()));
//        mapDriver.runTest();
//    }

}
