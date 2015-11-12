package org.opencb.opencga.storage.hadoop.mr;

import com.google.protobuf.HBaseZeroCopyByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSample;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice.Builder;
import org.opencb.opencga.storage.hadoop.models.variantcall.protobuf.VariantCallMeta;

import java.io.IOException;
import java.util.*;

public class GenomeVariantTransformMapperTest {
    private GenomeVariantTransformMapper mapper;
    private MapDriver<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put> mapDriver;

    public static class TestHelper extends GenomeVariantTransformHelper{

        public TestHelper (Configuration conf) {
            super(conf);
        }

    }

    @Before
    public void setUp() throws Exception {
        final VariantCallMeta variantCallMeta = new VariantCallMeta();
        variantCallMeta.addSample("a", 1, HBaseZeroCopyByteString.copyFrom(Bytes.toBytes("a")));
        mapper = new GenomeVariantTransformMapper(){
            @Override
            protected GenomeVariantTransformHelper loadHelper(
                    Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) {
                return new TestHelper(context.getConfiguration());
            }

            @Override
            protected VariantCallMeta loadMeta() throws IOException {
                return variantCallMeta;
            }

            @Override
            protected void initVcfMetaMap(Configuration conf) throws IOException {
                // do nothing
            }

            @Override
            protected Map<String, Result> fetchCurrentValues(Context context, Set<String> keySet)
                    throws IOException {
                Map<String, Result> res = new HashMap<String, Result>();
                for(String s : keySet)
                    res.put(s, Result.EMPTY_RESULT);
                return res;
            }

        };
        mapDriver = MapDriver.newMapDriver(mapper);
        Configuration config = mapDriver.getConfiguration();
        setUpConfig(config);

    }

    public void setUpConfig(Configuration conf) throws IOException {
//        GenomeVariantHelper.setChunkSize(conf, 1);
//        GenomeVariantHelper.setMetaProtoFile(conf, "VCF_To_Avro_Conversion/vcfmeta.proto");

//        AvroSerialization.addToConfiguration(conf);
//        AvroSerialization.setKeyReaderSchema(conf, VariantAvro.SCHEMA$);
//        AvroSerialization.setKeyWriterSchema(conf, VariantAvro.SCHEMA$);


//        String metaStr = metaConvert(new File("VCF_To_Avro_Conversion/out.avro.meta"), null);
//        GenomeVariantHelper.setMetaProtoString(conf, metaStr);

        conf.setStrings("io.serializations", new String[] { conf.get("io.serializations"), MutationSerialization.class.getName(),
                ResultSerialization.class.getName() });

        conf.setStrings(GenomeVariantTransformHelper.OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT, "TEST-out");
        conf.setStrings(GenomeVariantTransformHelper.OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT, "TEST-in");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMapImmutableBytesWritableResultContext() throws IOException {
        ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes("1_123"));
        Builder vsb = VcfSlice.newBuilder();
        org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord.Builder recb = VcfRecord.newBuilder();
        recb.setRelativeStart(10).setReference("A").addAlternate("T");
        recb.addSampleFormatNonDefault("GT");
        org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSample.Builder sampleb = VcfSample.newBuilder();
        sampleb.addSampleValues("0/1");
        recb.addSamples(sampleb.build());
        vsb.setChromosome("1").setPosition(12300).addRecords(
                recb.build());
        byte[] vcf = vsb.build().toByteArray();

        KeyValue cell = new KeyValue(key.get(),Bytes.toBytes(GenomeVariantTransformHelper.DEFAULT_COLUMN_FAMILY),Bytes.toBytes("a"),vcf);
        List<Cell> cellLst = Arrays.asList(cell);
        Result val = Result.create(cellLst);
        mapDriver.setInput(key, val);

        ImmutableBytesWritable okey = new ImmutableBytesWritable(Bytes.toBytes("1_000000012310_A_T"));
        Put oval = new Put(Bytes.toBytes("1_000000012310_A_T"));

        mapDriver.withOutput(okey, oval);
        mapDriver.setValueComparator((o1, o2) -> {
            int res = Bytes.compareTo(o1.getRow(), o2.getRow());
            if(res != 0) return res;
            return 0;
        });
        mapDriver.runTest();
    }

}
