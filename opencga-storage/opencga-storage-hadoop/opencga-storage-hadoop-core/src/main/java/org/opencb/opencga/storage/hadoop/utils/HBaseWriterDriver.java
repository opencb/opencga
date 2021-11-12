package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

import static org.apache.hadoop.mapreduce.MRJobConfig.JOB_RUNNING_MAP_LIMIT;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.WRITE_MAPPERS_LIMIT_FACTOR;

/**
 * Created on 09/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseWriterDriver extends AbstractHBaseDriver {

    public static final String INPUT_FILE_PARAM = "inputFile";
    private final Logger logger = LoggerFactory.getLogger(HBaseWriterDriver.class);
    private String inputPath;

    public HBaseWriterDriver() {
    }

    public HBaseWriterDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        FileSystem fs = FileSystem.get(getConf());
        inputPath = getConf().get(INPUT_FILE_PARAM);
        if (!fs.exists(new Path(inputPath))) {
            throw new FileNotFoundException("Intermediate file not found: " + inputPath);
        }
    }

    @Override
    protected void setupJob(Job job, String table) throws IOException {
//        ObjectMap options = new ObjectMap();
//        getConf().iterator().forEachRemaining(entry -> options.put(entry.getKey(), entry.getValue()));
        int serversSize;
        try (HBaseManager hBaseManager = new HBaseManager(getConf())) {
            serversSize = hBaseManager.act(table, (t, admin) -> admin.getClusterStatus().getServersSize());
        }
        float factor = getConf().getFloat(WRITE_MAPPERS_LIMIT_FACTOR.key(),
                WRITE_MAPPERS_LIMIT_FACTOR.defaultValue());
        if (factor <= 0) {
            throw new IllegalArgumentException(WRITE_MAPPERS_LIMIT_FACTOR + " must be positive!");
        }
        int mapsLimit = Math.round(serversSize * factor);
        if (mapsLimit == 0) {
            mapsLimit = 40;
        }
        job.getConfiguration().setInt(JOB_RUNNING_MAP_LIMIT, mapsLimit);
        logger.info("Set job running map limit to " + mapsLimit + ". ServersSize: " + serversSize + ", mappersFactor: " + factor);

        // input
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);

        // mapper
        job.setMapperClass(HBaseWriterMapper.class);

        job.setSpeculativeExecution(false);

        // output
        VariantMapReduceUtil.setOutputHBaseTable(job, table);

        VariantMapReduceUtil.setNoneReduce(job);

    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
//        if (succeed) {
//            fs.delete(new Path(inputPath), true);
//        }
    }

    @Override
    protected String getJobName() {
        return "hbase write mutations";
    }


    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends AbstractVariantsTableDriver>) MethodHandles.lookup().lookupClass());
    }

    public static class HBaseWriterMapper extends Mapper<BytesWritable, BytesWritable, ImmutableBytesWritable, Mutation> {


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            context.getCounter(COUNTER_GROUP_NAME, ClientProtos.MutationProto.MutationType.PUT.toString()).increment(0);
            context.getCounter(COUNTER_GROUP_NAME, ClientProtos.MutationProto.MutationType.DELETE.toString()).increment(0);
        }

        @Override
        protected void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            ClientProtos.MutationProto proto;
            proto = ClientProtos.MutationProto.PARSER.parseFrom(value.getBytes(), 0, value.getLength());

            Mutation mutation = ProtobufUtil.toMutation(proto);
            context.getCounter(COUNTER_GROUP_NAME, proto.getMutateType().toString()).increment(1);
            context.write(new ImmutableBytesWritable(), mutation);

            // Indicate that the process is still alive
            context.progress();
        }
    }
}
