package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.utils.ValueOnlyTextOutputFormat;
import org.opencb.opencga.storage.hadoop.variant.io.VariantDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

public class StreamVariantDriver extends VariantDriver {

    public static final String INPUT_FORMAT_PARAM = "inputFormat";
    public static final String COMMAND_LINE_PARAM = "commandLine";
    public static final String COMMAND_LINE_BASE64_PARAM = "commandLineBase64";
    public static final String MAX_BYTES_PER_MAP_PARAM = "maxBytesPerMap";
    public static final String ENVIRONMENT_VARIABLES = "envVars";
    public static final String STDERR_TXT_GZ = ".stderr.txt.gz";

    private VariantWriterFactory.VariantOutputFormat format;
    private int maxBytesPerMap;
    private static Logger logger = LoggerFactory.getLogger(StreamVariantDriver.class);
    private String commandLine;
    private Map<String, String> envVars;

    private Class<? extends VariantMapper> mapperClass;
    private Class<? extends Reducer> reducerClass;
    private Class<? extends OutputFormat> outputFormatClass;

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = super.getParams();
        params.put(INPUT_FORMAT_PARAM, "<input-format>");
        params.put(COMMAND_LINE_PARAM, "<command-line>");
        params.put(COMMAND_LINE_BASE64_PARAM, "<command-line-base64>");

        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        String inputFormat = getParam(INPUT_FORMAT_PARAM);
        if (inputFormat == null) {
            throw new IllegalArgumentException("Missing input format!");
        }
        format = VariantWriterFactory.toOutputFormat(inputFormat, "");
        if (format == null) {
            throw new IllegalArgumentException("Unknown input format " + inputFormat);
        }
        maxBytesPerMap = Integer.parseInt(getParam(MAX_BYTES_PER_MAP_PARAM, String.valueOf(1024 * 1024 * 1024)));

        commandLine = getParam(COMMAND_LINE_PARAM);
        String commandLineBase64 = getParam(COMMAND_LINE_BASE64_PARAM);
        if (commandLine == null && commandLineBase64 == null) {
            throw new IllegalArgumentException("Missing command line!");
        }
        if (commandLine != null && commandLineBase64 != null) {
            throw new IllegalArgumentException("Only one of '" + COMMAND_LINE_PARAM + "' or '" + COMMAND_LINE_BASE64_PARAM + "'"
                    + " is allowed!");
        }

        if (commandLineBase64 != null) {
            commandLine = new String(java.util.Base64.getDecoder().decode(commandLineBase64));
        }

        envVars = new HashMap<>();
        String envVarsStr = getParam(ENVIRONMENT_VARIABLES);
        if (StringUtils.isNotEmpty(envVarsStr)) {
            String[] split = envVarsStr.split(",");
            for (String s : split) {
                String[] split1 = s.split("=");
                if (split1.length != 2) {
                    throw new IllegalArgumentException("Invalid environment variable '" + s + "'");
                }
                envVars.put(split1[0], split1[1]);
            }
        }


        String outdirStr = getParam(OUTPUT_PARAM);
        if (StringUtils.isEmpty(outdirStr)) {
            throw new IllegalArgumentException("Missing argument " + OUTPUT_PARAM);
        }
    }

    @Override
    protected Class<? extends VariantMapper> getMapperClass() {
        return mapperClass;
    }

    @Override
    protected Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }

    @Override
    protected Class<? extends Partitioner> getPartitioner() {
        return VariantLocusKeyPartitioner.class;
    }

    @Override
    protected Class<? extends OutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }

    @Override
    protected void setupJob(Job job) throws IOException {

        job.getConfiguration().setBoolean(JobContext.MAP_OUTPUT_COMPRESS, true);
        job.getConfiguration().setClass(JobContext.MAP_OUTPUT_COMPRESS_CODEC, DeflateCodec.class, CompressionCodec.class);

        Class<?> keyClass = VariantLocusKey.class;
//        Class<?> keyClass = ImmutableBytesWritable.class;
//        Class<?> keyClass = NullWritable.class;
//        Class<?> keyClass = Text.class;
        Class<Text> valueClass = Text.class;

        mapperClass = StreamVariantMapper.class;
        job.setMapOutputKeyClass(keyClass);
        job.setMapOutputValueClass(valueClass);

        StreamVariantMapper.setCommandLine(job, commandLine);
        StreamVariantMapper.setVariantFormat(job, format);
        StreamVariantMapper.setMaxInputBytesPerProcess(job, maxBytesPerMap);
        StreamVariantMapper.setEnvironment(job, envVars);

        reducerClass = StreamVariantReducer.class;

        MultipleOutputs.addNamedOutput(job, "stdout", ValueOnlyTextOutputFormat.class, keyClass, valueClass);
        MultipleOutputs.addNamedOutput(job, "stderr", ValueOnlyTextOutputFormat.class, keyClass, valueClass);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        outputFormatClass = LazyOutputFormat.class;

        job.setOutputFormatClass(ValueOnlyTextOutputFormat.class);
        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        TextOutputFormat.setOutputCompressorClass(job, DeflateCodec.class);
        job.setOutputKeyClass(keyClass);
        job.setOutputValueClass(valueClass);
    }

    @Override
    protected void setupReducer(Job job, String variantTableName) throws IOException {
        super.setupReducer(job, variantTableName);
        // TODO: Use a grouping comparator to group by chromosome and position, ignoring the rest of the key?
//        job.setGroupingComparatorClass(StreamVariantGroupingComparator.class);
//        job.setSortComparatorClass();
    }

    @Override
    protected String getJobOperationName() {
        return "stream-variants";
    }


    @Override
    protected void copyMrOutputToLocal() throws IOException {
        concatMrOutputToLocal(outdir, localOutput, true, "stdout");
        Path stderrOutput = localOutput.suffix(STDERR_TXT_GZ);
        concatMrOutputToLocal(outdir, stderrOutput, true, "stderr");
        printKeyValue("EXTRA_OUTPUT_STDERR", stderrOutput);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends Tool>) MethodHandles.lookup().lookupClass());
    }

}
