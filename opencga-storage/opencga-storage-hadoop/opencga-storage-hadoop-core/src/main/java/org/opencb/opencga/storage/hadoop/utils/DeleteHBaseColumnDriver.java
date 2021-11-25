package org.opencb.opencga.storage.hadoop.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.WRITE_MAPPERS_LIMIT_FACTOR;

/**
 * Created on 19/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DeleteHBaseColumnDriver extends AbstractHBaseDriver {

    public static final String COLUMNS_TO_DELETE = "columns_to_delete";
    public static final String DELETE_ALL_COLUMNS = "delete_all_columns";
    public static final String REGIONS_TO_DELETE = "regions_to_delete";
    public static final String TWO_PHASES_PARAM = "two_phases_delete";
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteHBaseColumnDriver.class);
    // This separator is not valid at Bytes.toStringBinary
    private static final String REGION_SEPARATOR = "\\\\";
    public static final String DELETE_HBASE_COLUMN_TASK_CLASS = "delete.hbase.column.task.class";

    private Map<String, List<String>> columns;
    private List<Pair<byte[], byte[]>> regions;
    private Path outdir;

    public void setupJob(Job job, String table) throws IOException {
        Set<String> allColumns = columns.entrySet()
                .stream()
                .flatMap(e -> Stream.concat(Stream.of(e.getKey()), e.getValue().stream()))
                .collect(Collectors.toSet());
        job.getConfiguration().set(COLUMNS_TO_DELETE, serializeColumnsToDelete(columns));
        // There is a maximum number of counters
        job.getConfiguration().setStrings(COLUMNS_TO_COUNT, new ArrayList<>(allColumns)
                .subList(0, Math.min(allColumns.size(), 50)).toArray(new String[0]));

        Scan templateScan = new Scan();
        int caching = job.getConfiguration().getInt(HadoopVariantStorageOptions.MR_HBASE_SCAN_CACHING.key(), 100);
        LOGGER.info("Scan set Caching to " + caching);
        templateScan.setCaching(caching);        // 1 is the default in Scan
        templateScan.setCacheBlocks(false);  // don't set to true for MR jobs
        templateScan.setFilter(new KeyOnlyFilter());
        for (String column : allColumns) {
            String[] split = column.split(":");
            templateScan.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
        }

        List<Scan> scans;
        if (!regions.isEmpty()) {
            scans = new ArrayList<>(regions.size() / 2);
            for (Pair<byte[], byte[]> region : regions) {
                Scan scan = new Scan(templateScan);
                scans.add(scan);
                if (region.getFirst() != null && region.getFirst().length != 0) {
                    scan.setStartRow(region.getFirst());
                }
                if (region.getSecond() != null && region.getSecond().length != 0) {
                    scan.setStopRow(region.getSecond());
                }
            }
        } else {
            scans = Collections.singletonList(templateScan);
        }

        // set other scan attrs
        boolean twoPhases = Boolean.parseBoolean(getParam(TWO_PHASES_PARAM));
        if (!twoPhases) {
            VariantMapReduceUtil.initTableMapperJob(job, table, scans, DeleteHBaseColumnMapper.class);
            VariantMapReduceUtil.setOutputHBaseTable(job, table);
            VariantMapReduceUtil.setNoneReduce(job);
        } else {
            VariantMapReduceUtil.initTableMapperJob(job, table, scans, DeleteHBaseColumnToProtoMapper.class);
            outdir = getTempOutdir("opencga_delete", table, true);
            outdir.getFileSystem(getConf()).deleteOnExit(outdir);

            LOGGER.info(" * Temporary outdir file: " + outdir.toUri());


            job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
            job.setOutputValueClass(BytesWritable.class);
            job.setOutputKeyClass(BytesWritable.class);
            SequenceFileAsBinaryOutputFormat.setOutputPath(job, outdir);
            SequenceFileAsBinaryOutputFormat.setCompressOutput(job, true);
            SequenceFileAsBinaryOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            SequenceFileAsBinaryOutputFormat.setOutputCompressorClass(job, DeflateCodec.class);

            VariantMapReduceUtil.setNoneReduce(job);
        }
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        try {
            if (succeed) {
                if (outdir != null) {
                    FileSystem fs = outdir.getFileSystem(getConf());
                    ContentSummary contentSummary = fs.getContentSummary(outdir);
                    LOGGER.info("Generated file " + outdir.toUri());
                    LOGGER.info(" - Size (HDFS)         : " + IOUtils.humanReadableByteCount(contentSummary.getLength(), false));
                    LOGGER.info(" - SpaceConsumed (raw) : " + IOUtils.humanReadableByteCount(contentSummary.getSpaceConsumed(), false));

                    String writeMapperslimitFactor = getParam(WRITE_MAPPERS_LIMIT_FACTOR.key(),
                            WRITE_MAPPERS_LIMIT_FACTOR.defaultValue().toString());
                    int code = new HBaseWriterDriver(getConf()).run(HBaseWriterDriver.buildArgs(table,
                            new ObjectMap()
                                    .append(HBaseWriterDriver.INPUT_FILE_PARAM, outdir.toUri().toString())
                                    .append(WRITE_MAPPERS_LIMIT_FACTOR.key(), writeMapperslimitFactor)));
                    if (code != 0) {
                        throw new StorageEngineException("Error writing mutations");
                    }
                }
            }
        } catch (StorageEngineException e) {
            // Don't double wrap this exception
            throw e;
        } catch (Exception e) {
            throw new StorageEngineException("Error writing mutations", e);
        } finally {
            if (outdir != null) {
                deleteTemporaryFile(outdir);
            }
        }
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        DeleteHBaseColumnTask task = getDeleteHBaseColumnTask(getConf());
        columns = task.getColumnsToDelete(getConf());

        if (columns.isEmpty()) {
            if (getConf().getBoolean(DELETE_ALL_COLUMNS, false)) {
                LOGGER.info("Delete ALL columns");
            } else {
                throw new IllegalArgumentException("No columns specified!");
            }
        }

        for (Map.Entry<String, List<String>> entry : columns.entrySet()) {
            checkColumn(entry.getKey());
            for (String s : entry.getValue()) {
                checkColumn(s);
            }
        }

        regions = task.getRegionsToDelete(getConf());
    }


    private void checkColumn(String column) {
        if (!column.contains(":")) {
            throw new IllegalArgumentException("Malformed column '" + column + "'. Requires <family>:<column>");
        }
    }

    private static String serializeColumnsToDelete(Map<String, List<String>> columnsToDelete) {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, List<String>> entry : columnsToDelete.entrySet()) {
            if (sb.length() > 0) {
                sb.append(";");
            }

            sb.append(entry.getKey());
            if (CollectionUtils.isNotEmpty(entry.getValue())) {
                sb.append("-->");
                sb.append(String.join(",", entry.getValue()));
            }
        }

        return sb.toString();
    }

    @Override
    protected String getJobName() {
        return "opencga: delete columns from table '" + table + '\'';
    }

    @Override
    protected String getUsage() {
        return "Usage: " + getClass().getSimpleName()
                + " [generic options] <table> <family_1>:<column_1>(,<family_n>:<column_n>)* (<key> <value>)*";
    }

    public static String[] buildArgs(String table, List<String> columns, ObjectMap options) {
        return buildArgs(table, columns.stream().collect(Collectors.toMap(s -> s, s -> Collections.emptyList())), options);
    }

    public static String[] buildArgs(String table, Map<String, List<String>> columns, ObjectMap options) {
        return buildArgs(table, columns, false, null, options);
    }

    public static String[] buildArgs(String table, Map<String, List<String>> columns, boolean deleteAllColumns,
                                     List<Pair<byte[], byte[]>> regions, ObjectMap options) {
        ObjectMap args = new ObjectMap();
        if (deleteAllColumns) {
            args.append(DELETE_ALL_COLUMNS, true);
        } else {
            args.append(COLUMNS_TO_DELETE, serializeColumnsToDelete(columns));
        }

        if (regions != null) {
            StringBuilder sb = new StringBuilder();
            Iterator<Pair<byte[], byte[]>> iterator = regions.iterator();
            while (iterator.hasNext()) {
                Pair<byte[], byte[]> region = iterator.next();
                String start = Bytes.toStringBinary(region.getFirst());
                String end = Bytes.toStringBinary(region.getSecond());

                sb.append(start);
                sb.append(REGION_SEPARATOR);
                sb.append(end);
                if (iterator.hasNext()) {
                    sb.append(REGION_SEPARATOR);
                }
            }
            args.append(REGIONS_TO_DELETE, sb.toString());
        }

        args.putAll(options);

        return buildArgs(table, args);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends AbstractVariantsTableDriver>) MethodHandles.lookup().lookupClass());
    }

    public static class DeleteHBaseColumnToProtoMapper extends TableMapper<BytesWritable, BytesWritable> {
        private DeleteHBaseColumnTask task;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            task = getDeleteHBaseColumnTask(context.getConfiguration());
            task.setup(context);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            for (Mutation value : task.map(result)) {
                ClientProtos.MutationProto proto;
                if (value instanceof Delete) {
                    proto = ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.DELETE, value);
                } else if (value instanceof Put) {
                    proto = ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, value);
                } else {
                    throw new IllegalArgumentException("Unknown mutation type " + value.getClass());
                }
                context.write(new BytesWritable(value.getRow()), new BytesWritable(proto.toByteArray()));
            }
            // Indicate that the process is still alive
            context.progress();
        }
    }

    public static class DeleteHBaseColumnMapper extends TableMapper<ImmutableBytesWritable, Mutation> {
        private DeleteHBaseColumnTask task;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            task = getDeleteHBaseColumnTask(context.getConfiguration());
            task.setup(context);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            for (Mutation mutation : task.map(result)) {
                context.write(key, mutation);
            }
            // Indicate that the process is still alive
            context.progress();
        }
    }

    public static class DeleteHBaseColumnTask {
        private TaskAttemptContext context;
        protected Set<String> columnsToCount;
        protected Map<String, List<String>> columnsToDelete;
        protected boolean deleteAllColumns;

        protected void initCounter(String counter) {
            count(counter, 0);
        }

        protected final void count(String counter) {
            count(counter, 1);
        }

        protected final void count(String counter, int incr) {
            context.getCounter(COUNTER_GROUP_NAME, counter).increment(incr);
        }

        protected final void setup(TaskAttemptContext context) throws IOException {
            this.context = context;
            setup(context.getConfiguration());
            initCounter("INPUT_ROWS");
            initCounter("DELETE");
            initCounter("NO_DELETE");
        }

        protected void setup(Configuration configuration) throws IOException {
            deleteAllColumns = configuration.getBoolean(DELETE_ALL_COLUMNS, false);
            columnsToCount = new HashSet<>(configuration.get(COLUMNS_TO_COUNT) == null
                    ? Collections.emptyList()
                    : Arrays.asList(configuration.getStrings(COLUMNS_TO_COUNT)));
            columnsToDelete = getColumnsToDelete(configuration);
        }

        protected List<Mutation> map(Result result) {
            Delete delete = new Delete(result.getRow());
            count("INPUT_ROWS");
            if (deleteAllColumns) {
                count("DELETE");
                return Collections.singletonList(delete);
            } else {
                for (Cell cell : result.rawCells()) {
                    byte[] family = CellUtil.cloneFamily(cell);
                    byte[] qualifier = CellUtil.cloneQualifier(cell);
                    String c = Bytes.toString(family) + ':' + Bytes.toString(qualifier);
                    List<String> otherColumns = columnsToDelete.get(c);
                    if (columnsToDelete.containsKey(c)) {
                        delete.addColumn(family, qualifier);
                        for (String otherColumn : otherColumns) {
                            if (columnsToCount.contains(otherColumn)) {
                                count(otherColumn);
                            }
                            String[] split = otherColumn.split(":", 2);
                            delete.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
                        }
                        if (columnsToCount.contains(c)) {
                            count(c);
                        }
                    }
                }
                if (delete.isEmpty()) {
                    count("NO_DELETE");
                    return Collections.emptyList();
                } else {
                    count("DELETE");
                    return Collections.singletonList(delete);
                }
            }
        }

        public List<Pair<byte[], byte[]>> getRegionsToDelete(Configuration configuration) {
            List<Pair<byte[], byte[]>> regions = new ArrayList<>();
            String regionsStr = configuration.get(REGIONS_TO_DELETE);
            if (regionsStr != null && !regionsStr.isEmpty()) {
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(regionsStr, REGION_SEPARATOR);
                if (split.length % 2 != 0) {
                    throw new IllegalArgumentException("Expected pair number of elements in region.  Got " + split.length);
                }
                for (int i = 0; i < split.length; i += 2) {
                    regions.add(new Pair<>(Bytes.toBytesBinary(split[i]), Bytes.toBytesBinary(split[i + 1])));
                }
            }
            return regions;
        }

        public Map<String, List<String>> getColumnsToDelete(Configuration conf) {
            Map<String, List<String>> columns;
            if (conf.get(COLUMNS_TO_DELETE) == null) {
                columns = Collections.emptyMap();
            } else {
                String[] columnStrings = conf.get(COLUMNS_TO_DELETE).split(";");
                columns = new HashMap<>(columnStrings.length);
                for (String elem : columnStrings) {
                    String[] split = elem.split("-->");
                    if (split.length > 1) {
                        columns.put(split[0], Arrays.asList(split[1].split(",")));
                    } else {
                        columns.put(elem, Collections.emptyList());
                    }
                }
            }
            return columns;
        }
    }

    public static Class<? extends DeleteHBaseColumnTask> getDeleteHbaseColumnTaskClass(Configuration configuration) {
        return configuration
                .getClass(DELETE_HBASE_COLUMN_TASK_CLASS, DeleteHBaseColumnTask.class, DeleteHBaseColumnTask.class);
    }

    public static DeleteHBaseColumnTask getDeleteHBaseColumnTask(Configuration configuration) {
        Class<? extends DeleteHBaseColumnTask> taskClass = getDeleteHbaseColumnTaskClass(configuration);
        try {
            return taskClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Unable to create new instance of " + DeleteHBaseColumnTask.class, e);
        }
    }
}
