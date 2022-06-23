package org.opencb.opencga.storage.hadoop.variant.prune;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.GenomeHelper.COLUMN_FAMILY_BYTES;

public class VariantPruneDriver extends AbstractVariantsTableDriver {

    private Logger logger = LoggerFactory.getLogger(HadoopVariantPruneManager.class);
    public static final String ATTRIBUTE_DELETION_TYPE = "d_type";
    public static final byte[] ATTRIBUTE_DELETION_TYPE_FULL = Bytes.toBytes("FULL");
    public static final byte[] ATTRIBUTE_DELETION_TYPE_PARTIAL = Bytes.toBytes("PARTIAL");
    public static final String ATTRIBUTE_DELETION_STUDIES = "d_studies";
    private final VariantPruneDriverParams params = new VariantPruneDriverParams();
    private MapReduceOutputFile output;

    @Override
    protected Class<VariantPruneMapper> getMapperClass() {
        return VariantPruneMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return "vairants-prune";
    }

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        for (String key : this.params.fields().keySet()) {
            params.put(key, "<" + key + ">");
        }
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        for (String key : params.fields().keySet()) {
            String value = getParam(key);
            if (value != null) {
                params.updateParams(new HashMap<>(Collections.singletonMap(key, value)));
            }
        }
        output = new MapReduceOutputFile(
                () -> "variant_prune_report." + TimeUtils.getTime() + ".txt",
                "variant_prune_report");
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        Scan scan = new Scan()
                .addFamily(COLUMN_FAMILY_BYTES);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        scan.setFilter(filterList);

        scan.addColumn(COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.INDEX_NOT_SYNC.bytes());
        scan.addColumn(COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.INDEX_UNKNOWN.bytes());
        scan.addColumn(COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.INDEX_STUDIES.bytes());

        Map<Integer, List<String>> columnsPerStudy = new HashMap<>();
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        for (Map.Entry<String, Integer> entry : metadataManager.getStudies().entrySet()) {
            Integer studyId = entry.getValue();
            Integer cohortId = metadataManager.getCohortId(studyId, StudyEntry.DEFAULT_COHORT);

            scan.addColumn(COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStudyColumn(studyId).bytes());
            scan.addColumn(COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStatsColumn(studyId, cohortId).bytes());

            List<String> columnsToDelete = new ArrayList<>();
            for (PhoenixHelper.Column c : VariantPhoenixSchema.getStudyColumns(studyId)) {
                columnsToDelete.add(c.column());
            }
            metadataManager.cohortIterator(studyId).forEachRemaining(cohortMetadata -> {
                for (PhoenixHelper.Column c : VariantPhoenixSchema.getStatsColumns(studyId, cohortMetadata.getId())) {
                    columnsToDelete.add(c.column());
                }
            });
            for (VariantScoreMetadata variantScore : metadataManager.getStudyMetadata(studyId).getVariantScores()) {
                columnsToDelete.add(VariantPhoenixSchema.getVariantScoreColumn(studyId, variantScore.getId()).column());
            }
            columnsPerStudy.put(studyId, columnsToDelete);
        }

        VariantMapReduceUtil.configureMapReduceScan(scan, getConf());

        if (!params.isDryRun()) {
            // TODO: Remove this line. Test purposes only
            logger.warn("Not dry mode! Enforce dry-mode");
            params.setDryRun(true);
        }

        FileOutputFormat.setCompressOutput(job, false);
        FileOutputFormat.setOutputPath(job, output.getOutdir());

        if (params.isDryRun()) {
            logger.info("Dry mode");
            VariantMapReduceUtil.initTableMapperJob(job, variantTable, scan, getMapperClass());
            VariantMapReduceUtil.setNoneReduce(job);
            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputValueClass(Mutation.class);
            job.setOutputFormatClass(VariantPruneReportOutputFormat.class);
        } else {
            logger.info("Configure multi output job");
            VariantMapReduceUtil.initTableMapperMultiOutputJob(job, variantTable, Collections.singletonList(scan), getMapperClass());
            job.setOutputFormatClass(VariantPruneReportAndWriteOutputFormat.class);
        }

        VariantTableHelper.setVariantsTable(job.getConfiguration(), variantTable);
        job.getConfiguration().set(VariantPruneMapper.PENDING_TABLE, getTableNameGenerator().getPendingSecondaryIndexPruneTableName());
        VariantPruneMapper.setColumnsPerStudy(job.getConfiguration(), columnsPerStudy);
        return job;
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        output.postExecute(succeed);
    }

    public static class VariantPruneMapper extends TableMapper<ImmutableBytesWritable, Mutation> {

        public static final String PENDING_TABLE = "VariantPruneMapper.pending_table";
        public static final String COLUMNS_PER_STUDY = "VariantPruneMapper.columnsPerStudy";

        private ImmutableBytesWritable variantsTable;
        private ImmutableBytesWritable pendingDeletionVariantsTable;
        private Map<Integer, List<byte[]>> columnsPerStudy;


        public static final byte[] COLUMN = Bytes.toBytes("v");
        public static final byte[] VALUE = new byte[0];

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            variantsTable = new ImmutableBytesWritable(Bytes.toBytes(VariantTableHelper.getVariantsTable(conf)));
            pendingDeletionVariantsTable = new ImmutableBytesWritable(Bytes.toBytes(conf.get(PENDING_TABLE)));

            this.columnsPerStudy = getColumnsPerStudy(conf);
        }

        public static void setColumnsPerStudy(Configuration conf, Map<Integer, List<String>> columnsPerStudy) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Integer, List<String>> entry : columnsPerStudy.entrySet()) {
                sb.append(entry.getKey()).append("=");
                for (String column : entry.getValue()) {
                    sb.append(column).append(",");
                }
                sb.append(";");
            }
            conf.set(COLUMNS_PER_STUDY, sb.toString());
        }

        public static Map<Integer, List<byte[]>> getColumnsPerStudy(Configuration conf) {
            Map<Integer, List<byte[]>> columnsPerStudy = new HashMap<>();
            for (String s : conf.get(COLUMNS_PER_STUDY).split(";")) {
                String[] studyColumns = s.split("=");
                Integer studyId = Integer.valueOf(studyColumns[0]);
                columnsPerStudy.put(studyId, Arrays.stream(studyColumns[1].split(",")).map(Bytes::toBytes).collect(Collectors.toList()));
            }
            return columnsPerStudy;
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {
            VariantRow variantRow = new VariantRow(value);

            List<Integer> emptyStudies = new ArrayList<>();
            List<Integer> studies = new ArrayList<>();

            variantRow.walker()
                    .onStudy(studyId -> {
                        studies.add(studyId);
                    })
                    .onCohortStats(c -> {
                        VariantStats variantStats = c.toJava();
                        if (variantStats.getFileCount() == 0) {
                            emptyStudies.add(c.getStudyId());
                        }
                    })
                    .walk();

            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);

            // It might happen that the variant has 0 studies, so emptyStudies is empty
            if (emptyStudies.size() == studies.size()) {
                // Drop variant && add to deleted variants list
                context.getCounter(COUNTER_GROUP_NAME, "variants_deleted").increment(1);

                context.write(pendingDeletionVariantsTable,
                        new Put(value.getRow()).addColumn(COLUMN_FAMILY_BYTES, COLUMN, VALUE));
                Delete delete = new Delete(value.getRow());
                delete.addFamily(COLUMN_FAMILY_BYTES);
                delete.setAttribute(ATTRIBUTE_DELETION_TYPE, ATTRIBUTE_DELETION_TYPE_FULL);
                delete.setAttribute(ATTRIBUTE_DELETION_STUDIES,
                        Bytes.toBytes(emptyStudies.stream().map(Object::toString).collect(Collectors.joining(","))));
                context.write(variantsTable, delete);
            } else if (emptyStudies.isEmpty()) {
                // skipVariant
                context.getCounter(COUNTER_GROUP_NAME, "variants_untouched").increment(1);
            } else {
                context.getCounter(COUNTER_GROUP_NAME, "variants_delete_some_studies").increment(1);
                // Drop studies from variant
                Delete delete = new Delete(value.getRow());

                for (Integer emptyStudy : emptyStudies) {
                    List<byte[]> columnsToDelete = columnsPerStudy.get(emptyStudy);
                    for (byte[] columnToDelete : columnsToDelete) {
                        delete.addColumns(COLUMN_FAMILY_BYTES, columnToDelete);
                    }
                }
                delete.setAttribute(ATTRIBUTE_DELETION_TYPE, ATTRIBUTE_DELETION_TYPE_PARTIAL);
                delete.setAttribute(ATTRIBUTE_DELETION_STUDIES,
                        Bytes.toBytes(emptyStudies.stream().map(Object::toString).collect(Collectors.joining(","))));

                Put updateSecondaryIndexColumns = new Put(value.getRow());

                HadoopVariantSearchIndexUtils.addNotSyncStatus(updateSecondaryIndexColumns);

                context.write(variantsTable, delete);
                context.write(variantsTable, updateSecondaryIndexColumns);
            }
        }
    }

    public static class VariantPruneReportAndWriteOutputFormat extends OutputFormat<ImmutableBytesWritable, Mutation> {

        MultiTableOutputFormat hbaseOutputFormat;
        VariantPruneReportOutputFormat reportOutputFormat;

        public VariantPruneReportAndWriteOutputFormat() {
            hbaseOutputFormat = new MultiTableOutputFormat();
            reportOutputFormat = new VariantPruneReportOutputFormat();
        }

        @Override
        public RecordWriter<ImmutableBytesWritable, Mutation> getRecordWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<ImmutableBytesWritable, Mutation> hbaseRecordWriter = hbaseOutputFormat.getRecordWriter(context);
            RecordWriter<ImmutableBytesWritable, Mutation> reportRecordWriter = reportOutputFormat.getRecordWriter(context);
            return new RecordWriter<ImmutableBytesWritable, Mutation>() {

                @Override
                public void write(ImmutableBytesWritable key, Mutation value) throws IOException, InterruptedException {
                    reportRecordWriter.write(key, value);
                    hbaseRecordWriter.write(key, value);
                }

                @Override
                public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                    reportRecordWriter.close(context);
                    hbaseRecordWriter.close(context);
                }
            };
        }

        @Override
        public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
            hbaseOutputFormat.checkOutputSpecs(context);
            reportOutputFormat.checkOutputSpecs(context);
        }

        @Override
        public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
            // Ignore TableOutputCommitter as it is no-op.
            return reportOutputFormat.getOutputCommitter(context);
        }
    }

    public static class VariantPruneReportOutputFormat extends FileOutputFormat<ImmutableBytesWritable, Mutation> {

        protected static class ReportRecordWriter extends RecordWriter<ImmutableBytesWritable, Mutation> {
            private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
            private static final byte[] SEPARATOR = "\t".getBytes(StandardCharsets.UTF_8);

            protected DataOutputStream out;

            public ReportRecordWriter(DataOutputStream out) {
                this.out = out;
            }

            public synchronized void write(ImmutableBytesWritable key, Mutation mutation)
                    throws IOException {
                if (mutation instanceof Delete) {
                    Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(mutation.getRow());
                    out.write(variant.toString().getBytes(StandardCharsets.UTF_8));
                    out.write(SEPARATOR);
                    out.write(mutation.getAttribute(ATTRIBUTE_DELETION_TYPE));
                    out.write(SEPARATOR);
                    out.write(mutation.getAttribute(ATTRIBUTE_DELETION_STUDIES));
                    out.write(NEWLINE);
                }
            }

            public synchronized void close(TaskAttemptContext context) throws IOException {
                out.close();
            }
        }

        public RecordWriter<ImmutableBytesWritable, Mutation> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            boolean isCompressed = getCompressOutput(job);
            CompressionCodec codec = null;
            String extension = "";
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass =
                        getOutputCompressorClass(job, GzipCodec.class);
                codec = ReflectionUtils.newInstance(codecClass, conf);
                extension = codec.getDefaultExtension();
            }
            Path file = getDefaultWorkFile(job, extension);
            FileSystem fs = file.getFileSystem(conf);
            DataOutputStream fileOut = fs.create(file, false);
            if (isCompressed) {
                fileOut = new DataOutputStream(codec.createOutputStream(fileOut));
            }
            return new ReportRecordWriter(fileOut);
        }
    }

}
