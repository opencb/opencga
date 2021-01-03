package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.extractFileIdOrNull;

public class CheckVariantStatsDriver extends AbstractVariantsTableDriver {
    private static final String NUM_FILES = "numFiles";

    private final Logger logger = LoggerFactory.getLogger(CheckVariantStatsDriver.class);
    private Path outputDir;
    private Path expectedResultsOutputDir;
    private String region;

    @Override
    protected Class<CheckVariantStatsMapper> getMapperClass() {
        return CheckVariantStatsMapper.class;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        region = getParam(VariantQueryParam.REGION.key());
        outputDir = new Path("stats_by_file_" + getVariantsTable() + "_" + getStudyId()
                + "." + TimeUtils.getTime() + ".tsv");
        expectedResultsOutputDir = new Path("expected_stats_by_file_" + getVariantsTable() + "_" + getStudyId()
                + "." + TimeUtils.getTime() + ".tsv");
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        Scan scan = new Scan();

        if (StringUtils.isNotEmpty(region)) {
            VariantHBaseQueryParser.addRegionFilter(scan, new Region(region));
        }

        scan.setFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes(VariantPhoenixSchema.buildStudyColumnsPrefix(getStudyId())))));

        // ^1_[^_]*_F$
//        String regex = "^" + buildStudyColumnsPrefix(getStudyId()) + "[^" + COLUMN_KEY_SEPARATOR_STR + "]*" + FILE_SUFIX + "$";
//        scan.setFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
//                new RegexStringComparator(
//                        regex,
//                        RegexStringComparator.EngineType.JONI)));

        scan.setMaxVersions(1);
        VariantMapReduceUtil.configureMapReduceScan(scan, job.getConfiguration());
        job.getConfiguration().setInt(NUM_FILES, getMetadataManager().getIndexedFiles(getStudyId())
                .stream()
                .mapToInt(Integer::intValue)
                .max()
                .orElse(0) + 1);

        VariantMapReduceUtil.initTableMapperJob(job, variantTable, scan, getMapperClass());

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(CheckVariantStatsWritable.class);

        job.setCombinerClass(CheckVariantStatsReducer.class);
        job.setReducerClass(CheckVariantStatsReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setCompressOutput(job, true);
//        TextOutputFormat.setCompressOutput(job, false);
        TextOutputFormat.setOutputPath(job, outputDir);


        return job;
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);

        if (succeed) {
            try (FSDataOutputStream os =
                         FileSystem.get(expectedResultsOutputDir.toUri(), getConf()).create(expectedResultsOutputDir);
                 PrintStream out = new PrintStream(os)) {

                out.print("#FILE_ID\tFILE_NAME\tNUM_VARIANTS");
                for (VariantType value : VariantType.values()) {
                    out.print("\t");
                    out.print(value);
                }
                for (int i = 0; i < FileStatsWritable.NUM_CHROMOSOMES; i++) {
                    out.print("\t");
                    out.print(FileStatsWritable.getChromosome(i));
                }
                out.println();


                Iterator<VariantFileMetadata> it = getMetadataManager().variantFileMetadataIterator(getStudyId(), new QueryOptions());
                while (it.hasNext()) {
                    VariantFileMetadata next = it.next();
                    VariantSetStats stats = next.getStats();
                    out.print(next.getId());
                    out.print("\t");
                    out.print(next.getPath().substring(next.getPath().lastIndexOf("/") + 1));
                    out.print("\t");
                    out.print(stats.getVariantCount());

                    for (VariantType value : VariantType.values()) {
                        out.print("\t");
                        out.print(stats.getTypeCount().getOrDefault(value.name(), 0L));
                    }

                    Map<String, Long> chromosomeCounts = stats.getChromosomeCount().entrySet()
                            .stream()
                            .collect(Collectors.toMap(
                                    e -> Region.normalizeChromosome(e.getKey()), // Normalize chromosomes
                                    Map.Entry::getValue));
                    for (int i = 0; i < FileStatsWritable.NUM_CHROMOSOMES; i++) {
                        String chr = FileStatsWritable.getChromosome(i);
                        out.print("\t");
                        if (chr.equals("OTHER")) {
                            int count = 0;
                            for (Map.Entry<String, Long> entry : chromosomeCounts.entrySet()) {
                                if (FileStatsWritable.getChromosomeIdx(entry.getKey()) == i) {
                                    count += entry.getValue();
                                }
                            }
                            out.print(count);
                        } else {
                            out.print(chromosomeCounts.getOrDefault(chr, 0L));
                        }
                    }
                    out.println();
                }
            }
        }
//        if (succeed) {
//            FileSystem.get(outputDir.toUri(), getConf())
//                    .copyToLocalFile(outputDir, new Path(URI.create("file:///tmp/ttt")));
//        }
        logger.info("Actual Stats Output = " + outputDir.toUri());
        logger.info("Expected Stats Output = " + expectedResultsOutputDir.toUri());
    }

    @Override
    protected String getJobOperationName() {
        return "CheckVariantStats";
    }

    public static class FileStatsWritable implements Writable {

        public static final int NUM_CHROMOSOMES = 26;
        private int fileId;
        private int numVariants;
        private final int[] numByOverlappingStatus = new int[VariantOverlappingStatus.values().length];
        private final int[] numByType = new int[VariantType.values().length];
        private final int[] numByChromosome = new int[NUM_CHROMOSOMES];

        public static int getChromosomeIdx(String chromosome) {
            chromosome = Region.normalizeChromosome(chromosome);
            if (StringUtils.isNumeric(chromosome)) {
                return Integer.valueOf(chromosome) - 1;
            } else if (chromosome.equals("X")) {
                return 22;
            } else if (chromosome.equals("Y")) {
                return 23;
            } else if (chromosome.equals("M") || chromosome.equals("MT")) {
                return 24;
            } else {
                return 25;
            }
        }

        public static String getChromosome(int idx) {
            if (idx < 22) {
                return String.valueOf(idx + 1);
            } else if (idx == 22) {
                return "X";
            } else if (idx == 23) {
                return "Y";
            } else if (idx == 24) {
                return "MT";
            } else {
                return "OTHER";
            }
        }

        public FileStatsWritable() {
        }

        public FileStatsWritable(int fileId) {
            this.fileId = fileId;
        }

        public void combine(FileStatsWritable other) {
            if (other.fileId != fileId) {
                throw new IllegalArgumentException("FileId does not match!");
            }
            this.numVariants += other.numVariants;
            combineArray(numByOverlappingStatus, other.numByOverlappingStatus);
            combineArray(numByType, other.numByType);
            combineArray(numByChromosome, other.numByChromosome);
        }

        protected void combineArray(int[] target, int[] other) {
            for (int i = 0; i < other.length; i++) {
                target[i] += other[i];
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(fileId);
            out.writeInt(numVariants);

            writeArray(out, numByOverlappingStatus);
            writeArray(out, numByType);
            writeArray(out, numByChromosome);
        }

        protected void writeArray(DataOutput out, int[] array) throws IOException {
            for (int i : array) {
                out.writeInt(i);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            fileId = in.readInt();
            numVariants = in.readInt();

            readArray(in, numByOverlappingStatus);
            readArray(in, numByType);
            readArray(in, numByChromosome);
        }

        protected void readArray(DataInput in, int[] array) throws IOException {
            for (int i = 0; i < array.length; i++) {
                array[i] = in.readInt();
            }
        }
    }

    public static class CheckVariantStatsWritable implements Writable {
        private ArrayWritable files;

        public CheckVariantStatsWritable() {
            files = new ArrayWritable(FileStatsWritable.class);
        }

        public CheckVariantStatsWritable(int numFiles) {
            files = new ArrayWritable(FileStatsWritable.class);
            files.set(new Writable[numFiles]);
            for (int fileId = 0; fileId < numFiles; fileId++) {
                files.get()[fileId] = new FileStatsWritable(fileId);
            }
        }

        public void combine(CheckVariantStatsWritable other) {
            for (int fileId = 0; fileId < files.get().length; fileId++) {
                this.getFile(fileId).combine(other.getFile(fileId));
            }
        }

        protected FileStatsWritable getFile(int fileId) {
            return (FileStatsWritable) files.get()[fileId];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            files.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            files.readFields(in);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("#FILE_ID\tNUM_VARIANTS");
            for (VariantType value : VariantType.values()) {
                sb.append("\t").append(value);
            }
            for (int i = 0; i < FileStatsWritable.NUM_CHROMOSOMES; i++) {
                sb.append("\t").append(FileStatsWritable.getChromosome(i));
            }
            for (VariantOverlappingStatus value : VariantOverlappingStatus.values()) {
                sb.append("\t").append(value);
            }
            sb.append("\n");

            for (Writable writable : files.get()) {
                FileStatsWritable file = (FileStatsWritable) writable;
                sb.append(file.fileId).append("\t");
                sb.append(file.numVariants);
                for (int i = 0; i < VariantType.values().length; i++) {
                    sb.append("\t").append(file.numByType[i]);
                }
                for (int i = 0; i < FileStatsWritable.NUM_CHROMOSOMES; i++) {
                    sb.append("\t").append(file.numByChromosome[i]);
                }
                for (int i = 0; i < VariantOverlappingStatus.values().length; i++) {
                    sb.append("\t").append(file.numByOverlappingStatus[i]);
                }
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    public static class CheckVariantStatsMapper extends TableMapper<NullWritable, CheckVariantStatsWritable> {

        private CheckVariantStatsWritable stats;
        private int numFiles;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numFiles = context.getConfiguration().getInt(NUM_FILES, -1);
            stats = new CheckVariantStatsWritable(numFiles);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(value.getRow());
            VariantType type = variant.getType();
            int chromosomeIdx = FileStatsWritable.getChromosomeIdx(variant.getChromosome());
            for (Cell cell : value.rawCells()) {
                Integer fileId = extractFileIdOrNull(
                        cell.getQualifierArray(),
                        cell.getQualifierOffset(),
                        cell.getQualifierLength());
                if (fileId == null || fileId > numFiles) {
                    continue;
                }

                String statusStr = getString(cell, HBaseToStudyEntryConverter.FILE_VARIANT_OVERLAPPING_STATUS_IDX);
                VariantOverlappingStatus status = VariantOverlappingStatus.valueFromShortString(statusStr);

                FileStatsWritable file = stats.getFile(fileId);
                file.numByOverlappingStatus[status.ordinal()]++;
                // Only count variants that where originally in the file
                if (status.equals(VariantOverlappingStatus.NONE)) {
                    file.numVariants++;
                    file.numByType[type.ordinal()]++;
                    file.numByChromosome[chromosomeIdx]++;
                    context.getCounter(COUNTER_GROUP_NAME, "file").increment(1);
                }
            }

            context.getCounter(COUNTER_GROUP_NAME, "result").increment(1);
        }

        private String getString(Cell cell, int idx) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable(
                    cell.getValueArray(),
                    cell.getValueOffset(),
                    cell.getValueLength());
            PhoenixHelper.positionAtArrayElement(ptr, idx, PVarchar.INSTANCE, null);
            return Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), stats);
        }
    }

    public static class CheckVariantStatsReducer
            extends Reducer<NullWritable, CheckVariantStatsWritable, NullWritable, CheckVariantStatsWritable> {

        private int numFiles;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            numFiles = context.getConfiguration().getInt(NUM_FILES, -1);
        }

        @Override
        protected void reduce(NullWritable key, Iterable<CheckVariantStatsWritable> values, Context context)
                throws IOException, InterruptedException {
            CheckVariantStatsWritable stats = new CheckVariantStatsWritable(numFiles);

            for (CheckVariantStatsWritable value : values) {
                stats.combine(value);
            }

            context.write(key, stats);
        }
    }


    public static void main(String[] args) {
        int exitCode;
        try {
            exitCode = ToolRunner.run(new CheckVariantStatsDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

}
