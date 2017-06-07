package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VariantToVcfSliceConverter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHBaseArchiveDataWriter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantMergerTableMapper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created on 23/05/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopDBWriterTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {


    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();
    private HadoopVariantStorageEngine engine;
    private VariantHadoopDBAdaptor dbAdaptor;
    private StudyConfiguration sc1;
    private int fileId1;
    private int fileId2;
    private AtomicLong timestamp;
    private static final int NUM_SAMPLES = 4;

    @Before
    public void setUp() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        clearDB(variantStorageManager.getVariantTableName());
        clearDB(variantStorageManager.getArchiveTableName(STUDY_ID));
        //Force HBaseConverter to fail if something goes wrong
        HBaseToVariantConverter.setFailOnWrongVariants(true);
        engine = getVariantStorageEngine();
        dbAdaptor = engine.getDBAdaptor();

        sc1 = new StudyConfiguration(1, "study_1");

        fileId1 = createNewFile(sc1);
        fileId2 = createNewFile(sc1);

        timestamp = new AtomicLong(1L);
    }

    public int createNewFile(StudyConfiguration sc) {
        int fileId = sc.getFileIds().size() + 1;
        LinkedHashSet<Integer> sampleIds = new LinkedHashSet<>(NUM_SAMPLES);
        for (int samplePos = 0; samplePos < NUM_SAMPLES; samplePos++) {
            int sampleId = samplePos + 1 + (fileId - 1) * NUM_SAMPLES;
            sc.getSampleIds().put("S" + sampleId, sampleId);
            sampleIds.add(sampleId);
        }
        sc.getFileIds().put("file" + fileId, fileId);
        sc.getSamplesInFiles().put(fileId, sampleIds);
        return fileId;
    }

    @Test
    public void test() throws Exception {

        sc1.getAttributes().put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DP,GQX");
        loadVariants(sc1, fileId1, createFile1Variants());
        loadVariants(sc1, fileId2, createFile2Variants());

        VariantHbaseTestUtils.printVariants(sc1, getVariantStorageEngine().getDBAdaptor(), newOutputUri());

        Map<String, Variant> variants = dbAdaptor.stream().collect(Collectors.toMap(Variant::toString, i -> i));
        Assert.assertEquals(4, variants.size());

    }

    private void loadVariants(StudyConfiguration studyConfiguration, int fileId, List<Variant> variants) throws Exception {
        stageVariants(studyConfiguration, fileId, variants);
        mergeVariants(studyConfiguration, fileId);
    }

    private void stageVariants(StudyConfiguration study, int fileId, List<Variant> variants) throws Exception {
        String archiveTableName = engine.getArchiveTableName(study.getStudyId());
        ArchiveTableHelper.createArchiveTableIfNeeded(dbAdaptor.getGenomeHelper(), archiveTableName);

        // Create empty VariantSource
        dbAdaptor.getVariantSourceDBAdaptor().update(new VariantSource(String.valueOf(fileId), String.valueOf(fileId), String.valueOf(study.getStudyId()), study.getStudyName()));

        // Create dummy reader
        VariantSliceReader reader = new VariantSliceReader(100, new VariantReader() {
            boolean empty = false;
            @Override public List<String> getSampleNames() { return variants.get(0).getStudies().get(0).getOrderedSamplesName(); }
            @Override public String getHeader() { return null; }
            @Override public List<Variant> read(int i) {
                if (empty) {
                    return Collections.emptyList();
                } else {
                    empty = true;
                    return variants;
                }
            }
        });

        // Task supplier
        Supplier<ParallelTaskRunner.Task<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice>> taskSupplier = () -> {
            VariantToVcfSliceConverter converter = new VariantToVcfSliceConverter();
            return list -> {
                System.out.println("list.size() = " + list.size());
                List<VcfSliceProtos.VcfSlice> vcfSlice = new ArrayList<>(list.size());
                for (ImmutablePair<Long, List<Variant>> pair : list) {
                    vcfSlice.add(converter.convert(pair.getRight(), pair.getLeft().intValue()));
                }
                return vcfSlice;
            };
        };

        // Writer
        VariantHBaseArchiveDataWriter writer = new VariantHBaseArchiveDataWriter(dbAdaptor.getArchiveHelper(study.getStudyId(), fileId), archiveTableName, dbAdaptor.getHBaseManager());

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(1).build();
        ParallelTaskRunner<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice> ptr = new ParallelTaskRunner<>(reader, taskSupplier, writer, config);

        // Execute stage
        System.out.println("Stage start!");
        ptr.run();
        System.out.println("Stage finished!");

    }

    private void mergeVariants(StudyConfiguration study, Integer ...fileIds) throws Exception {
        mergeVariants(study, Arrays.asList(fileIds));
    }

    private void mergeVariants(StudyConfiguration study, List<Integer> fileIds) throws Exception {

        VariantTableHelper.createVariantTableIfNeeded(dbAdaptor.getGenomeHelper(), DB_NAME);
        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(study, new QueryOptions());
        VariantMergerTableMapper mapper = new VariantMergerTableMapper();


        // Create SCAN
        Scan scan = new Scan();
        FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (Integer id : fileIds) {// specify return columns (file IDs)
            filter.addFilter(new ColumnRangeFilter(Bytes.toBytes(ArchiveTableHelper.getColumnName(id)), true,
                    Bytes.toBytes(ArchiveTableHelper.getColumnName(id)), true));
        }
        filter.addFilter(new ColumnPrefixFilter(GenomeHelper.VARIANT_COLUMN_B_PREFIX));
        scan.setFilter(filter);

        // Configure mapper
        String archiveTableName = engine.getArchiveTableName(study.getStudyId());
        Configuration conf = dbAdaptor.getGenomeHelper().getConf();
        VariantTableHelper.setStudyId(conf, study.getStudyId());
        VariantTableHelper.setAnalysisTable(conf, DB_NAME);
        VariantTableHelper.setArchiveTable(conf, archiveTableName);
        conf.setLong(AbstractAnalysisTableDriver.TIMESTAMP, timestamp.getAndIncrement());
        conf.setStrings(VariantStorageEngine.Options.FILE_ID.key(), fileIds.stream().map(Object::toString).collect(Collectors.joining(",")));

        // Mock context
        Map<String, Counter> counterMap = new TreeMap<>();
        Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation>.Context context = mockContext(counterMap);

        // Iterate
        try (Table table = dbAdaptor.getConnection().getTable(TableName.valueOf(archiveTableName))) {
            ResultScanner scanner = table.getScanner(scan);
            mapper.setup(context);
            Result result = scanner.next();
            while (result != null) {
                mapper.map(new ImmutableBytesWritable(result.getRow()), result, context);
                result = scanner.next();
            }
            mapper.cleanup(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Print counters
        for (Map.Entry<String, Counter> entry : counterMap.entrySet()) {
            System.out.println('\t' + entry.getKey() + '\t' + entry.getValue().getValue());
        }


        // Mark files as indexed and register new samples in phoenix
        study.getIndexedFiles().addAll(fileIds);
        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(study, QueryOptions.empty());
        VariantPhoenixHelper phoenixHelper = new VariantPhoenixHelper(dbAdaptor.getGenomeHelper());
        phoenixHelper.registerNewStudy(dbAdaptor.getJdbcConnection(), DB_NAME, study.getStudyId());
        phoenixHelper.registerNewSamples(dbAdaptor.getJdbcConnection(), DB_NAME, study.getStudyId(), fileIds.stream().flatMap(i -> study.getSamplesInFiles().get(i).stream()).collect(Collectors.toSet()));
    }

    private Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation>.Context mockContext(Map<String, Counter> counterMap) throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation>.Context context = Mockito.mock(Mapper.Context.class);
        Mockito.doReturn(new TaskAttemptID("", 1, TaskType.MAP, 1, 1)).when(context).getTaskAttemptID();
        Mockito.doReturn(dbAdaptor.getGenomeHelper().getConf()).when(context).getConfiguration();
        Mockito.doAnswer(invocation -> counterMap.computeIfAbsent(invocation.getArgument(0) + "_" + invocation.getArgument(1), (k) -> new GenericCounter(k, k)))
                .when(context).getCounter(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        Mockito.doAnswer(invocation -> counterMap.computeIfAbsent(invocation.getArgument(0).toString(), (k) -> new GenericCounter(k, k))).when(context).getCounter(ArgumentMatchers.any());
        Mockito.doAnswer(invocation -> dbAdaptor.getHBaseManager().act(((ImmutableBytesWritable) invocation.getArgument(0)).get(), table -> {
            table.put(((Put) invocation.getArgument(1)));
            return 0;
        })).when(context).write(ArgumentMatchers.any(), ArgumentMatchers.any());

        return context;
    }

    public List<Variant> createFile1Variants() {
        return createFile1Variants("1", fileId1, sc1.getStudyId());
    }

    public static List<Variant> createFile1Variants(String chromosome, Integer fileId, Integer studyId) {
        List<Variant> variants = new LinkedList<>();
        variants.add(newVariant(chromosome, 999, 999, "A", "C", fileId, studyId));
        variants.add(newVariant(chromosome, 1000, 1000, "A", "C", fileId, studyId));
        variants.add(newVariant(chromosome, 1002, 1002, "A", "C", fileId, studyId));
        return variants;
    }

    public List<Variant> createFile2Variants() {
        return createFile2Variants("1", fileId2, sc1.getStudyId());
    }

    public static List<Variant> createFile2Variants(String chromosome, Integer fileId, Integer studyId) {
        List<Variant> variants = new LinkedList<>();
        variants.add(newVariant(chromosome, 999, 999, "A", "C", fileId, studyId));
        variants.add(newVariant(chromosome, 1000, 1000, "A", "T", fileId, studyId));
        variants.add(newVariant(chromosome, 1002, 1002, "A", "C", fileId, studyId));
        return variants;
    }

    public static Variant newVariant(String chromosome, int start, int end, String reference, String alternate, Integer fileId, Integer studyId) {
        Variant variant;
        StudyEntry sourceEntry;
        variant = new Variant(chromosome, start, end, reference, alternate);
        sourceEntry = new StudyEntry(fileId.toString(), studyId.toString());
        sourceEntry.setFiles(Collections.singletonList(new FileEntry(fileId.toString(), null, Collections.singletonMap(StudyEntry.FILTER, "PASS"))));
        variant.addStudyEntry(sourceEntry);

        int pad = (fileId - 1) * NUM_SAMPLES;
        addSample(variant, "S" + (1 + pad), "./.", String.valueOf(11 + pad), "0.7");
        addSample(variant, "S" + (2 + pad), "1/1", String.valueOf(12 + pad), "0.7");
        addSample(variant, "S" + (3 + pad), "0/0", String.valueOf(13 + pad), "0.7");
        addSample(variant, "S" + (4 + pad), "1/0", String.valueOf(14 + pad), "0.7");

        return variant;
    }

    @SuppressWarnings("unchecked")
    public static void addSample(Variant variant, String s1, String gt, String dp, String gqx) {
        variant.getStudies().get(0).addSampleData(s1, ((Map) new ObjectMap("GT", gt).append("DP", dp).append("GQX", gqx)));
    }

}
