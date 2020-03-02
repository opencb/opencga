/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.converters.proto.VariantToVcfSliceConverter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHBaseArchiveDataWriter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.load.VariantHadoopDBWriter;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
    private StudyMetadata sm1;
    private int fileId1;
    private int fileId2;
    private AtomicLong timestamp;
    private static final int NUM_SAMPLES = 4;
    private VariantStorageMetadataManager metadataManager;

    @Before
    public void setUp() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        clearDB(variantStorageManager.getVariantTableName());
        clearDB(variantStorageManager.getArchiveTableName(STUDY_ID));
        //Force HBaseConverter to fail if something goes wrong
        HBaseToVariantConverter.setFailOnWrongVariants(true);
        engine = getVariantStorageEngine();
        dbAdaptor = engine.getDBAdaptor();

//        dbAdaptor.getMetadataManager().getProjectMetadata(engine.getOptions());

        metadataManager = dbAdaptor.getMetadataManager();
        sm1 = metadataManager.createStudy("study_1");
        sm1.getVariantHeader().getComplexLines()
                .add(new VariantFileHeaderComplexLine("INFO", "AD", "", "R", "Integer", Collections.emptyMap()));
        sm1.getVariantHeader().getComplexLines()
                .add(new VariantFileHeaderComplexLine("FORMAT", "AD", "", "R", "Integer", Collections.emptyMap()));

        metadataManager.unsecureUpdateStudyMetadata(sm1);


        fileId1 = createNewFile(sm1);
        fileId2 = createNewFile(sm1);
        timestamp = new AtomicLong(1L);
    }

    public int createNewFile(StudyMetadata sc) throws StorageEngineException {
        int fileId = metadataManager.registerFile(sc.getId(), "file" + RandomUtils.nextInt());
//        int fileId = sc.getFileIds().size() + 1;
        LinkedHashSet<Integer> sampleIds = new LinkedHashSet<>(NUM_SAMPLES);
        for (int samplePos = 0; samplePos < NUM_SAMPLES; samplePos++) {
            int sampleId = samplePos + 1 + (fileId - 1) * NUM_SAMPLES;
            metadataManager.unsecureUpdateSampleMetadata(sc.getId(), new SampleMetadata(sc.getId(), sampleId, "S" + sampleId));
            sampleIds.add(sampleId);
        }
        metadataManager.updateFileMetadata(sc.getId(), fileId, f -> f.setSamples(sampleIds));
        return fileId;
    }

    @Test
    public void testBasicMerge() throws Exception {

        sm1=metadataManager.updateStudyMetadata(sm1.getId(), sm -> {
            sm.getAttributes()
                    .append(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DP,GQX,AD")
                    .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
            return sm;
        });

        int studyId = sm1.getId();
        List<Variant> variants1 = new LinkedList<>();
        variants1.addAll(newVariants("1", 1000, 1000, "A", asList("C", "T"), fileId1, studyId));
        variants1.addAll(newVariants("1", 1002, 1002, "A", asList("C", "G"), fileId1, studyId));

        loadVariantsBasic(sm1, fileId1, variants1);

        List<Variant> variants2 = new LinkedList<>();
        variants2.addAll(newVariants("1", 1000, 1000, "A", asList("C", "G"), fileId2, studyId));
        variants2.addAll(newVariants("1", 1002, 1002, "A", asList("C", "G", "T"), fileId2, studyId));

        loadVariantsBasic(sm1, fileId2, variants2);
        VariantHbaseTestUtils.printVariants(sm1, getVariantStorageEngine().getDBAdaptor(), newOutputUri());

        VariantMerger merger = new VariantMerger(false);
        List<String> expectedSamples = new ArrayList<>(variants1.get(0).getStudies().get(0).getOrderedSamplesName());
        expectedSamples.addAll(variants2.get(0).getStudies().get(0).getOrderedSamplesName());
        merger.setExpectedSamples(expectedSamples);
        merger.setExpectedFormats(asList("GT", VariantMerger.GENOTYPE_FILTER_KEY, "DP", "GQX", "AD"));
        merger.setDefaultValue("DP", ".");
        merger.setDefaultValue("GQX", ".");
        merger.setDefaultValue("AD", ".");
        merger.configure(sm1.getVariantHeader());

        Map<String, Variant> loadedVariants = dbAdaptor.stream(new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "."), new QueryOptions(HBaseToVariantConverter.STUDY_NAME_AS_STUDY_ID, false))
                .collect(Collectors.toMap(Variant::toString, i -> i));

        for (Variant variant : variants1) {
            Variant expected = merger.merge(variant, variants2.stream().filter(v -> v.toString().equals(variant.toString())).collect(Collectors.toList()));
            if (expected.getId() == null) {
                expected.setId(expected.toString());
            }
            assertThat(loadedVariants.keySet(), CoreMatchers.hasItem(expected.toString()));
            Variant actual = loadedVariants.get(expected.toString());
            actual.setAnnotation(null);

            List<AlternateCoordinate> expectedAlternates = expected.getStudies().get(0).getSecondaryAlternates();
            List<AlternateCoordinate> actualAlternates = actual.getStudies().get(0).getSecondaryAlternates();
            if (!expectedAlternates.equals(actualAlternates)) {
                if (new HashSet<>(expectedAlternates).equals(new HashSet<>(actualAlternates))) {
                    assertEquals(2, expectedAlternates.size());
                    List<List<String>> expectedSamplesData = expected.getStudies().get(0).getSamplesData();
                    List<List<String>> actualSamplesData = actual.getStudies().get(0).getSamplesData();
                    for (int i = 0; i < expectedSamplesData.size(); i++) {
                        // Up to 3 alternates. The first alternate must match. Swap second and third alternate (1/2 -> 1/3)
                        String newGT = expectedSamplesData.get(i).get(0).replace('2', 'X').replace('3', '2').replace('X', '3');
                        expectedSamplesData.get(i).set(0, newGT);
                        if (expectedSamplesData.get(i).get(4).equals("1,2,3,0")) {
                            expectedSamplesData.get(i).set(4, "1,2,0,3");
                        } else if (expectedSamplesData.get(i).get(4).equals("1,2,0,3")) {
                            expectedSamplesData.get(i).set(4, "1,2,3,0");
                        } else {
                            fail("Unexpected expected sample data " + expectedSamplesData.get(i).get(4));
                        }
                    }
                    assertEquals(expectedSamplesData, actualSamplesData);
                } else {
                    System.out.println("Expected = " + expected.toJson());
                    System.out.println("Actual   = " + actual.toJson());
                    // This will fail!
                    assertEquals("Wrong set of alternates", expectedAlternates, actualAlternates);
                }
            } else {
                if (!expected.equals(actual)) {
                    System.out.println("Expected = " + expected.toJson());
                    System.out.println("Actual   = " + actual.toJson());
                }

                assertEquals(expected.getStudies().get(0).getSamplesData(), expected.getStudies().get(0).getSamplesData());
            }
        }
        assertEquals(6, loadedVariants.size());

    }

    private void loadVariantsBasic(StudyMetadata sc, int fileId, List<Variant> variants) throws Exception {
        String archiveTableName = engine.getArchiveTableName(sc.getId());
        sc.getAttributes().append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
        VariantStorageMetadataManager metadataManager = this.metadataManager;
        metadataManager.unsecureUpdateStudyMetadata(sc);
        ArchiveTableHelper.createArchiveTableIfNeeded(dbAdaptor.getGenomeHelper().getConf(), archiveTableName);
        VariantTableHelper.createVariantTableIfNeeded(dbAdaptor.getGenomeHelper(), dbAdaptor.getVariantTable());
        metadataManager.updateProjectMetadata(projectMetadata -> {
            if (projectMetadata == null) {
                return new ProjectMetadata("hsapiens", "grch37", 1);
            } else {
                return projectMetadata;
            }
        });

        // Create empty VariantFileMetadata
        VariantFileMetadata fileMetadata = new VariantFileMetadata(String.valueOf(fileId), String.valueOf(fileId));
        fileMetadata.setSampleIds(variants.get(0).getStudies().get(0).getOrderedSamplesName());
        metadataManager.updateVariantFileMetadata(String.valueOf(sc.getId()), fileMetadata);

        ArchiveTableHelper helper = new ArchiveTableHelper(dbAdaptor.getGenomeHelper(), sc.getId(), fileMetadata);


        // Create dummy reader
        VariantSliceReader reader = getVariantSliceReader(variants, sc.getId(), fileId);

        // Writers
        VariantHBaseArchiveDataWriter archiveWriter = new VariantHBaseArchiveDataWriter(helper, archiveTableName, dbAdaptor.getHBaseManager());
        VariantHadoopDBWriter hadoopDBWriter = new VariantHadoopDBWriter(helper, dbAdaptor.getVariantTable(),
                sc.getId(),
                metadataManager, dbAdaptor.getHBaseManager(), false);

        // TaskMetadata
        HadoopLocalLoadVariantStoragePipeline.GroupedVariantsTask task = new HadoopLocalLoadVariantStoragePipeline.GroupedVariantsTask(archiveWriter, hadoopDBWriter, null, null);

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(1).setBatchSize(1).build();
        ParallelTaskRunner<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice> ptr =
                new ParallelTaskRunner<>(reader, task, null, config);
        ptr.run();

        // Mark files as indexed and register new samples in phoenix
        metadataManager.unsecureUpdateStudyMetadata(sc);
        metadataManager.updateFileMetadata(sc.getId(), fileId, f->f.setIndexStatus(TaskMetadata.Status.READY));
        VariantPhoenixHelper phoenixHelper = new VariantPhoenixHelper(dbAdaptor.getGenomeHelper());
        phoenixHelper.registerNewStudy(dbAdaptor.getJdbcConnection(), dbAdaptor.getVariantTable(), sc.getId());
        phoenixHelper.registerNewFiles(dbAdaptor.getJdbcConnection(), dbAdaptor.getVariantTable(), sc.getId(), Collections.singleton(fileId), metadataManager.getFileMetadata(sc.getId(), fileId).getSamples());
        phoenixHelper.registerRelease(dbAdaptor.getJdbcConnection(), dbAdaptor.getVariantTable(), 1);
    }

    private void stageVariants(StudyMetadata study, int fileId, List<Variant> variants) throws Exception {
        String archiveTableName = engine.getArchiveTableName(study.getId());
        ArchiveTableHelper.createArchiveTableIfNeeded(dbAdaptor.getGenomeHelper().getConf(), archiveTableName);

        // Create empty VariantFileMetadata
        VariantFileMetadata fileMetadata = new VariantFileMetadata(String.valueOf(fileId), String.valueOf(fileId));
        fileMetadata.setSampleIds(variants.get(0).getStudies().get(0).getOrderedSamplesName());
        metadataManager.updateVariantFileMetadata(String.valueOf(study.getId()), fileMetadata);

        // Create dummy reader
        VariantSliceReader reader = getVariantSliceReader(variants, study.getId(), fileId);

        // TaskMetadata supplier
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
        VariantHBaseArchiveDataWriter writer = new VariantHBaseArchiveDataWriter(dbAdaptor.getArchiveHelper(study.getId(), fileId), archiveTableName, dbAdaptor.getHBaseManager());

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(1).build();
        ParallelTaskRunner<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice> ptr = new ParallelTaskRunner<>(reader, taskSupplier, writer, config);

        // Execute stage
        System.out.println("Stage start!");
        ptr.run();
        System.out.println("Stage finished!");

    }

    private VariantSliceReader getVariantSliceReader(List<Variant> variants, Integer studyId, Integer fileId) {
        return new VariantSliceReader(100, new VariantReader() {
            boolean empty = false;
            @Override public List<String> getSampleNames() { return variants.get(0).getStudies().get(0).getOrderedSamplesName(); }
            @Override public VariantFileMetadata getVariantFileMetadata() { return null; }
            @Override public List<Variant> read(int i) {
                if (empty) {
                    return Collections.emptyList();
                } else {
                    empty = true;
                    return variants;
                }
            }
        }, studyId, fileId, 10);
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
        return createFile1Variants("1", fileId1, sm1.getId());
    }

    public static List<Variant> createFile1Variants(String chromosome, Integer fileId, Integer studyId) {
        List<Variant> variants = new LinkedList<>();
        variants.add(newVariant(chromosome, 999, 999, "A", "C", fileId, studyId));
        variants.add(newVariant(chromosome, 1000, 1000, "A", "C", fileId, studyId));
        variants.add(newVariant(chromosome, 1002, 1002, "A", "C", fileId, studyId));
        return variants;
    }

    public List<Variant> createFile2Variants() {
        return createFile2Variants("1", fileId2, sm1.getId());
    }

    public static List<Variant> createFile2Variants(String chromosome, Integer fileId, Integer studyId) {
        List<Variant> variants = new LinkedList<>();
        variants.add(newVariant(chromosome, 999, 999, "A", "C", fileId, studyId));
        variants.add(newVariant(chromosome, 1000, 1000, "A", "T", fileId, studyId));
        variants.add(newVariant(chromosome, 1002, 1002, "A", "C", fileId, studyId));
        return variants;
    }

    public static Variant newVariant(String chromosome, int start, int end, String reference, String alternate, Integer fileId, Integer studyId) {
        int pad = (fileId - 1) * NUM_SAMPLES;
        return Variant.newBuilder()
                .setChromosome(chromosome)
                .setStart(start)
                .setEnd(end)
                .setReference(reference)
                .setAlternate(alternate)
                .setStudyId(studyId.toString())
                .setFileId(fileId.toString())
                .setFilter("PASS_" + fileId)
                .setQuality(fileId * 100.0)
                .addAttribute("AD", "1,2")
                .setFormat("GT", "DP", "GQX", "AD")
                .addSample("S" + (1 + pad), "./.", String.valueOf(11 + pad), "0.7", "1,2")
                .addSample("S" + (2 + pad), "1/1", String.valueOf(12 + pad), "0.7", "1,2")
                .addSample("S" + (3 + pad), "0/0", String.valueOf(13 + pad), "0.7", "1,2")
                .addSample("S" + (4 + pad), "1/0", String.valueOf(14 + pad), "0.7", "1,2").build();
    }

    public static List<Variant> newVariants(String chromosome, int start, int end, String reference, List<String> alternates, Integer fileId, Integer studyId) {
        StringBuilder adBuilder = new StringBuilder();
        adBuilder.append(1);
        for (int i = 0; i < alternates.size(); i++) {
            adBuilder.append(',');
            adBuilder.append(i + 2);
        }
        String ad = adBuilder.toString();

        VariantBuilder builder = Variant.newBuilder()
                .setChromosome(chromosome)
                .setStart(start)
                .setEnd(end)
                .setReference(reference)
                .setAlternates(alternates)
                .setStudyId(studyId.toString())
                .setFileId(fileId.toString())
                .setFilter("PASS_" + fileId)
                .setQuality(fileId * 100.0)
                .addAttribute("AD", ad)
                .setFormat("GT", "DP", "GQX", "AD");

        int pad = (fileId - 1) * NUM_SAMPLES;
        int count = 0;
        for (int i = 0; i < NUM_SAMPLES; i++) {
            int allele1 = (count++) % (alternates.size() + 1);
            int allele2 = (count++) % (alternates.size() + 1);
            String gt = allele1 + "/" + allele2;
            builder.addSample("S" + (i + 1 + pad), gt, String.valueOf(i + 1 + pad), "0.7", ad);
        }

        VariantNormalizer normalizer = new VariantNormalizer(true, true);
        return normalizer.apply(Collections.singletonList(builder.build()));
    }

}
