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

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateParams;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsTest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils.printVariants;
import static org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils.printVariantsFromVariantsTable;
import static org.opencb.opencga.storage.hadoop.variant.gaps.FillMissingFromArchiveTask.VARIANT_COLUMN_B_PREFIX;

/**
 * Created on 21/01/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(LongTests.class)
@Ignore
public class VariantHadoopMultiSampleTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    public static final Set<VariantType> VARIANT_TYPES = HadoopVariantStorageEngine.TARGET_VARIANT_TYPE_SET;

    // Variants that are wrong in the platinum files that should not be included
    private static final HashSet<String> PLATINUM_SKIP_VARIANTS = new HashSet<>();

    @Before
    public void setUp() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        clearDB(variantStorageManager.getDBName());
        //Force HBaseConverter to fail if something goes wrong
        HBaseToVariantConverter.setFailOnWrongVariants(true);
    }

    @After
    public void tearDown() throws Exception {
        printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
    }

    @Override
    public Map<String, ?> getOtherStorageConfigurationOptions() {
        return new ObjectMap(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), true).append(VariantStorageOptions.ANNOTATE.key(), false);
    }

    public VariantFileMetadata loadFile(String resourceName, StudyMetadata studyMetadata) throws Exception {
        return loadFile(resourceName, studyMetadata, null);
    }

    public VariantFileMetadata loadFile(String resourceName, StudyMetadata studyMetadata, Map<? extends String, ?> otherParams) throws Exception {
        return loadFile(resourceName, studyMetadata, otherParams, true, true, true);
    }

    public VariantFileMetadata loadFile(String resourceName, StudyMetadata studyMetadata,
                                        Map<? extends String, ?> otherParams, boolean doTransform, boolean loadArchive, boolean loadVariant)
            throws Exception {
        return VariantHbaseTestUtils.loadFile(getVariantStorageEngine(), DB_NAME, outputUri, resourceName, studyMetadata,
                otherParams, doTransform, loadArchive, loadVariant);
    }

    @Test
    public void testTwoFiles() throws Exception {

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        loadFile("s1.genome.vcf", studyMetadata, new ObjectMap());
        checkArchiveTableTimeStamp(dbAdaptor);

        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        loadFile("s2.genome.vcf", studyMetadata, new ObjectMap());

        checkArchiveTableTimeStamp(dbAdaptor);
        printVariants(studyMetadata, dbAdaptor, newOutputUri());


        checkLoadedFilesS1S2(studyMetadata, dbAdaptor);

    }

    @Test
    public void testTwoFilesBasicFillMissing() throws Exception {
        ObjectMap params = new ObjectMap();
        params.put(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
        params.put(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro");

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        loadFile("s1.genome.vcf", studyMetadata, params);
        checkArchiveTableTimeStamp(dbAdaptor);

        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        loadFile("s2.genome.vcf", studyMetadata, params);

        printVariants(studyMetadata, dbAdaptor, newOutputUri());
        checkArchiveTableTimeStamp(dbAdaptor);

        getVariantStorageEngine().aggregate(studyMetadata.getName(), new VariantAggregateParams(false, false),
                new ObjectMap("local", true)
                        .append(HadoopVariantStorageOptions.FILL_MISSING_SIMPLIFIED_MULTIALLELIC_VARIANTS.key(), false)
        );
        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        printVariants(studyMetadata, dbAdaptor, newOutputUri());

        checkLoadedFilesS1S2(studyMetadata, dbAdaptor);

    }

    @Test
    public void testTwoFilesBasicAggregateLimitArchiveRefFields() throws Exception {
        ObjectMap params = new ObjectMap();
        params.put(HadoopVariantStorageOptions.ARCHIVE_FIELDS.key(), "QUAL,FORMAT:DP,INFO:DP");
        params.put(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
        params.put(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro");

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        loadFile("s1.genome.vcf", studyMetadata, params);
        checkArchiveTableTimeStamp(dbAdaptor);

        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        loadFile("s2.genome.vcf", studyMetadata, params);

        printVariants(studyMetadata, dbAdaptor, newOutputUri());
        checkArchiveTableTimeStamp(dbAdaptor);

        ((VariantStorageEngine) getVariantStorageEngine()).aggregateFamily(studyMetadata.getName(), new VariantAggregateFamilyParams(Arrays.asList("s1", "s2"), false), new ObjectMap("local", true));
        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        printVariants(studyMetadata, dbAdaptor, newOutputUri());

        checkLoadedFilesS1S2(studyMetadata, dbAdaptor);

        dbAdaptor.getHBaseManager().act(dbAdaptor.getArchiveTableName(1), table -> {
            for (Result r : table.getScanner(new Scan())) {
                for (Map.Entry<byte[], byte[]> entry : r.getFamilyMap(GenomeHelper.COLUMN_FAMILY_BYTES).entrySet()) {
                    if (Bytes.toString(entry.getKey()).endsWith(ArchiveTableHelper.REF_COLUMN_SUFIX)) {
                        VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(entry.getValue());
                        List<String> formats = vcfSlice.getFields().getFormatsList();
                        assertThat(formats, anyOf(
                                equalTo(Arrays.asList("GT:DP", "GT")),
                                equalTo(Arrays.asList("GT:DP")),
                                equalTo(Arrays.asList("GT"))));
                    }
                }
            }
        });
    }


    @Test
    public void testTwoFilesBasicAggregateNoneArchiveRefFields() throws Exception {
        ObjectMap params = new ObjectMap();
        params.put(HadoopVariantStorageOptions.ARCHIVE_FIELDS.key(), VariantQueryUtils.NONE);
        params.put(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
        params.put(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro");

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        loadFile("s1.genome.vcf", studyMetadata, params);
        checkArchiveTableTimeStamp(dbAdaptor);

        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        loadFile("s2.genome.vcf", studyMetadata, params);

        printVariants(studyMetadata, dbAdaptor, newOutputUri());
        checkArchiveTableTimeStamp(dbAdaptor);

        ((VariantStorageEngine) getVariantStorageEngine()).aggregateFamily(studyMetadata.getName(), new VariantAggregateFamilyParams(Arrays.asList("s1", "s2"), false), new ObjectMap("local", true));
        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        printVariants(studyMetadata, dbAdaptor, newOutputUri());

        dbAdaptor.getHBaseManager().act(dbAdaptor.getArchiveTableName(1), table -> {
            for (Result r : table.getScanner(new Scan())) {
                for (Map.Entry<byte[], byte[]> entry : r.getFamilyMap(GenomeHelper.COLUMN_FAMILY_BYTES).entrySet()) {
                    assertFalse(Bytes.toString(entry.getKey()).endsWith(ArchiveTableHelper.REF_COLUMN_SUFIX));
                }
            }
        });
    }

    @Test
    public void testTwoFiles_reverse() throws Exception {

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        loadFile("s2.genome.vcf", studyMetadata, new ObjectMap());
        checkArchiveTableTimeStamp(dbAdaptor);

        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        loadFile("s1.genome.vcf", studyMetadata, new ObjectMap());

        checkArchiveTableTimeStamp(dbAdaptor);
        printVariants(studyMetadata, dbAdaptor, newOutputUri());


        checkLoadedFilesS1S2(studyMetadata, dbAdaptor);

    }

    @Test
    public void testTwoFilesConcurrent() throws Exception {

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        ObjectMap options = variantStorageManager.getOptions();
        options.put(VariantStorageOptions.TRANSFORM_FORMAT.key(), "proto");
        options.put(VariantStorageOptions.STUDY.key(), studyMetadata.getName());

        List<URI> inputFiles = Arrays.asList(getResourceUri("s1.genome.vcf"), getResourceUri("s2.genome.vcf"));
        List<StoragePipelineResult> index = variantStorageManager.index(inputFiles, outputUri, true, true, true);


        VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();

        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());

        for (StoragePipelineResult storagePipelineResult : index) {
            System.out.println(storagePipelineResult);
        }

        printVariants(studyMetadata, dbAdaptor, newOutputUri());

        for (Variant variant : dbAdaptor) {
            System.out.println("variant = " + variant);
        }
        checkLoadedFilesS1S2(studyMetadata, dbAdaptor);

        assertThat(dbAdaptor.getMetadataManager().getIndexedFiles(studyMetadata.getId()), hasItems(1, 2));
    }

    @Test
    public void testMultipleFilesProtoConcurrent() throws Exception {
        List<URI> protoFiles = new LinkedList<>();

        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        ObjectMap options = variantStorageManager.getOptions();
        options.put(VariantStorageOptions.TRANSFORM_FORMAT.key(), "proto");
        options.put(VariantStorageOptions.STUDY.key(), STUDY_NAME);

        List<URI> inputFiles = new LinkedList<>();

//        for (int fileId = 12877; fileId <= 12893; fileId++) {
        for (int fileId = 12877; fileId <= 12879; fileId++) {
            String fileName = "platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
//            inputFiles.add(getResourceUri(fileName));
            List<StoragePipelineResult> results = variantStorageManager.index(Collections.singletonList(getResourceUri(fileName)), outputUri, true, true, false);
            protoFiles.add(results.get(0).getTransformResult());

        }

        // dbAdaptor.getStudyMetadataManager().updateStudyMetadata(studyMetadata, null);

        protoFiles = protoFiles.subList(0, 2); // TODO remove

        List<StoragePipelineResult> index2 = variantStorageManager.index(protoFiles, outputUri, false, false, true);

        System.out.println(index2);

    }

    @Test
    public void testMultipleFilesConcurrentMergeBasic() throws Exception {
        testMultipleFilesConcurrent(new ObjectMap(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                .append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                .append(HadoopVariantStorageOptions.HADOOP_LOAD_FILES_IN_PARALLEL.key(), 5));
    }

    @Test
    public void testMultipleFilesConcurrentMergeBasicMultipleBatches() throws Exception {
        testMultipleFilesConcurrent(new ObjectMap(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                .append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                .append(HadoopVariantStorageOptions.HADOOP_LOAD_FILES_IN_PARALLEL.key(), 5)
                .append(HadoopVariantStorageOptions.ARCHIVE_FILE_BATCH_SIZE.key(), 5));

        ArchiveRowKeyFactory rowKeyFactory = new ArchiveRowKeyFactory(1000, 5);

        HadoopVariantStorageEngine engine = getVariantStorageEngine();
        VariantHadoopDBAdaptor dbAdaptor = engine.getDBAdaptor();
        Integer count = dbAdaptor.getHBaseManager().act(engine.getArchiveTableName(STUDY_ID), table -> {
            int numBlocks = 0;
            for (Result result : table.getScanner(GenomeHelper.COLUMN_FAMILY_BYTES)) {
                numBlocks++;
                int batch = rowKeyFactory.extractFileBatchFromBlockId(Bytes.toString(result.getRow()));
                for (byte[] column : result.getFamilyMap(GenomeHelper.COLUMN_FAMILY_BYTES).keySet()) {
                    if (!Bytes.startsWith(column, VARIANT_COLUMN_B_PREFIX)) {
                        int fileId = ArchiveTableHelper.getFileIdFromNonRefColumnName(column);
                        int expectedBatch = rowKeyFactory.getFileBatch(fileId);
                        assertEquals(expectedBatch, batch);
                    }
                }
            }
            return numBlocks;
        });
        assertTrue(count > 0);

    }

    public void testMultipleFilesConcurrent(ObjectMap extraParams) throws Exception {

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();

        List<URI> inputFiles = new LinkedList<>();

        for (int fileId = 12877; fileId <= 12893; fileId++) {
            String fileName = "platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
            inputFiles.add(getResourceUri(fileName));
        }

        ObjectMap options = variantStorageManager.getOptions();
        options.put(VariantStorageOptions.TRANSFORM_FORMAT.key(), "proto");
//        options.put(VariantStorageEngine.Options.STUDY_ID.key(), studyMetadata.getStudyId());
        options.put(VariantStorageOptions.STUDY.key(), studyMetadata.getName());
//        options.put(VariantStorageEngine.Options.EXTRA_FORMAT_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DP,AD");
        options.putAll(extraParams);
        List<StoragePipelineResult> index = variantStorageManager.index(inputFiles, outputUri, true, true, true);

        for (StoragePipelineResult storagePipelineResult : index) {
            System.out.println(storagePipelineResult);
        }

        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        printVariants(studyMetadata, dbAdaptor, newOutputUri(1));

//        checkLoadedVariants(expectedVariants, dbAdaptor, PLATINUM_SKIP_VARIANTS);


        for (Variant variant : dbAdaptor) {
            System.out.println("variant = " + variant);
        }

        studyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        System.out.println("StudyMetadata = " + studyMetadata);

        Set<Integer> loadedFiles = dbAdaptor.getMetadataManager().getIndexedFiles(studyMetadata.getId());
        System.out.println("loadedFiles = " + loadedFiles);
        for (int fileId = 1; fileId <= 17; fileId++) {
            assertThat(loadedFiles, hasItem(fileId));
        }
        for (Integer loadedFile : loadedFiles) {
            VariantFileMetadata variantFileMetadata = dbAdaptor.getMetadataManager().getVariantFileMetadata(studyMetadata.getId(), loadedFile, null).first();
            assertNotNull(variantFileMetadata);
        }

        printVariants(studyMetadata, dbAdaptor, newOutputUri(1));
    }

    @Test
    @Ignore
    // FIXME
    public void testTwoFilesFailOne() throws Exception {

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
//        ObjectMap otherParams = new ObjectMap(VariantMergerTableMapperFail.SLICE_TO_FAIL, "00000_1_000000000011"); // FIXME
        ObjectMap otherParams = new ObjectMap();
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        try {
            loadFile("s1.genome.vcf", studyMetadata, otherParams);
            fail();
        } catch (StoragePipelineException e) {
            studyMetadata = metadataManager.getStudyMetadata(STUDY_NAME);
            System.out.println("studyMetadata: " + studyMetadata);
            System.out.println(metadataManager.getIndexedFiles(studyMetadata.getId()));
            e.printStackTrace();
        }
        Integer fileId = metadataManager.getFileId(studyMetadata.getId(), "s1.genome.vcf");
        System.out.println("fileId = " + fileId);
//        otherParams.put(VariantMergerTableMapperFail.SLICE_TO_FAIL, "_"); // FIXME
        loadFile("s1.genome.vcf.variants.proto.gz", studyMetadata, otherParams, false, false, true);
        checkArchiveTableTimeStamp(dbAdaptor);
        loadFile("s2.genome.vcf", studyMetadata, new ObjectMap());
        checkArchiveTableTimeStamp(dbAdaptor);

//        printVariants(studyMetadata, dbAdaptor, newOutputUri());

        checkLoadedFilesS1S2(studyMetadata, dbAdaptor);

        TaskMetadata[] tasks = Iterators.toArray(metadataManager.taskIterator(studyMetadata.getId()), TaskMetadata.class);
        assertEquals(2, tasks.length);

        TaskMetadata batch = tasks[0];
        assertEquals(TaskMetadata.Status.READY, batch.currentStatus());
        assertThat(batch.getStatus().values(), hasItem(TaskMetadata.Status.ERROR));

        batch = tasks[1];
        assertEquals(TaskMetadata.Status.READY, batch.currentStatus());
        assertThat(batch.getStatus().values(),
                not(hasItem(TaskMetadata.Status.ERROR)));


    }

    public void checkLoadedFilesS1S2(StudyMetadata studyMetadata, VariantHadoopDBAdaptor dbAdaptor) throws IOException, StorageEngineException {

        Path path = Paths.get(getResourceUri("s1_s2.genome.vcf"));
        VariantFileMetadata fileMetadata = variantReaderUtils.readVariantFileMetadata(path, null);

        VariantReader variantReader = new VariantVcfHtsjdkReader(path, fileMetadata.toVariantStudyMetadata(STUDY_NAME));
        variantReader.open();
        variantReader.pre();
        Map<String, Variant> expectedVariants = new VariantNormalizer(true, true)
                .apply(variantReader.read(1000))
                .stream()
                .collect(Collectors.toMap(Variant::toString, v -> v));
        variantReader.post();
        variantReader.close();

        System.out.println("studyMetadata = " + studyMetadata);
        Map<String, Variant> variants = new HashMap<>();
        for (Variant variant : dbAdaptor.iterable(new VariantQuery().includeSampleAll(), new QueryOptions())) {
            String v = variant.toString();
            assertFalse(variants.containsKey(v));
            variants.put(v, variant);
//            VariantAnnotation a = variant.getAnnotation();
//            variant.setAnnotation(null);
//            System.out.println(variant.toJson());
//            variant.setAnnotation(a);
        }
        String studyName = studyMetadata.getName();

        // TODO: Add more asserts
        // TODO: Update with last changes!
        /*
        #CHROM   POS     REF                   ALT                 s1      s2
        1        10013   T                     C                   0/1     0/0
        1        10014   A                     T,G                 0/1     0/2
        1        10030   T                     G                   0/0     0/1
        1        10031   T                     G                   0/1     1/1
        1        10032   A                     G                   0/1     0/0
        1        10064   C                     CTTTTT              0/0     0/1
        1        11000   T                     G                   1/1     0/1
        1        12000   T                     G                   1/1     .
        1        12081   ATTACTTACTTTTTTTTTTT  ATTTTTTT,ATTACTTAC  1/2     .
        1        12094   C                     .                   0/0     .
        1        13000   T                     G                   0/0     0/1
        1        13488   G                     TGAAGTATGCAGGGT,T   1/1     0/2
        1        13563   TACACACACAC           TACACAC,T           1/2     0/0
        
        */

        boolean missingUpdated = studyMetadata.getAttributes().getBoolean(HadoopVariantStorageEngine.MISSING_GENOTYPES_UPDATED, false);
        String defaultGenotype = missingUpdated ? "0/0" : "?/?";

        List<String> errors = new ArrayList<>();

        String[] samples = {"s1", "s2"};
        String[] format = {VariantMerger.GT_KEY};
        // TODO: Add FT to the merged vcf!
//        String[] format = {VariantMerger.GT_KEY, VariantMerger.GENOTYPE_FILTER_KEY};
        for (String key : expectedVariants.keySet()) {
            if (variants.containsKey(key)) {
                Variant variant = variants.get(key);
                StudyEntry actualStudyEntry = variant.getStudy(studyName);
                StudyEntry expectedStudyEntry = expectedVariants.get(key).getStudies().get(0);
                for (String sample : samples) {
                    for (String formatKey : format) {
                        String expected = expectedStudyEntry.getSampleData(sample, formatKey);
                        if (expected.equals("./.")) {
                            expected = ".";
                        }
                        String actual = actualStudyEntry.getSampleData(sample, formatKey);
                        if (actual.equals("./.")) {
                            actual = ".";
                        }
                        if (!expected.equals(actual)) {
                            if (missingUpdated && actual.equals(defaultGenotype)) {
                                errors.add("In variant " + key + " wrong " + formatKey + " for sample " + sample + ". Expected: " + expected + ", Actual: " + actual);
                            }
                        }
                    }
                }
                int expectedFiles = 0;
                if (!actualStudyEntry.getSampleData("s1", "GT").equals(defaultGenotype)) {
                    expectedFiles++;
                }
                if (!actualStudyEntry.getSampleData("s2", "GT").equals(defaultGenotype)) {
                    expectedFiles++;
                }
                if (defaultGenotype.equals("0/0") && variant.toString().equals("1:10013:T:C")) {
                    // Special case
                    // From file s2.genome.vcf there is a variant with GT 0/0 which has an associated file
                    expectedFiles++;
                }
                assertEquals(key, expectedFiles, actualStudyEntry.getFiles().size());
                if (missingUpdated) {
                    assertEquals(key, expectedStudyEntry.getSecondaryAlternates().size(), actualStudyEntry.getSecondaryAlternates().size());
                    assertEquals(key,
                            expectedStudyEntry.getSecondaryAlternates().stream().map(AlternateCoordinate::getAlternate).collect(Collectors.toList()),
                            actualStudyEntry.getSecondaryAlternates().stream().map(AlternateCoordinate::getAlternate).collect(Collectors.toList()));
                }
            } else {
                errors.add("Missing variant! " + key);
            }
        }
        for (String key : variants.keySet()) {
            if (!expectedVariants.containsKey(key)) {
                errors.add("Extra variant! " + key);
            }
        }

        if (!errors.isEmpty()) {
            errors.forEach(System.out::println);
            assertThat(errors, not(hasItem(any(String.class))));
        }

//        assertEquals(16, variants.size());
//        assertTrue(variants.containsKey("1:10013:T:C"));
//        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals("0/0", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));
//
//        assertTrue(variants.containsKey("1:10014:A:T"));
//        assertEquals("0/1", variants.get("1:10014:A:T").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals("0/2", variants.get("1:10014:A:T").getStudy(studyName).getSampleData("s2", "GT"));
//
//        assertTrue(variants.containsKey("1:10014:A:G"));
//        assertEquals("0/2", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals("0/1", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s2", "GT"));
//
//        assertTrue(variants.containsKey("1:10030:T:G"));
//        assertEquals("0/0", variants.get("1:10030:T:G").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals("0/1", variants.get("1:10030:T:G").getStudy(studyName).getSampleData("s2", "GT"));
//
//        assertTrue(variants.containsKey("1:10031:T:G"));
//        assertEquals("0/1", variants.get("1:10031:T:G").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals("0/1", variants.get("1:10031:T:G").getStudy(studyName).getSampleData("s2", "GT"));
//
//        assertTrue(variants.containsKey("1:10032:A:G"));
//        assertEquals("1", variants.get("1:10032:A:G").getStudy(studyName).getFiles().get(0).getAttributes().get("PASS"));
//        assertEquals("0/1", variants.get("1:10032:A:G").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals("PASS", variants.get("1:10032:A:G").getStudy(studyName).getSampleData("s1", VariantMerger.GENOTYPE_FILTER_KEY));
//        assertEquals("0/0", variants.get("1:10032:A:G").getStudy(studyName).getSampleData("s2", "GT"));
//        assertEquals("LowGQX", variants.get("1:10032:A:G").getStudy(studyName).getSampleData("s2", VariantMerger.GENOTYPE_FILTER_KEY));
//
//        assertTrue(variants.containsKey("1:11000:T:G"));
//        assertEquals("1/1", variants.get("1:11000:T:G").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals("0/1", variants.get("1:11000:T:G").getStudy(studyName).getSampleData("s2", "GT"));
//
//        assertTrue(variants.containsKey("1:12000:T:G"));
//        assertEquals("1/1", variants.get("1:12000:T:G").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals(".", variants.get("1:12000:T:G").getStudy(studyName).getSampleData("s1", VariantMerger.GENOTYPE_FILTER_KEY));
//        assertEquals("0/0", variants.get("1:12000:T:G").getStudy(studyName).getSampleData("s2", "GT"));
//        assertEquals("HighDPFRatio;LowGQX", variants.get("1:12000:T:G").getStudy(studyName).getSampleData("s2", VariantMerger.GENOTYPE_FILTER_KEY));
//
//        assertTrue(variants.containsKey("1:13000:T:G"));
//        assertEquals("0/0", variants.get("1:13000:T:G").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals("0/1", variants.get("1:13000:T:G").getStudy(studyName).getSampleData("s2", "GT"));
    }

    @Test
    public void testMultiSampleFile() throws Exception {

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        loadFile("s1_s2.genome.vcf", studyMetadata, new ObjectMap(HadoopVariantStorageOptions.VARIANT_TABLE_LOAD_REFERENCE.key(), true));
        checkArchiveTableTimeStamp(dbAdaptor);


        printVariants(studyMetadata, dbAdaptor, newOutputUri());

        int numHomRef = 0;
        for (Variant variant : dbAdaptor.iterable(new VariantQuery().includeSample(ParamConstants.ALL), new QueryOptions())) {
            StudyEntry study = variant.getStudies().get(0);
            for (String s : study.getSamplesName()) {
                String gt = study.getSampleData(s, "GT");
                assertNotEquals(GenotypeClass.UNKNOWN_GENOTYPE, gt);
                if (GenotypeClass.HOM_REF.test(gt)) {
                    numHomRef++;
                    assertTrue(StringUtils.isNumeric(study.getSampleData(s, "DP")));
                }
            }
        }
        assertNotEquals(0, numHomRef);
    }

    @Test
    public void testPlatinumFilesOneByOne() throws Exception {
        testPlatinumFilesOneByOne(new ObjectMap(), 4);
    }

    @Test
    public void testPlatinumFilesOneByOne_extraFields() throws Exception {
        testPlatinumFilesOneByOne(new ObjectMap()
                .append(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DP,GQX,MQ"), 6);
    }

    @Test
    public void testPlatinumFilesOneByOne_MergeBasic() throws Exception {
        StudyMetadata studyMetadata = testPlatinumFilesOneByOne(new ObjectMap()
                        .append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                /*.append(VariantStorageEngine.Options.EXTRA_FORMAT_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DP,GQX,MQ")*/, 4);


        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        List<String> samples = new ArrayList<>(metadataManager.getIndexedSamplesMap(studyMetadata.getId()).keySet());

        FillGapsTest.fillGaps(variantStorageEngine, studyMetadata, samples.subList(0, samples.size() / 2));
        printVariants(studyMetadata, dbAdaptor, newOutputUri());

        FillGapsTest.fillGaps(variantStorageEngine, studyMetadata, samples.subList(samples.size() / 2, samples.size()));
        printVariants(studyMetadata, dbAdaptor, newOutputUri());

        FillGapsTest.fillGaps(variantStorageEngine, studyMetadata, samples);
        printVariants(studyMetadata, dbAdaptor, newOutputUri());


    }

    public StudyMetadata testPlatinumFilesOneByOne(ObjectMap otherParams, int maxFilesLoaded) throws Exception {

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        List<VariantFileMetadata> filesMetadata = new LinkedList<>();
        Set<String> expectedVariants = new HashSet<>();

        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();


        for (int i = 12877; i <= 12893; i++) {
            ObjectMap params = new ObjectMap();
            params.putAll(otherParams);
            String fileName = "1K.end.platinum-genomes-vcf-NA" + i + "_S1.genome.vcf.gz";
            String resourceName = "platinum/" + fileName;
            VariantFileMetadata fileMetadata = loadFile(resourceName, studyMetadata, params);

            studyMetadata = metadataManager.getStudyMetadata(studyMetadata.getId());
            System.out.println(studyMetadata);

            Set<String> variants = checkArchiveTableLoadedVariants(studyMetadata, dbAdaptor, fileMetadata);
            filesMetadata.add(fileMetadata);
            expectedVariants.addAll(variants);
            Integer fileId = metadataManager.getFileId(studyMetadata.getId(), fileName);
            assertNotNull(fileId);
            assertTrue(metadataManager.getIndexedFiles(studyMetadata.getId()).contains(fileId));
//            printVariants(studyMetadata, dbAdaptor, newOutputUri());

//            checkLoadedVariants(expectedVariants, dbAdaptor, PLATINUM_SKIP_VARIANTS);
            checkArchiveTableTimeStamp(dbAdaptor);
            if (filesMetadata.size() >= maxFilesLoaded) {
                break;
            }
        }

        printVariants(studyMetadata, dbAdaptor, newOutputUri(1));

        for (Variant variant : dbAdaptor) {
            System.out.println("variant = " + variant);
        }

        System.out.println(studyMetadata);

        checkLoadedVariants(expectedVariants, dbAdaptor, PLATINUM_SKIP_VARIANTS);

        return studyMetadata;
    }

    public void checkLoadedVariants(Set<String> expectedVariants, VariantHadoopDBAdaptor dbAdaptor, HashSet<String> platinumSkipVariants)
            throws IOException {
        long count = dbAdaptor.count().first();
        expectedVariants.removeAll(platinumSkipVariants);
        System.out.println("count = " + count);
        System.out.println("expectedVariants = " + expectedVariants.size());
        if (expectedVariants.size() != count) {
            Set<String> loadedVariants = new HashSet<>();
            for (Variant variant : dbAdaptor) {
                loadedVariants.add(variant.toString());
                if (!expectedVariants.contains(variant.toString())) {
                    System.out.println("unexpectedVariant: " + variant);
                }
            }
            for (String expectedVariant : expectedVariants) {
                if (!loadedVariants.contains(expectedVariant)) {
                    System.out.println("Missing variant: " + expectedVariant);
                }
            }
            printVariantsFromVariantsTable(dbAdaptor);
        }
        assertEquals(expectedVariants.size(), count);
        count = 0;
        for (Variant variant : dbAdaptor) {
            count++;
            assertTrue(expectedVariants.contains(variant.toString()));
        }
        assertEquals(expectedVariants.size(), count);
    }

    public Set<String> checkArchiveTableLoadedVariants(StudyMetadata studyMetadata, VariantHadoopDBAdaptor dbAdaptor,
                                                       VariantFileMetadata fileMetadata) {
        int fileId = Integer.parseInt(fileMetadata.getId());
        Set<String> variants = getVariants(dbAdaptor, studyMetadata, fileId);
        int expected = (int) fileMetadata.getStats().getTypeCount().entrySet().stream()
                .filter(entry -> VARIANT_TYPES.contains(VariantType.valueOf(entry.getKey())))
                .mapToLong(Map.Entry::getValue)
                .sum();
        assertEquals(expected, variants.size());
        return variants;
    }


    protected Set<String> getVariants(VariantHadoopDBAdaptor dbAdaptor, StudyMetadata studyMetadata, int fileId) {
//        Map<String, Integer> variantCounts = new HashMap<>();
        Set<String> variants = new HashSet<>();
        Set<String> observed = new HashSet<>(Arrays.asList("M:516:-:CA", "1:10231:C:-", "1:10352:T:A", "M:515:G:A"));

        System.out.println("Query from Archive table");
        dbAdaptor.archiveIterator(studyMetadata.getName(), String.valueOf(fileId), new Query(), new QueryOptions())
                .forEachRemaining(variant -> {
                    if (VARIANT_TYPES.contains(variant.getType())) {
                        String string = variant.toString();
                        if (observed.contains(string)) {
                            System.out.println("Variant " + string + " found in file " + fileId);
                        }
                        variants.add(string);
                    }
//                    variantCounts.compute(variant.getType().toString(), (s, integer) -> integer == null ? 1 : (integer + 1));
                });
        return variants;
    }

    protected void checkArchiveTableTimeStamp(VariantHadoopDBAdaptor dbAdaptor) throws Exception {
    }

}