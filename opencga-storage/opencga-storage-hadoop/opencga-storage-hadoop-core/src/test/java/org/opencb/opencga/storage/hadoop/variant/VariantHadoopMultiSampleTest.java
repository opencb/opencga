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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.FileStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.VariantVcfDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsTaskTest;
import org.opencb.opencga.storage.hadoop.variant.index.VariantMergerTableMapper;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils.printVariants;
import static org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils.printVariantsFromVariantsTable;

/**
 * Created on 21/01/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopMultiSampleTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    public static final Set<VariantType> VARIANT_TYPES = VariantMergerTableMapper.getTargetVariantType();

    // Variants that are wrong in the platinum files that should not be included
    private static final HashSet<String> PLATINUM_SKIP_VARIANTS = new HashSet<>();
    private Map<String, Object> notCollapseDeletions = new ObjectMap(HadoopVariantStorageEngine.MERGE_COLLAPSE_DELETIONS, false).append(VariantStorageEngine.Options.ANNOTATE.key(), true);

    @Before
    public void setUp() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        clearDB(variantStorageManager.getVariantTableName());
        clearDB(variantStorageManager.getArchiveTableName(STUDY_ID));
        //Force HBaseConverter to fail if something goes wrong
        HBaseToVariantConverter.setFailOnWrongVariants(true);
    }

    @Override
    public Map<String, ?> getOtherStorageConfigurationOptions() {
        return new ObjectMap(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true).append(VariantStorageEngine.Options.ANNOTATE.key(), true);
    }

    public VariantFileMetadata loadFile(String resourceName, int fileId, StudyConfiguration studyConfiguration) throws Exception {
        return loadFile(resourceName, fileId, studyConfiguration, null);
    }

    public VariantFileMetadata loadFile(String resourceName, StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams) throws Exception {
        return loadFile(resourceName, -1, studyConfiguration, otherParams);
    }

    public VariantFileMetadata loadFile(String resourceName, int fileId, StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams) throws Exception {
        return loadFile(resourceName, fileId, studyConfiguration, otherParams, true, true, true);
    }

    public VariantFileMetadata loadFile(String resourceName, int fileId, StudyConfiguration studyConfiguration,
                                        Map<? extends String, ?> otherParams, boolean doTransform, boolean loadArchive, boolean loadVariant)
            throws Exception {
        return VariantHbaseTestUtils.loadFile(getVariantStorageEngine(), DB_NAME, outputUri, resourceName, fileId, studyConfiguration,
                otherParams, doTransform, loadArchive, loadVariant);
    }

    @Test
    public void testTwoFiles() throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        loadFile("s1.genome.vcf", studyConfiguration, notCollapseDeletions);
        checkArchiveTableTimeStamp(dbAdaptor);

        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        loadFile("s2.genome.vcf", studyConfiguration, notCollapseDeletions);

        checkArchiveTableTimeStamp(dbAdaptor);
        printVariants(studyConfiguration, dbAdaptor, newOutputUri());


        checkLoadedFilesS1S2(studyConfiguration, dbAdaptor);

    }

    @Test
    public void testTwoFiles_reverse() throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        loadFile("s2.genome.vcf", studyConfiguration, notCollapseDeletions);
        checkArchiveTableTimeStamp(dbAdaptor);

        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        loadFile("s1.genome.vcf", studyConfiguration, notCollapseDeletions);

        checkArchiveTableTimeStamp(dbAdaptor);
        printVariants(studyConfiguration, dbAdaptor, newOutputUri());


        checkLoadedFilesS1S2(studyConfiguration, dbAdaptor);

    }

    @Test
    public void testTwoFilesConcurrent() throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        ObjectMap options = variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
        options.put(HadoopVariantStorageEngine.HADOOP_LOAD_DIRECT, true);
        options.put(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "proto");
        options.put(VariantStorageEngine.Options.STUDY_ID.key(), studyConfiguration.getStudyId());
        options.put(VariantStorageEngine.Options.STUDY_NAME.key(), studyConfiguration.getStudyName());
        options.putAll(notCollapseDeletions);

        List<URI> inputFiles = Arrays.asList(getResourceUri("s1.genome.vcf"), getResourceUri("s2.genome.vcf"));
        List<StoragePipelineResult> index = variantStorageManager.index(inputFiles, outputUri, true, true, true);


        VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();

        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();

        for (StoragePipelineResult storagePipelineResult : index) {
            System.out.println(storagePipelineResult);
        }

        printVariants(studyConfiguration, dbAdaptor, newOutputUri());

        for (Variant variant : dbAdaptor) {
            System.out.println("variant = " + variant);
        }
        checkLoadedFilesS1S2(studyConfiguration, dbAdaptor);

        assertThat(studyConfiguration.getIndexedFiles(), hasItems(1, 2));
    }

    @Test
    public void testMultipleFilesProtoConcurrent() throws Exception {

        List<URI> protoFiles = new LinkedList<>();

        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        ObjectMap options = variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
        options.put(HadoopVariantStorageEngine.HADOOP_LOAD_ARCHIVE, false);
        options.put(HadoopVariantStorageEngine.HADOOP_LOAD_VARIANT, false);
        options.put(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "proto");
        options.put(VariantStorageEngine.Options.STUDY_ID.key(), STUDY_ID);
        options.put(VariantStorageEngine.Options.STUDY_NAME.key(), STUDY_NAME);
        options.put(VariantStorageEngine.Options.FILE_ID.key(), -1);

        List<URI> inputFiles = new LinkedList<>();

//        for (int fileId = 12877; fileId <= 12893; fileId++) {
        for (int fileId = 12877; fileId <= 12879; fileId++) {
            String fileName = "platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
//            inputFiles.add(getResourceUri(fileName));
            List<StoragePipelineResult> results = variantStorageManager.index(Collections.singletonList(getResourceUri(fileName)), outputUri, true, true, false);
            protoFiles.add(results.get(0).getTransformResult());

        }

       // dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);

        protoFiles = protoFiles.subList(0,2); // TODO remove

        options.put(HadoopVariantStorageEngine.HADOOP_LOAD_DIRECT, true);
        options.put(HadoopVariantStorageEngine.HADOOP_LOAD_ARCHIVE, true);
        options.put(HadoopVariantStorageEngine.HADOOP_LOAD_VARIANT, false);
        options.put(HadoopVariantStorageEngine.HADOOP_LOAD_VARIANT_PENDING_FILES, Arrays.asList(5,6,7));

        List<StoragePipelineResult> index2 = variantStorageManager.index(protoFiles, outputUri, false, false, true);

        System.out.println(index2);

    }

    @Test
    public void testMultipleFilesConcurrentSpecificPut() throws Exception {
        testMultipleFilesConcurrent(new ObjectMap(HadoopVariantStorageEngine.MERGE_LOAD_SPECIFIC_PUT, true));
    }

    @Test
    public void testMultipleFilesConcurrentFullPut() throws Exception {
        testMultipleFilesConcurrent(new ObjectMap(HadoopVariantStorageEngine.MERGE_LOAD_SPECIFIC_PUT, false));
    }

    @Test
    public void testMultipleFilesConcurrentMergeBasic() throws Exception {
        testMultipleFilesConcurrent(new ObjectMap(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                .append(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "avro")
                .append(HadoopVariantStorageEngine.HADOOP_LOAD_VARIANT_BATCH_SIZE, 5)
                .append(HadoopVariantStorageEngine.HADOOP_LOAD_ARCHIVE_BATCH_SIZE, 5));
    }

    public void testMultipleFilesConcurrent(ObjectMap extraParams) throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();

        List<URI> inputFiles = new LinkedList<>();

        for (int fileId = 12877; fileId <= 12893; fileId++) {
            String fileName = "platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
            inputFiles.add(getResourceUri(fileName));
        }

        ObjectMap options = variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
        options.put(HadoopVariantStorageEngine.HADOOP_LOAD_DIRECT, true);
        options.put(HadoopVariantStorageEngine.MERGE_COLLAPSE_DELETIONS, false);
        options.put(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "proto");
        options.put(VariantStorageEngine.Options.STUDY_ID.key(), studyConfiguration.getStudyId());
        options.put(VariantStorageEngine.Options.STUDY_NAME.key(), studyConfiguration.getStudyName());
        options.put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DP,AD");
        options.putAll(extraParams);
        List<StoragePipelineResult> index = variantStorageManager.index(inputFiles, outputUri, true, true, true);

        for (StoragePipelineResult storagePipelineResult : index) {
            System.out.println(storagePipelineResult);
        }

        URI outputUri = newOutputUri(1);
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        printVariants(studyConfiguration, dbAdaptor, outputUri);

//        checkLoadedVariants(expectedVariants, dbAdaptor, PLATINUM_SKIP_VARIANTS);


        for (Variant variant : dbAdaptor) {
            System.out.println("variant = " + variant);
        }

        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        System.out.println("StudyConfiguration = " + studyConfiguration);

        HadoopVariantFileMetadataDBAdaptor fileMetadataManager = dbAdaptor.getVariantFileMetadataDBAdaptor();
        Set<Integer> loadedFiles = fileMetadataManager.getLoadedFiles(studyConfiguration.getStudyId());
        System.out.println("loadedFiles = " + loadedFiles);
        for (int fileId = 1; fileId <= 17; fileId++) {
            assertThat(loadedFiles, hasItem(fileId));
        }
        for (Integer loadedFile : loadedFiles) {
            VariantFileMetadata variantFileMetadata = fileMetadataManager.getVariantFileMetadata(studyConfiguration.getStudyId(), loadedFile, null);
            assertNotNull(variantFileMetadata);
        }

        FileStudyConfigurationAdaptor.write(studyConfiguration, new File(outputUri.resolve("study_configuration.json").getPath()).toPath());
        try (FileOutputStream out = new FileOutputStream(outputUri.resolve("platinum.merged.vcf").getPath())) {
            VariantVcfDataWriter.htsExport(dbAdaptor.iterator(new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."), new QueryOptions(QueryOptions.SORT, true)),
                    studyConfiguration, dbAdaptor.getVariantFileMetadataDBAdaptor(), out, new Query(), new QueryOptions());
        }
    }

    @Test
    public void testTwoFilesFailOne() throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        ObjectMap otherParams = new ObjectMap(VariantMergerTableMapperFail.SLICE_TO_FAIL, "1_000000000011");
        otherParams.putAll(notCollapseDeletions);
        try {
            loadFile("s1.genome.vcf", studyConfiguration, otherParams);
            fail();
        } catch (StoragePipelineException e) {
            StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();
            studyConfiguration = scm.getStudyConfiguration(STUDY_ID, new QueryOptions()).first();
            System.out.println("studyConfiguration: " + studyConfiguration);
            System.out.println(studyConfiguration.getIndexedFiles());
            e.printStackTrace();
        }
        Integer fileId = studyConfiguration.getFileIds().get("s1.genome.vcf");
        System.out.println("fileId = " + fileId);
        otherParams.put(VariantMergerTableMapperFail.SLICE_TO_FAIL, "_");
        loadFile("s1.genome.vcf.variants.proto.gz", -1, studyConfiguration, otherParams, false, false, true);
        checkArchiveTableTimeStamp(dbAdaptor);
        loadFile("s2.genome.vcf", studyConfiguration, notCollapseDeletions);
        checkArchiveTableTimeStamp(dbAdaptor);

//        printVariants(studyConfiguration, dbAdaptor, newOutputUri());

        checkLoadedFilesS1S2(studyConfiguration, dbAdaptor);

        assertEquals(2, studyConfiguration.getBatches().size());

        BatchFileOperation batch = studyConfiguration.getBatches().get(0);
        assertEquals(BatchFileOperation.Status.READY, batch.currentStatus());
        assertThat(batch.getStatus().values(), hasItem(BatchFileOperation.Status.ERROR));

        batch = studyConfiguration.getBatches().get(1);
        assertEquals(BatchFileOperation.Status.READY, batch.currentStatus());
        assertThat(batch.getStatus().values(),
                not(hasItem(BatchFileOperation.Status.ERROR)));


    }

    public void checkLoadedFilesS1S2(StudyConfiguration studyConfiguration, VariantHadoopDBAdaptor dbAdaptor) throws IOException, StorageEngineException {

        Path path = Paths.get(getResourceUri("s1_s2.genome.vcf"));
        VariantFileMetadata fileMetadata = VariantReaderUtils.readVariantFileMetadata(path, null);

        VariantReader variantReader = new VariantVcfHtsjdkReader(new FileInputStream(path.toFile()), fileMetadata.toVariantStudyMetadata(STUDY_NAME));
        variantReader.open();
        variantReader.pre();
        Map<String, Variant> expectedVariants = new LinkedHashMap<>();
        new VariantNormalizer(true, true)
                .apply(variantReader.read(1000))
                .forEach(v -> expectedVariants.put(v.toString(), v));
        variantReader.post();
        variantReader.close();

        System.out.println("studyConfiguration = " + studyConfiguration);
        Map<String, Variant> variants = new HashMap<>();
        for (Variant variant : dbAdaptor) {
            String v = variant.toString();
            assertFalse(variants.containsKey(v));
            variants.put(v, variant);
//            VariantAnnotation a = variant.getAnnotation();
//            variant.setAnnotation(null);
//            System.out.println(variant.toJson());
//            variant.setAnnotation(a);
        }
        String studyName = studyConfiguration.getStudyName();

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

        List<String> errors = new ArrayList<>();

        String[] samples = {"s1", "s2"};
        String[] format = {VariantMerger.GT_KEY};
        // TODO: Add FT to the merged vcf!
//        String[] format = {VariantMerger.GT_KEY, VariantMerger.GENOTYPE_FILTER_KEY};
        for (String key : expectedVariants.keySet()) {
            if (variants.containsKey(key)) {
                StudyEntry studyEntry = variants.get(key).getStudy(studyName);
                StudyEntry expectedStudyEntry = expectedVariants.get(key).getStudies().get(0);
                for (String sample : samples) {
                    for (String formatKey : format) {
                        String expected = expectedStudyEntry.getSampleData(sample, formatKey);
                        if (expected.equals("./.")) {
                            expected = ".";
                        }
                        String actual = studyEntry.getSampleData(sample, formatKey);
                        if (actual.equals("./.")) {
                            actual = ".";
                        }
                        if (!expected.equals(actual)) {
                            errors.add("In variant " + key + " wrong " + formatKey + " for sample " + sample + ". Expected: " + expected + ", Actual: " + actual);
                        }
                    }
                }
                int numFiles = 0;
                if (!studyEntry.getSampleData("s1", "GT").equals("0/0")) {
                    numFiles++;
                }
                if (!studyEntry.getSampleData("s2", "GT").equals("0/0")) {
                    numFiles++;
                }
                assertEquals(numFiles, studyEntry.getFiles().size());
                assertEquals(expectedStudyEntry.getSecondaryAlternates().size(), studyEntry.getSecondaryAlternates().size());
                assertEquals(
                        expectedStudyEntry.getSecondaryAlternates().stream().map(AlternateCoordinate::getAlternate).collect(Collectors.toList()),
                        studyEntry.getSecondaryAlternates().stream().map(AlternateCoordinate::getAlternate).collect(Collectors.toList()));

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
    public void testPlatinumFilesOneByOne() throws Exception {
        testPlatinumFilesOneByOne(new ObjectMap(), 4);
    }

    @Test
    public void testPlatinumFilesOneByOne_extraFields() throws Exception {
        testPlatinumFilesOneByOne(new ObjectMap()
                .append(HadoopVariantStorageEngine.MERGE_ARCHIVE_SCAN_BATCH_SIZE, 2)
                .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DP,GQX,MQ"), 6);
    }

    @Test
    public void testPlatinumFilesOneByOne_extraFields_noCollapseDels() throws Exception {
        testPlatinumFilesOneByOne(new ObjectMap()
                .append(HadoopVariantStorageEngine.MERGE_COLLAPSE_DELETIONS, false)
                .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DP,GQX,MQ"), 4);
    }

    @Test
    public void testPlatinumFilesOneByOne_MergeBasic() throws Exception {
        StudyConfiguration studyConfiguration = testPlatinumFilesOneByOne(new ObjectMap()
                .append(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "avro")
                .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC)
                /*.append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), VariantMerger.GENOTYPE_FILTER_KEY + ",DP,GQX,MQ")*/, 4);


        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        List<Integer> sampleIds = new ArrayList<>(studyConfiguration.getSampleIds().values());

        FillGapsTaskTest.fillGaps(variantStorageEngine, studyConfiguration, sampleIds.subList(0, sampleIds.size()/2));
        printVariants(studyConfiguration, dbAdaptor, newOutputUri());

        FillGapsTaskTest.fillGaps(variantStorageEngine, studyConfiguration, sampleIds.subList(sampleIds.size()/2, sampleIds.size()));
        printVariants(studyConfiguration, dbAdaptor, newOutputUri());

        FillGapsTaskTest.fillGaps(variantStorageEngine, studyConfiguration, sampleIds);
        printVariants(studyConfiguration, dbAdaptor, newOutputUri());


    }

    public StudyConfiguration testPlatinumFilesOneByOne(ObjectMap otherParams, int maxFilesLoaded) throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        List<VariantFileMetadata> filesMetadata = new LinkedList<>();
        Set<String> expectedVariants = new HashSet<>();

        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();


        for (int fileId = 12877; fileId <= 12893; fileId++) {
            ObjectMap params = new ObjectMap();
            params.putAll(otherParams);
            String resourceName = "platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
            VariantFileMetadata fileMetadata = loadFile(resourceName, fileId, studyConfiguration, params);

            studyConfiguration = scm.getStudyConfiguration(studyConfiguration.getStudyId(), new QueryOptions()).first();
            System.out.println(studyConfiguration);

            Set<String> variants = checkArchiveTableLoadedVariants(studyConfiguration, dbAdaptor, fileMetadata);
            filesMetadata.add(fileMetadata);
            expectedVariants.addAll(variants);
            assertTrue(studyConfiguration.getIndexedFiles().contains(fileId));
//            printVariants(studyConfiguration, dbAdaptor, newOutputUri());

//            checkLoadedVariants(expectedVariants, dbAdaptor, PLATINUM_SKIP_VARIANTS);
            checkArchiveTableTimeStamp(dbAdaptor);
            if (filesMetadata.size() >= maxFilesLoaded) {
                break;
            }
        }

        printVariants(studyConfiguration, dbAdaptor, newOutputUri(1));

        for (Variant variant : dbAdaptor) {
            System.out.println("variant = " + variant);
        }

        System.out.println(studyConfiguration);

        checkLoadedVariants(expectedVariants, dbAdaptor, PLATINUM_SKIP_VARIANTS);

        return studyConfiguration;
    }

    @Test
    public void testPlatinumFilesBatchLoad() throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        List<VariantFileMetadata> filesMetadata = new LinkedList<>();
        Set<String> expectedVariants = new HashSet<>();
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        List<Integer> fileIds = IntStream.range(12877, 12894).boxed().collect(Collectors.toList());

        for (Integer fileId : fileIds.subList(0, fileIds.size() - 1)) {
            VariantFileMetadata fileMetadata = loadFile("platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz", fileId, studyConfiguration,
                    new ObjectMap(HadoopVariantStorageEngine.HADOOP_LOAD_VARIANT, false));
            filesMetadata.add(fileMetadata);
            expectedVariants.addAll(checkArchiveTableLoadedVariants(studyConfiguration, dbAdaptor, fileMetadata));
            assertFalse(studyConfiguration.getIndexedFiles().contains(fileId));
        }
        Integer fileId = fileIds.get(fileIds.size() - 1);
        VariantFileMetadata fileMetadata = loadFile("platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz", fileId, studyConfiguration,
                new ObjectMap(HadoopVariantStorageEngine.HADOOP_LOAD_VARIANT, true)
                        .append(HadoopVariantStorageEngine.MERGE_COLLAPSE_DELETIONS, false)
                        .append(HadoopVariantStorageEngine.HADOOP_LOAD_VARIANT_PENDING_FILES, StringUtils.join(fileIds, ","))
        );
        filesMetadata.add(fileMetadata);
        expectedVariants.addAll(checkArchiveTableLoadedVariants(studyConfiguration, dbAdaptor, fileMetadata));

        StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();
        studyConfiguration = scm.getStudyConfiguration(studyConfiguration.getStudyId(), new QueryOptions()).first();

        System.out.println("studyConfiguration = " + studyConfiguration.getAttributes().toJson());
        System.out.println("HBaseStudyConfiguration = " + studyConfiguration);

        for (fileId = 12877; fileId <= 12893; fileId++) {
            assertTrue(studyConfiguration.getIndexedFiles().contains(fileId));
        }

        printVariants(studyConfiguration, dbAdaptor, newOutputUri());

        for (Variant variant : dbAdaptor) {
            System.out.println(variant);
        }

        checkArchiveTableTimeStamp(dbAdaptor);
        checkLoadedVariants(expectedVariants, dbAdaptor, PLATINUM_SKIP_VARIANTS);

    }

    public void checkLoadedVariants(Set<String> expectedVariants, VariantHadoopDBAdaptor dbAdaptor, HashSet<String> platinumSkipVariants)
            throws IOException {
        long count = dbAdaptor.count(null).first();
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

    public Set<String> checkArchiveTableLoadedVariants(StudyConfiguration studyConfiguration, VariantHadoopDBAdaptor dbAdaptor,
                                                       VariantFileMetadata source) {
        int fileId = Integer.valueOf(source.getId());
        Set<String> variants = getVariants(dbAdaptor, studyConfiguration, fileId);
        int expected = source.getStats().getVariantTypeCounts().entrySet().stream()
                .filter(entry -> VARIANT_TYPES.contains(VariantType.valueOf(entry.getKey())))
                .map(Map.Entry::getValue)
                .reduce(Integer::sum)
                .orElse(0);
        assertEquals(expected, variants.size());
        return variants;
    }


    protected Set<String> getVariants(VariantHadoopDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration, int fileId){
//        Map<String, Integer> variantCounts = new HashMap<>();
        Set<String> variants = new HashSet<>();
        Set<String> observed = new HashSet<>(Arrays.asList("M:516:-:CA", "1:10231:C:-", "1:10352:T:A", "M:515:G:A"));

        System.out.println("Query from Archive table");
        dbAdaptor.iterator(
                new Query()
                        .append(VariantQueryParam.STUDY.key(), studyConfiguration.getStudyId())
                        .append(VariantQueryParam.FILE.key(), fileId),
                new QueryOptions("archive", true))
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
        StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();
        StudyConfiguration studyConfiguration = scm.getStudyConfiguration(STUDY_ID, new QueryOptions()).first();

        String tableName = HadoopVariantStorageEngine.getArchiveTableName(STUDY_ID, dbAdaptor.getConfiguration());
        System.out.println("Query from archive HBase " + tableName);
        HBaseManager hm = new HBaseManager(configuration.get());

        GenomeHelper helper = dbAdaptor.getGenomeHelper();

        if (studyConfiguration.getBatches().isEmpty()) {
            return;
        }
        long ts = studyConfiguration.getBatches().get(studyConfiguration.getBatches().size() - 1).getTimestamp();

        hm.act(tableName, table -> {
            Scan scan = new Scan();
            scan.setFilter(new PrefixFilter(GenomeHelper.VARIANT_COLUMN_B_PREFIX));
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                List<Cell> cells = GenomeHelper.getVariantColumns(result.rawCells());
                assertNotEquals(0, cells.size());
                for (Cell cell : cells) {
                    VariantTableStudyRowsProto proto = VariantTableStudyRowsProto.parseFrom(CellUtil.cloneValue(cell));
                    assertEquals(ts, proto.getTimestamp());
                }
            }
            resultScanner.close();
            return null;
        });


    }

}