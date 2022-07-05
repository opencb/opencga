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

package org.opencb.opencga.storage.core.variant;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 31/05/15.
 */
@Ignore
public abstract class VariantStorageBaseTest extends GenericTest implements VariantStorageTest {

    public static final int NUM_VARIANTS = 9792;
    @Deprecated public static final int STUDY_ID = 1;
    public static final String STUDY_NAME = "1000g";
    public static final String STUDY_NAME_1 = STUDY_NAME;
    public static final String STUDY_NAME_2 = "study_2";
    public static final String DB_NAME = "opencga_variants_test";
    @Deprecated public static final int FILE_ID = 1;
    public static final Set<String> VARIANTS_WITH_CONFLICTS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "22:16050655-16063474:-:<CN0>",
            "22:16050655-16063474:-:<CN2>",
            "22:16050655-16063474:-:<CN3>",
            "22:16050655-16063474:-:<CN4>",
            "22:16050655:G:A", // Overlaps with a CNV
            "22:16080425:TA:-",
            "22:16080425:T:C",
            "22:16080426:A:G",
            "22:16136748:ATTA:-",
            "22:16136749:T:A",
            "22:16137302:G:C",
            "22:16137301:AG:-",
//            "22:16206615:C:A",
//            "22:16206615:-:C",
//            "22:16285168:-:CAAAC", // <-- This won't be a conflict with the new var end.
//            "22:16285169:T:G",
            "22:16464051:T:C",
            "22:16482314:C:T",
//            "22:16482314:C:-",
            "22:16532311:A:C",
            "22:16532321:A:T",
            "22:16538352:A:C",
            "22:16555584:CT:-",
            "22:16555584:C:T",
            "22:16556120:AGTGTTCTGGAATCCTATGTGAGGGACAAACACTCACACCCTCAGAGG:-",
            "22:16556162:C:T",
            "22:16592392:G:C",
            "22:16614404:G:A",
            "22:16616085:-:T",
            "22:16637481:A:T",
            "22:16616084:G:A"
    )));

    public static final String VCF_TEST_FILE_NAME = "10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz";
    protected static URI inputUri;

    public static final String SMALL_VCF_TEST_FILE_NAME = "variant-test-file.vcf.gz";
    protected static URI smallInputUri;

    public static final String VCF_CORRUPTED_FILE_NAME = "variant-test-file-corrupted.vcf";
    protected static URI corruptedInputUri;

    protected static URI outputUri;
    protected VariantStorageEngine variantStorageEngine;
    protected VariantStorageMetadataManager metadataManager;
    private static Logger logger = LoggerFactory.getLogger(VariantStorageBaseTest.class);
    private static Path rootDir = null;
    private static boolean cleanFirst = false;
    public static IOConnectorProvider ioConnectorProvider;
    public static  VariantReaderUtils variantReaderUtils;
//    private static AtomicInteger count = new AtomicInteger(0);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void _beforeClass() throws Exception {
//        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug");
        newRootDir();
        if (rootDir.toFile().exists()) {
            IOUtils.deleteDirectory(rootDir);
            Files.createDirectories(rootDir);
        }
        Path inputPath = rootDir.resolve(VCF_TEST_FILE_NAME);
        Path smallInputPath = rootDir.resolve(SMALL_VCF_TEST_FILE_NAME);
        Path corruptedInputPath = rootDir.resolve(VCF_CORRUPTED_FILE_NAME);
        Files.copy(VariantStorageEngineTest.class.getClassLoader().getResourceAsStream(VCF_TEST_FILE_NAME), inputPath,
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(VariantStorageEngineTest.class.getClassLoader().getResourceAsStream(SMALL_VCF_TEST_FILE_NAME), smallInputPath,
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(VariantStorageEngineTest.class.getClassLoader().getResourceAsStream(VCF_CORRUPTED_FILE_NAME), corruptedInputPath,
                StandardCopyOption.REPLACE_EXISTING);

        inputUri = inputPath.toUri();
        smallInputUri = smallInputPath.toUri();
        corruptedInputUri = corruptedInputPath.toUri();
        outputUri = rootDir.toUri();
//        logger.info("count: " + count.getAndIncrement());

    }

    public static URI getResourceUri(String resourceName) throws IOException {
        return getResourceUri(resourceName, resourceName);
    }

    public static URI getResourceUri(String resourceName, String targetName) throws IOException {
        Path rootDir = getTmpRootDir();
        Path resourcePath = rootDir.resolve(targetName);
        if (!resourcePath.getParent().toFile().exists()) {
            Files.createDirectories(resourcePath.getParent());
        }
        if (!resourcePath.toFile().exists()) {
            InputStream stream;
            if ("managed-schema".equals(resourceName)) {
                stream = VariantSearchManager.class.getClassLoader().getResourceAsStream(resourceName);
            } else {
                stream = VariantStorageEngineTest.class.getClassLoader().getResourceAsStream(resourceName);
            }

            Assert.assertNotNull(resourceName, stream);
            Files.copy(stream, resourcePath, StandardCopyOption.REPLACE_EXISTING);
        }
        return resourcePath.toUri();
    }

    public static Path getTmpRootDir() throws IOException {
//        Path rootDir = Paths.get("/tmp", "VariantStorageManagerTest");

        if (rootDir == null) {
            newRootDir();
        }
        return rootDir;
    }

    public static URI getPlatinumFile(int fileId) throws IOException {
        String fileName;
        if (fileId < 17) {
            fileName = "1K.end.platinum-genomes-vcf-NA" + (fileId + 12877) + "_S1.genome.vcf.gz";
        } else if (fileId >= 12877 && fileId <= 12893){
            fileName = "1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
        } else {
            throw new IllegalArgumentException("Unknown platinum file " + fileId);
        }
        return getResourceUri("platinum/" + fileName);
    }


    private static void newRootDir() throws IOException {
        rootDir = Paths.get("target/test-data", "junit-opencga-storage-" + TimeUtils.getTimeMillis() + "_" + RandomStringUtils.randomAlphabetic(3));
        Files.createDirectories(rootDir);
    }

    public static void setRootDir(Path rootDir) {
        VariantStorageBaseTest.rootDir = rootDir;
    }

    public static URI newOutputUri() throws IOException {
        return newOutputUri(1, outputUri);
    }

    public static URI newOutputUri(int extraCalls) throws IOException {
        return newOutputUri(1 + extraCalls, outputUri);
    }

    public static URI newOutputUri(int extraCalls, URI outputUri) throws IOException {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        // stackTrace[0] = "Thread.currentThread"
        // stackTrace[1] = "newOutputUri"
        // stackTrace[2] =  caller method
        String testName = stackTrace[2 + extraCalls].getMethodName();
        return newOutputUri(testName, outputUri);
    }

    protected static URI newOutputUri(String testName) throws IOException {
        return newOutputUri(testName, outputUri);
    }

    protected static URI newOutputUri(String testName, URI outputUri) throws IOException {
        int c = 0;
        URI finalOutputUri = outputUri.resolve("test_" + testName + "/");
        while (Paths.get(finalOutputUri).toFile().exists()) {
            finalOutputUri = outputUri.resolve("test_" + testName + "-" + ++c + "/");
        }
        Files.createDirectory(Paths.get(finalOutputUri));
        return finalOutputUri;
    }

//    private static File tempFile = null;
//    protected static Path getTmpRootDir() throws IOException {
//        if (tempFile == null) {
//            tempFile = File.createTempFile("opencga-variants-storage-test-", null);
//        }
//        return tempFile.toPath();
//    }

//    @ClassRule
//    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
//
//    protected static Path getTmpRootDir() throws IOException {
//        return temporaryFolder.getRoot().toPath();
//    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        cleanFirst = true;
    }

    @Before
    public void before() throws Exception {
        clearDB(DB_NAME);
    }

    @Before
    public final void _before() throws Exception {
        if (cleanFirst) {
            clearDB(DB_NAME);
            cleanFirst = false;
        }
        printActiveThreadsNumber();
        variantStorageEngine = getVariantStorageEngine();
        variantStorageEngine.getOptions().put(VariantStorageOptions.QUERY_LIMIT_DEFAULT.key(), 10000);
        variantStorageEngine.getOptions().put(VariantStorageOptions.QUERY_LIMIT_MAX.key(), 10000);
        variantStorageEngine.getOptions().put(VariantStorageOptions.QUERY_SAMPLE_LIMIT_DEFAULT.key(), 10000);
        variantStorageEngine.getOptions().put(VariantStorageOptions.QUERY_SAMPLE_LIMIT_MAX.key(), 10000);
        variantStorageEngine.getOptions().put(VariantStorageOptions.ANNOTATION_THREADS.key(), 2);
        metadataManager = variantStorageEngine.getMetadataManager();
        variantReaderUtils = variantStorageEngine.getVariantReaderUtils();
        ioConnectorProvider = variantStorageEngine.getIOManagerProvider();
    }

    @After
    public final void _after() throws Exception {
        close();
    }


    /* ---------------------------------------------------- */
    /* Static methods to run a simple ETL to index Variants */
    /* ---------------------------------------------------- */


    public static StoragePipelineResult runETL(VariantStorageEngine variantStorageManager, ObjectMap options)
            throws IOException, FileFormatException, StorageEngineException {
        return runETL(variantStorageManager, options, true, true, true);
    }

    public static StoragePipelineResult runETL(VariantStorageEngine variantStorageManager, URI input, String study, ObjectMap options)
            throws IOException, FileFormatException, StorageEngineException {
        return runETL(variantStorageManager, input, outputUri, options.append(VariantStorageOptions.STUDY.key(), study), true, true, true);
    }

    public static StoragePipelineResult runETL(VariantStorageEngine variantStorageManager, ObjectMap options,
                                               boolean doExtract,
                                               boolean doTransform,
                                               boolean doLoad)
            throws IOException, FileFormatException, StorageEngineException {
        return runETL(variantStorageManager, inputUri, outputUri, options, doExtract, doTransform, doLoad);
    }

    public static StoragePipelineResult runDefaultETL(VariantStorageEngine variantStorageManager, StudyMetadata studyMetadata)
            throws URISyntaxException, IOException, FileFormatException, StorageEngineException {
        return runDefaultETL(inputUri, variantStorageManager, studyMetadata);
    }

    public static StoragePipelineResult runDefaultETL(URI inputUri, VariantStorageEngine variantStorageManager, StudyMetadata studyMetadata)
            throws URISyntaxException, IOException, FileFormatException, StorageEngineException {
        return runDefaultETL(inputUri, variantStorageManager, studyMetadata, new ObjectMap());
    }

    public static StoragePipelineResult runDefaultETL(URI inputUri, VariantStorageEngine variantStorageManager,
                                                      StudyMetadata studyMetadata, ObjectMap params)
            throws URISyntaxException, IOException, FileFormatException, StorageEngineException {
        return runDefaultETL(inputUri, variantStorageManager, studyMetadata, params, true, true);
    }

    public static StoragePipelineResult runDefaultETL(URI inputUri, VariantStorageEngine variantStorageManager,
                                                      StudyMetadata studyMetadata, ObjectMap params, boolean doTransform, boolean doLoad)
            throws URISyntaxException, IOException, FileFormatException, StorageEngineException {

        ObjectMap newParams = new ObjectMap(params);

        if (studyMetadata == null) {
            newParams.putIfAbsent(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        } else {
            newParams.putIfAbsent(VariantStorageOptions.STATS_AGGREGATION.key(), studyMetadata.getAggregation());
            newParams.putIfAbsent(VariantStorageOptions.STUDY.key(), studyMetadata.getName());
        }
//        newParams.putIfAbsent(VariantStorageEngine.Options.FILE_ID.key(), FILE_ID);
        // Default value is already avro
//        newParams.putIfAbsent(VariantStorageEngine.Options.TRANSFORM_FORMAT.key(), "avro");
        newParams.putIfAbsent(VariantStorageOptions.ANNOTATE.key(), true);
        newParams.putIfAbsent(VariantStorageOptions.SPECIES.key(), "hsapiens");
        newParams.putIfAbsent(VariantStorageOptions.ASSEMBLY.key(), "GRch37");
        newParams.putIfAbsent(VariantStorageOptions.STATS_CALCULATE.key(), true);

        StoragePipelineResult storagePipelineResult = runETL(variantStorageManager, inputUri, outputUri, newParams, true, doTransform, doLoad);

        if (studyMetadata != null) {
            int studyId = variantStorageManager.getMetadataManager().getStudyId(studyMetadata.getName());
            studyMetadata.setId(studyId);
        }
//        try (VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor()) {
//            StudyMetadata newStudyMetadata = dbAdaptor.getMetadataManager().getStudyMetadata(studyMetadata.getStudyName(), null).first();
//            if (newStudyMetadata != null) {
//                studyMetadata.copy(newStudyMetadata);
//            }
//        }

        return storagePipelineResult;
    }

    public static StoragePipelineResult runETL(VariantStorageEngine variantStorageManager, URI inputUri, URI outputUri,
                                               ObjectMap params,
                                               boolean doExtract,
                                               boolean doTransform,
                                               boolean doLoad)
            throws IOException, FileFormatException, StorageEngineException {


        variantStorageManager.getOptions().putAll(params);
        StoragePipelineResult storagePipelineResult =
                variantStorageManager.index(Collections.singletonList(inputUri), outputUri, doExtract, doTransform, doLoad).get(0);

        checkFileExists(storagePipelineResult.getExtractResult());
        checkFileExists(storagePipelineResult.getPreTransformResult());
        checkFileExists(storagePipelineResult.getTransformResult());
        checkFileExists(storagePipelineResult.getPostTransformResult());
        checkFileExists(storagePipelineResult.getPreLoadResult());
        checkFileExists(storagePipelineResult.getLoadResult());
        checkFileExists(storagePipelineResult.getPostLoadResult());

        return storagePipelineResult;
    }

    public static void checkFileExists(URI uri) {
        if (uri != null && ( uri.getScheme() == null || Objects.equals(uri.getScheme(), "file") )) {
            Assert.assertTrue("Intermediary file " + uri + " does not exist", Paths.get(uri).toFile().exists());
        }
    }

    protected static StudyMetadata newStudyMetadata() {
        return new StudyMetadata(STUDY_ID, STUDY_NAME);
    }

    public boolean assertWithConflicts(Variant variant, Runnable assertCondition) {
        try {
            assertCondition.run();
        } catch (AssertionError e) {
            if (VARIANTS_WITH_CONFLICTS.contains(variant.toString())) {
                logger.error(e.getMessage());
                return false;
            } else {
                throw e;
            }
        }
        return true;
    }

    public void printActiveThreadsNumber() {
        List<String> threads = Thread.getAllStackTraces()
                .keySet()
                .stream()
                .filter(t -> t.getThreadGroup() == null || !t.getThreadGroup().getName().equals("system"))
                .filter(t -> t.getState() != Thread.State.TERMINATED)
                .map(Thread::toString).collect(Collectors.toList());
        System.out.println("ActiveThreads: " + threads.size());
//        threads.forEach(s -> System.out.println("\t" + s));
    }

    public void printActiveThreads() {
        System.out.println("=========================================");
        System.out.println("Thread.activeCount() = " + Thread.activeCount());

        Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
        Set<String> groups = allStackTraces.keySet()
                .stream()
                .filter(t -> t.getThreadGroup() == null || !t.getThreadGroup().getName().equals("system"))
                .map(t -> String.valueOf(t.getThreadGroup()))
                .collect(Collectors.toSet());

        for (String group : groups) {
            System.out.println("group = " + group);
            for (Map.Entry<Thread, StackTraceElement[]> entry : allStackTraces.entrySet()) {
                Thread thread = entry.getKey();
                if (String.valueOf(thread.getThreadGroup()).equals(group)) {
                    System.out.println("\t[" + thread.getId() + "] " + thread.toString() + ":" + thread.getState());
                }
            }
        }
        System.out.println("=========================================");
    }
}
