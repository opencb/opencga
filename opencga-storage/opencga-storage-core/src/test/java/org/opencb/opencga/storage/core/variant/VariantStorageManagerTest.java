package org.opencb.opencga.storage.core.variant;

import org.junit.*;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

/**
 * Created by hpccoll1 on 13/03/15.
 */
@Ignore
public abstract class VariantStorageManagerTest {

    public static final String VCF_TEST_FILE_NAME = "10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz";
    public static final int NUM_VARIANTS = 9792;
    public static final int STUDY_ID = 5;
    public static final String STUDY_NAME = "1000g";
    protected static URI inputUri;
    protected static URI outputUri;
    protected VariantStorageManager variantStorageManager;
    public static Logger logger;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug");
        Path rootDir = Paths.get("tmp", "VariantStorageManagerTest");
        Files.createDirectories(rootDir);
        Path inputPath = rootDir.resolve(VCF_TEST_FILE_NAME);
        Files.copy(VariantStorageManagerTest.class.getClassLoader().getResourceAsStream(VCF_TEST_FILE_NAME), inputPath, StandardCopyOption.REPLACE_EXISTING);
        inputUri = inputPath.toUri();
        outputUri = rootDir.toUri();
        logger = LoggerFactory.getLogger(VariantStorageManagerTest.class);
    }

    @Before
    public void before() throws Exception {
        clearDB();
        variantStorageManager = getVariantStorageManager();
    }

    protected abstract VariantStorageManager getVariantStorageManager() throws Exception;
    protected abstract void clearDB() throws Exception;

    @Test
    public void basicIndex() throws Exception {
        clearDB();
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        ETLResult etlResult = runDefaultETL(variantStorageManager, studyConfiguration);
        Assert.assertTrue("Incorrect transform file extension " + etlResult.transformResult + ". Expected 'variants.json.snappy'" ,
                Paths.get(etlResult.transformResult).toFile().getName().endsWith("variants.json.snappy"));

        checkTransformedVariants(etlResult.transformResult, studyConfiguration);
        checkLoadedVariants(variantStorageManager.getDBAdaptor(null, null), studyConfiguration, true, false);
    }

    /**
     * Single Thread indexation. "Old Style" indexation
     *  With samples and "src"
     *  Gzip compression
     **/
    @Test
    public void singleThreadIndex() throws Exception {
        clearDB();
        ObjectMap params = new ObjectMap();
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        params.put(VariantStorageManager.STUDY_CONFIGURATION, studyConfiguration);
        params.put(VariantStorageManager.FILE_ID, 6);
        params.put(VariantStorageManager.COMPRESS_METHOD, "gZiP");
        params.put(VariantStorageManager.TRANSFORM_THREADS, 1);
        params.put(VariantStorageManager.LOAD_THREADS, 1);
        params.put(VariantStorageManager.INCLUDE_SAMPLES, true);
        params.put(VariantStorageManager.INCLUDE_SRC, true);
        ETLResult etlResult = runETL(variantStorageManager, params, true, true, true);

        Assert.assertTrue("Incorrect transform file extension " + etlResult.transformResult + ". Expected 'variants.json.gz'" ,
                Paths.get(etlResult.transformResult).toFile().getName().endsWith("variants.json.gz"));

        checkTransformedVariants(etlResult.transformResult, studyConfiguration);
        checkLoadedVariants(variantStorageManager.getDBAdaptor(null, null), studyConfiguration, true, false);

    }

    /**
     * Fast indexation.
     *  Without "src" and samples information.
     *  MultiThreads
     *  CompressMethod snappy
     *
     **/
    @Test
    public void fastIndex() throws Exception {
        clearDB();
        ObjectMap params = new ObjectMap();
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        params.put(VariantStorageManager.STUDY_CONFIGURATION, studyConfiguration);
        params.put(VariantStorageManager.FILE_ID, 6);
        params.put(VariantStorageManager.COMPRESS_METHOD, "snappy");
        params.put(VariantStorageManager.TRANSFORM_THREADS, 8);
        params.put(VariantStorageManager.LOAD_THREADS, 8);
        params.put(VariantStorageManager.INCLUDE_SAMPLES, false);
        params.put(VariantStorageManager.INCLUDE_SRC, false);
        ETLResult etlResult = runETL(variantStorageManager, params, true, true, true);

        Assert.assertTrue("Incorrect transform file extension " + etlResult.transformResult + ". Expected 'variants.json.snappy'" ,
                Paths.get(etlResult.transformResult).toFile().getName().endsWith("variants.json.snappy"));

        checkTransformedVariants(etlResult.transformResult, studyConfiguration);
        checkLoadedVariants(variantStorageManager.getDBAdaptor(null, null), studyConfiguration, false, false);

    }

    /* ---------------------------------------------------- */
    /* Check methods for loaded and transformed Variants    */
    /* ---------------------------------------------------- */


    private void checkTransformedVariants(URI variantsJson, StudyConfiguration studyConfiguration) {
        long start = System.currentTimeMillis();
        VariantJsonReader variantJsonReader = new VariantJsonReader(new VariantSource(VCF_TEST_FILE_NAME, "6", "", ""),
                variantsJson.getPath(),
                variantsJson.getPath().replace("variants", "file"));

        variantJsonReader.open();
        variantJsonReader.pre();

        List<Variant> read;
        int numVariants = 0;
        while ((read = variantJsonReader.read(100)) != null && !read.isEmpty()) {
            numVariants += read.size();
        }

        variantJsonReader.post();
        variantJsonReader.close();

        Assert.assertEquals(NUM_VARIANTS, numVariants); //9792
        logger.info("checkTransformedVariants time : " + (System.currentTimeMillis() - start) / 1000.0 + "s");
    }

    private void checkLoadedVariants(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration, boolean includeSamples, boolean includeSrc) {
        long start = System.currentTimeMillis();
        int numVariants = 0;
        String expectedStudyId = Integer.toString(studyConfiguration.getStudyId());
        QueryResult allVariants = dbAdaptor.getAllVariants(new QueryOptions("limit", 1));
        Assert.assertEquals(1, allVariants.getNumResults());
        Assert.assertEquals(NUM_VARIANTS, allVariants.getNumTotalResults());
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, VariantSourceEntry> entry : variant.getSourceEntries().entrySet()) {
                Assert.assertEquals(expectedStudyId, entry.getValue().getStudyId());
                if (includeSamples) {
                    Assert.assertNotNull(entry.getValue().getSamplesData());
                    Assert.assertNotEquals(0, entry.getValue().getSamplesData().size());

                    Assert.assertEquals(studyConfiguration.getSampleIds().size(), entry.getValue().getSamplesData().size());
                    Assert.assertEquals(studyConfiguration.getSampleIds().keySet(), entry.getValue().getSamplesData().keySet());
                }
                if (includeSrc) {
                    Assert.assertNotNull(entry.getValue().getAttribute("src"));
                }
            }
            numVariants++;
        }
        Assert.assertEquals(NUM_VARIANTS, numVariants); //9792
        logger.info("checkLoadedVariants time : " + (System.currentTimeMillis() - start)/1000.0 + "s");
    }


    /* ---------------------------------------------------- */
    /* Static methods to run a simple ETL to index Variants */
    /* ---------------------------------------------------- */

    /**
     * Simple class to store the output URIs generated by the ETL
     */
    public static class ETLResult {

        public URI extractResult;
        public URI preTransformResult;
        public URI transformResult;
        public URI postTransformResult;
        public URI preLoadResult;
        public URI loadResult;
        //        public URI postLoadResult;
    }

    protected static ETLResult runETL(VariantStorageManager variantStorageManager, ObjectMap params)
            throws IOException, FileFormatException, StorageManagerException {
        return runETL(variantStorageManager, params, true, true, true);
    }

    protected static ETLResult runETL(VariantStorageManager variantStorageManager, ObjectMap params,
                                    boolean doExtract,
                                    boolean doTransform,
                                    boolean doLoad)
            throws IOException, FileFormatException, StorageManagerException {
        return runETL(variantStorageManager, inputUri, outputUri, params, params, params, params, params, params, params, doExtract, doTransform, doLoad);
    }

    public static ETLResult runDefaultETL(VariantStorageManager variantStorageManager, StudyConfiguration studyConfiguration)
            throws URISyntaxException, IOException, FileFormatException, StorageManagerException {

        ObjectMap extractParams = new ObjectMap();

        ObjectMap preTransformParams = new ObjectMap();
        preTransformParams.put(VariantStorageManager.STUDY_CONFIGURATION, studyConfiguration);
        ObjectMap transformParams = new ObjectMap();
        transformParams.put(VariantStorageManager.STUDY_CONFIGURATION, studyConfiguration);
        transformParams.put(VariantStorageManager.INCLUDE_SAMPLES, true);
        transformParams.put(VariantStorageManager.FILE_ID, 6);
        ObjectMap postTransformParams = new ObjectMap();

        ObjectMap preLoadParams = new ObjectMap();
        preLoadParams.put(VariantStorageManager.STUDY_CONFIGURATION, studyConfiguration);
        ObjectMap loadParams = new ObjectMap();
        loadParams.put(VariantStorageManager.STUDY_CONFIGURATION, studyConfiguration);
        loadParams.put(VariantStorageManager.INCLUDE_SAMPLES, true);
        loadParams.put(VariantStorageManager.FILE_ID, 6);
        ObjectMap postLoadParams = new ObjectMap();

        return runETL(variantStorageManager, inputUri, outputUri, extractParams, preTransformParams, transformParams, postTransformParams, preLoadParams, loadParams, postLoadParams, true, true, true);
    }

    protected static ETLResult runETL(VariantStorageManager variantStorageManager, URI inputUri, URI outputUri,
                                      ObjectMap extractParams,
                                      ObjectMap preTransformParams, ObjectMap transformParams, ObjectMap postTransformParams,
                                      ObjectMap preLoadParams, ObjectMap loadParams, ObjectMap postLoadParams,
                                      boolean doExtract,
                                      boolean doTransform,
                                      boolean doLoad)
            throws IOException, FileFormatException, StorageManagerException {
        ETLResult etlResult = new ETLResult();

        if (doExtract) {
            inputUri = variantStorageManager.extract(inputUri, outputUri, extractParams);
            etlResult.extractResult = inputUri;
        }

        if (doTransform) {
            inputUri = variantStorageManager.preTransform(inputUri, preTransformParams);
            etlResult.preTransformResult = inputUri;
            Assert.assertTrue("Intermediary file " + inputUri + " does not exist", Paths.get(inputUri).toFile().exists());

            inputUri = variantStorageManager.transform(inputUri, null, outputUri, transformParams);
            etlResult.transformResult = inputUri;
            Assert.assertTrue("Intermediary file " + inputUri + " does not exist", Paths.get(inputUri).toFile().exists());

            inputUri = variantStorageManager.postTransform(inputUri, postTransformParams);
            etlResult.postTransformResult = inputUri;
            Assert.assertTrue("Intermediary file " + inputUri + " does not exist", Paths.get(inputUri).toFile().exists());
        }

        if (doLoad) {
            inputUri = variantStorageManager.preLoad(inputUri, outputUri, preLoadParams);
            etlResult.preLoadResult = inputUri;
            Assert.assertTrue("Intermediary file " + inputUri + " does not exist", Paths.get(inputUri).toFile().exists());

            inputUri = variantStorageManager.load(inputUri, loadParams);
            etlResult.loadResult = inputUri;
            Assert.assertTrue("Intermediary file " + inputUri + " does not exist", Paths.get(inputUri).toFile().exists());

            variantStorageManager.postLoad(inputUri, outputUri, postLoadParams);
        }
        return etlResult;
    }

    protected static StudyConfiguration newStudyConfiguration() {
        return new StudyConfiguration(STUDY_ID, STUDY_NAME);
    }

}
