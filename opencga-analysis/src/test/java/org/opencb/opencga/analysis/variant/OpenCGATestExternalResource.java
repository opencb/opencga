/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.variant;

import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created on 26/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class OpenCGATestExternalResource extends ExternalResource {

    private CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();
    private Path opencgaHome;
    private String storageEngine;
    private boolean initiated = false;

    Logger logger = LoggerFactory.getLogger(OpenCGATestExternalResource.class);
    private StorageConfiguration storageConfiguration;
    private StorageEngineFactory storageEngineFactory;
    private ToolRunner toolRunner;


    public static HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();

    public OpenCGATestExternalResource() {
        this(false);
    }

    public OpenCGATestExternalResource(boolean storageHadoop) {
        if (storageHadoop) {
            this.storageEngine = HadoopVariantStorageEngine.STORAGE_ENGINE_ID;
        } else {
            this.storageEngine = DummyVariantStorageEngine.STORAGE_ENGINE_ID;
        }
    }

    @Override
    public void before() throws Exception {
        before(storageEngine);
    }

    public void before(String storageEngine) throws Exception {
        if (initiated) {
            throw new IllegalArgumentException("Unable to call 'before'. " + getClass().getName() + " already initialized");
        }
        this.storageEngine = storageEngine;
        catalogManagerExternalResource.before();

        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();
            hadoopExternalResource.before();
        }
        opencgaHome = isolateOpenCGA();
        Files.createDirectory(opencgaHome.resolve("storage"));
        VariantStorageBaseTest.setRootDir(opencgaHome.resolve("storage"));
        clearStorageDB();
        initiated = true;
        toolRunner = new ToolRunner(opencgaHome.toString(), getCatalogManager(), getStorageEngineFactory());
//        ExecutorFactory.LOCAL_EXECUTOR_FACTORY.set((c, s) -> new StorageLocalExecutorManager(s));
    }

    @Override
    public void after() {
        if (!initiated) {
            // Ignore
            return;
        }
        super.after();
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            try {
                hadoopExternalResource.getVariantStorageEngine().getMetadataManager().clearCaches();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            hadoopExternalResource.after();
        }
        catalogManagerExternalResource.after();
        initiated = false;
        try {
            if (storageEngineFactory != null) {
                storageEngineFactory.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
//        if (storageHadoop) {
//            hadoopExternalResource.after();
//        }
    }

    public Path getOpencgaHome() {
        return opencgaHome;
    }

    public CatalogManager getCatalogManager() {
        return catalogManagerExternalResource.getCatalogManager();
    }

    public CatalogManagerExternalResource getCatalogManagerExternalResource() {
        return catalogManagerExternalResource;
    }

    public String getAdminToken() {
        return catalogManagerExternalResource.getAdminToken();
    }

    public StorageEngineFactory getStorageEngineFactory() {
        return storageEngineFactory;
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public Configuration getConfiguration() {
        return catalogManagerExternalResource.getConfiguration();
    }

    public ToolRunner getToolRunner() {
        return toolRunner;
    }

    public VariantStorageManager getVariantStorageManager() {
        return new VariantStorageManager(getCatalogManager(), getStorageEngineFactory());
    }

    public URI getResourceUri(String resourceName) throws IOException {
        return getResourceUri(resourceName, resourceName);
    }

    public URI getResourceUri(String resourceName, String targetName) throws IOException {
        return VariantStorageBaseTest.getResourceUri(resourceName, targetName, opencgaHome.resolve("resources"));
    }

    public VariantStorageManager getVariantStorageManager(VariantSolrExternalResource solrExternalResource) {
        return new VariantStorageManager(getCatalogManager(), getStorageEngineFactory()) {
            @Override
            protected VariantStorageEngine getVariantStorageEngineByProject(String project, ObjectMap params, String token) throws StorageEngineException, CatalogException {
                VariantStorageEngine engine = super.getVariantStorageEngineByProject(project, params, token);
                solrExternalResource.configure(engine);
                return engine;
            }

            @Override
            protected VariantStorageEngine getVariantStorageEngineForStudyOperation(String studyStr, ObjectMap params, String token) throws StorageEngineException, CatalogException {
                VariantStorageEngine engine = super.getVariantStorageEngineForStudyOperation(studyStr, params, token);
                solrExternalResource.configure(engine);
                return engine;
            }
        };
    }

    public Path isolateOpenCGA() throws IOException {
//        Path opencgaHome = Paths.get("/tmp/opencga-analysis-test");
        Path opencgaHome = catalogManagerExternalResource.getOpencgaHome();
        Path userHome = opencgaHome.resolve("user_home");
        Path conf = opencgaHome.resolve("conf");

        System.setProperty("app.home", opencgaHome.toString());
        System.setProperty("user.home", userHome.toString());

        Files.createDirectories(conf);
        Files.createDirectories(userHome);

        catalogManagerExternalResource.getConfiguration().getAdmin().setSecretKey(null);
        catalogManagerExternalResource.getConfiguration().getAdmin().setAlgorithm(null);
        catalogManagerExternalResource.getConfiguration().serialize(
                new FileOutputStream(conf.resolve("configuration.yml").toFile()));
        InputStream inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml");

        storageConfiguration = StorageConfiguration.load(inputStream, "yml");

        storageConfiguration.getVariant().setDefaultEngine(storageEngine);
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            HadoopVariantStorageTest.updateStorageConfiguration(storageConfiguration, hadoopExternalResource.getConf());
            ObjectMap variantHadoopOptions = storageConfiguration.getVariantEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID).getOptions();
            for (Map.Entry<String, String> entry : hadoopExternalResource.getConf()) {
                variantHadoopOptions.put(entry.getKey(), entry.getValue());
            }
        }
        try (OutputStream os = new FileOutputStream(conf.resolve("storage-configuration.yml").toFile())) {
            storageConfiguration.serialize(os);
        }
        StorageEngineFactory.configure(storageConfiguration);
        storageEngineFactory = StorageEngineFactory.get(storageConfiguration);

//        inputStream = StorageEngine.class.getClassLoader().getResourceAsStream("client-configuration-test.yml");
//        Files.copy(inputStream, conf.resolve("client-configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

//        inputStream = StorageEngine.class.getClassLoader().getResourceAsStream("configuration.yml");
//        Files.copy(inputStream, conf.resolve("configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        // Example files
        Files.createDirectories(opencgaHome.resolve("examples"));

//        inputStream = new FileInputStream("app/examples/20130606_g1k.ped");
//        Files.copy(inputStream, opencgaHome.resolve("examples").resolve("20130606_g1k.ped"), StandardCopyOption.REPLACE_EXISTING);
//
//        inputStream = new FileInputStream("app/examples/1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
//        Files.copy(inputStream, opencgaHome.resolve("examples")
//                .resolve("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), StandardCopyOption.REPLACE_EXISTING);

        // Mutational signatue analysis
        Path analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/mutational-signature")).toAbsolutePath();
        inputStream = new FileInputStream("../opencga-app/app/analysis/mutational-signature/sv_clustering.R");
        Files.copy(inputStream, analysisPath.resolve("sv_clustering.R"), StandardCopyOption.REPLACE_EXISTING);

        // Pedigree graph analysis
        analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/pedigree-graph")).toAbsolutePath();
        inputStream = new FileInputStream("../opencga-app/app/analysis/pedigree-graph/ped.R");
        Files.copy(inputStream, analysisPath.resolve("ped.R"), StandardCopyOption.REPLACE_EXISTING);

        return opencgaHome;
    }

    public File createFile(String studyId, String resourceName, String sessionId) throws IOException, CatalogException {
        URI uri = getResourceUri(resourceName);
        CatalogManager catalogManager = getCatalogManager();
        String folder = "data/";
        if (resourceName.contains("/")) {
            int idx = resourceName.lastIndexOf("/");
            folder += resourceName.substring(0, idx);
            resourceName = resourceName.substring(idx + 1);
        }
        catalogManager.getFileManager().createFolder(studyId, folder, true, "", new QueryOptions(), sessionId);
        catalogManager.getFileManager().link(studyId, uri, folder, new ObjectMap(), sessionId);
//        System.out.println("resourceName = " + resourceName);
//        file = new FileMetadataReader(catalogManager).create(studyId, uri, "data/vcfs/", "", true, null, sessionId).first();
//        new FileUtils(catalogManager).upload(uri, file, null, sessionId, false, false, true, false, Long.MAX_VALUE);
        return catalogManager.getFileManager().get(studyId, resourceName, null, sessionId).first();
    }

    public void clear() throws Exception {
        clearCatalog();
        clearStorageDB();
    }

    public void clearCatalog() throws Exception {
        catalogManagerExternalResource.after();
        catalogManagerExternalResource.before();
    }

    public void clearStorageDB(String dbName) {
        clearStorageDB(storageEngine, dbName);
    }

    public void clearStorageDB(String storageEngine, String dbName) {
        if (storageEngine.equalsIgnoreCase("MONGODB")) {
            logger.info("Cleaning MongoDB {}", dbName);
            MongoDataStoreManager mongoManager = new MongoDataStoreManager("localhost", 27017);
            MongoDataStore mongoDataStore = mongoManager.get(dbName);
            mongoManager.drop(dbName);
        } else if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)){
            try {
                hadoopExternalResource.clearDB(dbName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (DummyVariantStorageEngine.STORAGE_ENGINE_ID.equals(storageEngine)) {
            DummyVariantStorageMetadataDBAdaptorFactory.clear();
        }
    }

    public void clearStorageDB() {
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)){
            try {
                hadoopExternalResource.clearHBase();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (DummyVariantStorageEngine.STORAGE_ENGINE_ID.equals(storageEngine)) {
            DummyVariantStorageMetadataDBAdaptorFactory.clear();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public String createTmpOutdir(String studyId, String suffix, String sessionId) throws CatalogException, IOException {
        return createTmpOutdir(suffix);
    }

    public String createTmpOutdir() throws IOException {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        // stackTrace[0] = "Thread.currentThread"
        // stackTrace[1] = "newOutputUri"
        // stackTrace[2] =  caller method
        String testName = stackTrace[2].getMethodName();
        return createTmpOutdir(testName);
    }

    public String createTmpOutdir(String suffix) throws IOException {
        if (suffix.endsWith("_")) {
            suffix = suffix.substring(0, suffix.length() - 1);
        }
        String folder = "I_tmp_" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss.SSS").format(new Date()) + suffix;
        Path tmpOutDir = Paths.get(getCatalogManager().getConfiguration().getJobDir()).resolve(folder);
        Files.createDirectories(tmpOutDir);
        return tmpOutDir.toString();
//        return getCatalogManager().getJobManager().createJobOutDir(studyId, "I_tmp_" + date + sufix, sessionId).toString();
    }

//    private class StorageLocalExecutorManager extends LocalExecutorManager {
//
//        public StorageLocalExecutorManager(String sessionId) {
//            super(OpenCGATestExternalResource.this.catalogManagerExternalResource.getCatalogManager(), sessionId);
//        }
//        protected final Logger logger = LoggerFactory.getLogger(StorageLocalExecutorManager.class);
//
//        @Override
//        public QueryResult<Job> run(Job job) throws CatalogException, ExecutionException, IOException {
//
//            String[] args = Commandline.translateCommandline(job.getCommandLine());
//            int exitValue;
//            if (args[0].contains(AnalysisFileIndexer.OPENCGA_STORAGE_BIN_NAME)) {
//                logger.info("==========================================");
//                logger.info("Executing opencga-storage " + job.getName());
//                logger.info("==========================================");
//                exitValue = StorageMain.privateMain((Arrays.copyOfRange(args, 1, args.length)));
//                logger.info("==========================================");
//                logger.info("Finish opencga-storage");
//                logger.info("==========================================");
//            } else if (args[0].contains(AnalysisFileIndexer.OPENCGA_ANALYSIS_BIN_NAME)) {
//                logger.info("==========================================");
//                logger.info("Executing opencga-analysis " + job.getName());
//                logger.info("==========================================");
//                exitValue = AnalysisMain.privateMain((Arrays.copyOfRange(args, 1, args.length)));
//                logger.info("==========================================");
//                logger.info("Finish opencga-analysis");
//                logger.info("==========================================");
//            } else {
//                logger.info("Executing external job!");
//                return super.run(job);
//            }
//
//            return this.postExecuteLocal(job, exitValue, new ObjectMap(), null);
//        }
//    }
}
