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
import org.opencb.opencga.catalog.db.mongodb.MongoBackupUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created on 26/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class OpenCGATestExternalResource extends ExternalResource {

    private final CatalogManagerExternalResource catalogManagerExternalResource;
    private Path opencgaHome;
    private String storageEngine;
    private boolean initiated = false;

    Logger logger = LoggerFactory.getLogger(OpenCGATestExternalResource.class);
    private StorageConfiguration storageConfiguration;
    private StorageEngineFactory storageEngineFactory;
    private ToolRunner toolRunner;
    protected Path sourceAnalysisPath;

    public static HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource
            = new HadoopVariantStorageTest.HadoopExternalResource();

    public OpenCGATestExternalResource() {
        this(false, Paths.get("../opencga-app/app/analysis/"));
    }

    public OpenCGATestExternalResource(boolean storageHadoop) {
        this(storageHadoop, Paths.get("../opencga-app/app/analysis/"));
    }

    public OpenCGATestExternalResource(boolean storageHadoop, Path sourceAnalysisPath) {
        if (storageHadoop) {
            this.storageEngine = HadoopVariantStorageEngine.STORAGE_ENGINE_ID;
        } else {
            this.storageEngine = DummyVariantStorageEngine.STORAGE_ENGINE_ID;
        }
        this.sourceAnalysisPath = sourceAnalysisPath;
        catalogManagerExternalResource = new CatalogManagerExternalResource(sourceAnalysisPath);
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
        Files.createDirectories(opencgaHome.resolve("analysis/resources"));

        catalogManagerExternalResource.getConfiguration().serialize(
                new FileOutputStream(conf.resolve("configuration.yml").toFile()));
        try (InputStream inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml")) {
            storageConfiguration = StorageConfiguration.load(inputStream, "yml");
        }

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

        if (storageEngine.equals(DummyVariantStorageEngine.STORAGE_ENGINE_ID)) {
            DummyVariantStorageEngine.configure(getStorageEngineFactory(), true);
        }
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
        try (FileInputStream inputStream = new FileInputStream("../opencga-app/app/analysis/mutational-signature/sv_clustering.R")) {
            Files.copy(inputStream, analysisPath.resolve("sv_clustering.R"), StandardCopyOption.REPLACE_EXISTING);
        }

        // Pedigree graph analysis
        analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/pedigree-graph")).toAbsolutePath();
        try (FileInputStream inputStream = new FileInputStream("../opencga-app/app/analysis/pedigree-graph/ped.R")) {
            Files.copy(inputStream, analysisPath.resolve("ped.R"), StandardCopyOption.REPLACE_EXISTING);
        }

        // Exomiser analysis files
        List<String> exomiserVersions = Arrays.asList("13.1.0", "14.0.0");
        List<String> exomiserFiles = Arrays.asList("application.properties", "exomiser-analysis.yml", "output.yml");
        for (String exomiserVersion : exomiserVersions) {
            analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/exomiser").resolve(exomiserVersion).toAbsolutePath());
            Path exomiserPath = Paths.get("../opencga-app/app/analysis/exomiser");
            for (String exomiserFile : exomiserFiles) {
                String resource = exomiserVersion + "/" + exomiserFile;
                Files.copy(exomiserPath.resolve(resource).toAbsolutePath(), analysisPath.resolve(exomiserFile), StandardCopyOption.REPLACE_EXISTING);
            }
        }

        // Liftover analysis
        analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/liftover")).toAbsolutePath();
        try (FileInputStream inputStream = new FileInputStream("../opencga-app/app/analysis/liftover/liftover.sh")) {
            Files.copy(inputStream, analysisPath.resolve("liftover.sh"), StandardCopyOption.REPLACE_EXISTING);
        }

        // NGS pipeline analysis
        analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/ngs-pipeline")).toAbsolutePath();
        List<String> ngsFiles = Arrays.asList("main.py");
        for (String ngsFile : ngsFiles) {
            try (FileInputStream inputStream = new FileInputStream("../opencga-app/app/analysis/ngs-pipeline/" + ngsFile)) {
                Files.copy(inputStream, analysisPath.resolve(ngsFile), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/ngs-pipeline/processing")).toAbsolutePath();
        ngsFiles = Arrays.asList("__init__.py", "alignment.py", "base_processor.py", "prepare_reference_indexes.py", "quality_control.py",
                "variant_calling.py");
        for (String ngsFile : ngsFiles) {
            try (FileInputStream inputStream = new FileInputStream("../opencga-app/app/analysis/ngs-pipeline/processing/" + ngsFile)) {
                Files.copy(inputStream, analysisPath.resolve(ngsFile), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/ngs-pipeline/processing/aligners")).toAbsolutePath();
        ngsFiles = Arrays.asList("__init__.py", "aligner.py", "bwa_aligner.py", "bwamem2_aligner.py");
        for (String ngsFile : ngsFiles) {
            try (FileInputStream inputStream = new FileInputStream("../opencga-app/app/analysis/ngs-pipeline/processing/aligners/"
                    + ngsFile)) {
                Files.copy(inputStream, analysisPath.resolve(ngsFile), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        analysisPath = Files.createDirectories(opencgaHome.resolve("analysis/ngs-pipeline/processing/variant_callers")).toAbsolutePath();
        ngsFiles = Arrays.asList("__init__.py");
        for (String ngsFile : ngsFiles) {
            try (FileInputStream inputStream = new FileInputStream("../opencga-app/app/analysis/ngs-pipeline/processing/variant_callers/"
                    + ngsFile)) {
                Files.copy(inputStream, analysisPath.resolve(ngsFile), StandardCopyOption.REPLACE_EXISTING);
            }
        }

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
        return catalogManagerExternalResource.createTmpOutdir(suffix);
    }

    public String createTmpOutdir() throws IOException {
        return catalogManagerExternalResource.createTmpOutdir();
    }

    public String createTmpOutdir(String suffix) throws IOException {
        return catalogManagerExternalResource.createTmpOutdir(suffix);
    }

    public void restore(URL resource) throws Exception {
        if (resource.getProtocol().equals("jar")) {
            Reflections reflections = new Reflections(resource.getPath().replace('/','.'), new ResourcesScanner());
            Set<String> resources = reflections.getResources(x -> true);
            for (String file : resources) {
                catalogManagerExternalResource.getResourceUri(file.replace('.', '/'));
            }
            MongoBackupUtils.restore(getCatalogManager(), opencgaHome, opencgaHome
                    .resolve("resources")
                    .resolve(resource.getPath())
                    .resolve("mongodb"));
        } else {
            MongoBackupUtils.restore(getCatalogManager(), opencgaHome, Paths.get(resource.toURI()).resolve("mongodb"));
        }
        catalogManagerExternalResource.resetCatalogManager();
    }

    public final VariantStorageEngine getVariantStorageEngineByProject(String projectFqn) throws Exception {
        DataStore dataStore = getVariantStorageManager().getDataStoreByProjectId(projectFqn, getAdminToken());
        VariantStorageEngine variantStorageEngine = storageEngineFactory
                .getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
        if (dataStore.getOptions() != null) {
            variantStorageEngine.getOptions().putAll(dataStore.getOptions());
        }
        return variantStorageEngine;
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
