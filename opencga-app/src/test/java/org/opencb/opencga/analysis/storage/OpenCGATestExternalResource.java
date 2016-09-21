package org.opencb.opencga.analysis.storage;

import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.catalog.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.monitor.exceptions.ExecutionException;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManagerTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.getResourceUri;

/**
 * Created on 26/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class OpenCGATestExternalResource extends ExternalResource {

    private CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();
    private Path opencgaHome;
    private boolean storageHadoop;
    Logger logger = LoggerFactory.getLogger(OpenCGATestExternalResource.class);
    private StorageConfiguration storageConfiguration;
    private StorageManagerFactory storageManagerFactory;


    public HadoopVariantStorageManagerTestUtils.HadoopExternalResource hadoopExternalResource;

    public OpenCGATestExternalResource() {
        this(false);
    }

    public OpenCGATestExternalResource(boolean storageHadoop) {
        this.storageHadoop = storageHadoop;
    }

    @Override
    protected void before() throws Throwable {
        super.before();

        catalogManagerExternalResource.before();

        if (storageHadoop) {
            try {
                String name = HadoopVariantStorageManagerTestUtils.class.getName();
                Class.forName(name);
            } catch (ClassNotFoundException e) {
                logger.error("Missing dependency opencga-storage-hadoop!");
                throw e;
            }
        }

        if (storageHadoop) {
            hadoopExternalResource = new HadoopVariantStorageManagerTestUtils.HadoopExternalResource();
            hadoopExternalResource.before();
        }
        opencgaHome = isolateOpenCGA();
        Files.createDirectory(opencgaHome.resolve("storage"));
        VariantStorageManagerTestUtils.setRootDir(opencgaHome.resolve("storage"));

//        ExecutorManager.LOCAL_EXECUTOR_FACTORY.set((c, s) -> new StorageLocalExecutorManager(s));
    }

    @Override
    protected void after() {
        super.after();

        catalogManagerExternalResource.after();
        if (storageHadoop) {
            hadoopExternalResource.after();
        }
    }

    public Path getOpencgaHome() {
        return opencgaHome;
    }

    public CatalogManager getCatalogManager() {
        return catalogManagerExternalResource.getCatalogManager();
    }

    public StorageManagerFactory getStorageManagerFactory() {
        return storageManagerFactory;
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public Path isolateOpenCGA() throws IOException {

//        Path opencgaHome = Paths.get("/tmp/opencga-analysis-test");
        Path opencgaHome = catalogManagerExternalResource.getOpencgaHome();
        Path userHome = opencgaHome.resolve("user_home");
        Path conf = opencgaHome.resolve("conf");

        System.setProperty("app.home", opencgaHome.toString());
        System.setProperty("user.home", userHome.toString());
        Config.setOpenCGAHome(opencgaHome.toString());

        Files.createDirectories(conf);
        Files.createDirectories(userHome);

        InputStream inputStream;
        catalogManagerExternalResource.getCatalogConfiguration().serialize(
                new FileOutputStream(conf.resolve("catalog-configuration.yml").toFile()));

        inputStream = new ByteArrayInputStream((ExecutorManager.OPENCGA_ANALYSIS_JOB_EXECUTOR + "=LOCAL" + "\n" +
                AnalysisFileIndexer.OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX + "=" + "opencga_test_").getBytes());
        Files.copy(inputStream, conf.resolve("analysis.properties"), StandardCopyOption.REPLACE_EXISTING);

        inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml");

        storageConfiguration = StorageConfiguration.load(inputStream, "yml");
        if (storageHadoop) {
            HadoopVariantStorageManagerTestUtils.updateStorageConfiguration(storageConfiguration, hadoopExternalResource.getConf());
            ObjectMap variantHadoopOptions = storageConfiguration.getStorageEngine("hadoop").getVariant().getOptions();
            ObjectMap alignmentHadoopOptions = storageConfiguration.getStorageEngine("hadoop").getAlignment().getOptions();
            for (Map.Entry<String, String> entry : hadoopExternalResource.getConf()) {
                variantHadoopOptions.put(entry.getKey(), entry.getValue());
                alignmentHadoopOptions.put(entry.getKey(), entry.getValue());
            }
        }
        try (OutputStream os = new FileOutputStream(conf.resolve("storage-configuration.yml").toFile())) {
            storageConfiguration.serialize(os);
        }
        storageManagerFactory = StorageManagerFactory.get(storageConfiguration);

        inputStream = StorageManager.class.getClassLoader().getResourceAsStream("client-configuration-test.yml");
        Files.copy(inputStream, conf.resolve("client-configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        inputStream = StorageManager.class.getClassLoader().getResourceAsStream("configuration-test.yml");
        Files.copy(inputStream, conf.resolve("configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        // Example files
        Files.createDirectories(opencgaHome.resolve("examples"));

        inputStream = new FileInputStream("app/examples/20130606_g1k.ped");
        Files.copy(inputStream, opencgaHome.resolve("examples").resolve("20130606_g1k.ped"), StandardCopyOption.REPLACE_EXISTING);

        inputStream = new FileInputStream("app/examples/1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        Files.copy(inputStream, opencgaHome.resolve("examples")
                .resolve("1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), StandardCopyOption.REPLACE_EXISTING);

        return opencgaHome;
    }

    public File createFile(long studyId, String resourceName, String sessionId) throws IOException, CatalogException {
        File file;
        URI uri = getResourceUri(resourceName);
        CatalogManager catalogManager = getCatalogManager();
        file = new FileMetadataReader(catalogManager).create(studyId, uri, "data/vcfs/", "", true, null, sessionId).first();
        new CatalogFileUtils(catalogManager).upload(uri, file, null, sessionId, false, false, true, false, Long.MAX_VALUE);
        return catalogManager.getFile(file.getId(), sessionId).first();
    }

    public static Job runStorageJob(CatalogManager catalogManager, Job job, Logger logger, String sessionId)
            throws AnalysisExecutionException, CatalogException, IOException {
        try {
            ExecutorManager.execute(catalogManager, job, sessionId);
        } catch (ExecutionException e) {
            throw new AnalysisExecutionException(e.getCause());
        }
        return catalogManager.getJob(job.getId(), null, sessionId).first();
    }

    public Job runStorageJob(Job storageJob, String sessionId) throws CatalogException, AnalysisExecutionException, IOException {
        return runStorageJob(getCatalogManager(), storageJob, logger, sessionId);
    }

    public void clearStorageDB(String dbName) {
        clearStorageDB(getStorageConfiguration().getDefaultStorageEngineId(), dbName);
    }

    public void clearStorageDB(String storageEngine, String dbName) {
        if (storageEngine.equalsIgnoreCase("MONGODB")) {
            logger.info("Cleaning MongoDB {}", dbName);
            MongoDataStoreManager mongoManager = new MongoDataStoreManager("localhost", 27017);
            MongoDataStore mongoDataStore = mongoManager.get(dbName);
            mongoManager.drop(dbName);
        } else {
            throw new UnsupportedOperationException();
        }
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
