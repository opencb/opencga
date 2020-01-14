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

package org.opencb.opencga.analysis.variant;

import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

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
    private StorageEngineFactory storageEngineFactory;


//    public HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource;

    public OpenCGATestExternalResource() {
        this(false);
    }

    public OpenCGATestExternalResource(boolean storageHadoop) {
        this.storageHadoop = storageHadoop;
    }

    @Override
    public void before() throws Exception {
        catalogManagerExternalResource.before();

//        if (storageHadoop) {
//            try {
//                String name = HadoopVariantStorageTest.class.getName();
//                Class.forName(name);
//            } catch (ClassNotFoundException e) {
//                logger.error("Missing dependency opencga-storage-hadoop!");
//                throw e;
//            }
//        }

//        if (storageHadoop) {
//            hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();
//            hadoopExternalResource.before();
//        }
        opencgaHome = isolateOpenCGA();
        Files.createDirectory(opencgaHome.resolve("storage"));
        VariantStorageBaseTest.setRootDir(opencgaHome.resolve("storage"));

//        ExecutorFactory.LOCAL_EXECUTOR_FACTORY.set((c, s) -> new StorageLocalExecutorManager(s));
    }

    @Override
    public void after() {
        super.after();

        catalogManagerExternalResource.after();
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

    public StorageEngineFactory getStorageEngineFactory() {
        return storageEngineFactory;
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

        Files.createDirectories(conf);
        Files.createDirectories(userHome);

        catalogManagerExternalResource.getConfiguration().serialize(
                new FileOutputStream(conf.resolve("configuration.yml").toFile()));
        InputStream inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml");

        storageConfiguration = StorageConfiguration.load(inputStream, "yml");
//        if (storageHadoop) {
//            HadoopVariantStorageTest.updateStorageConfiguration(storageConfiguration, hadoopExternalResource.getConf());
//            ObjectMap variantHadoopOptions = storageConfiguration.getVariantEngine("hadoop").getVariant().getOptions();
//            ObjectMap alignmentHadoopOptions = storageConfiguration.getVariantEngine("hadoop").getAlignment().getOptions();
//            for (Map.Entry<String, String> entry : hadoopExternalResource.getConf()) {
//                variantHadoopOptions.put(entry.getKey(), entry.getValue());
//                alignmentHadoopOptions.put(entry.getKey(), entry.getValue());
//            }
//        }
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
        catalogManager.getFileManager().createFolder(studyId, folder, null, true, "", new QueryOptions(), sessionId);
        catalogManager.getFileManager().link(studyId, uri, folder, new ObjectMap(), sessionId);
//        System.out.println("resourceName = " + resourceName);
//        file = new FileMetadataReader(catalogManager).create(studyId, uri, "data/vcfs/", "", true, null, sessionId).first();
//        new FileUtils(catalogManager).upload(uri, file, null, sessionId, false, false, true, false, Long.MAX_VALUE);
        return catalogManager.getFileManager().get(studyId, resourceName, null, sessionId).first();
    }

    public void clearStorageDB(String dbName) {
        clearStorageDB(getStorageConfiguration().getVariant().getDefaultEngine(), dbName);
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

    public String createTmpOutdir(String studyId, String suffix, String sessionId) throws CatalogException, IOException {
        return createTmpOutdir(suffix);
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
