package org.opencb.opencga.analysis.storage;

import org.apache.tools.ant.types.Commandline;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.storage.app.StorageMain;
import org.opencb.opencga.storage.core.StorageManager;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

/**
 * Created on 26/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AnalysisStorageTestUtil {


    public static Path isolateOpenCGA() throws IOException {
        Path opencgaHome = Paths.get("/tmp/opencga-analysis-test");
        System.setProperty("app.home", opencgaHome.toString());
        Config.setOpenCGAHome(opencgaHome.toString());

        if (opencgaHome.toFile().exists()) {
            IOUtils.deleteDirectory(opencgaHome);
        }
        Files.createDirectories(opencgaHome);
        Files.createDirectories(opencgaHome.resolve("conf"));

        InputStream inputStream = CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("catalog.properties"), StandardCopyOption.REPLACE_EXISTING);
        inputStream = new ByteArrayInputStream((AnalysisJobExecutor.OPENCGA_ANALYSIS_JOB_EXECUTOR + "=LOCAL" + "\n" +
                AnalysisFileIndexer.OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX + "=" + "opencga_test_").getBytes());
        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("analysis.properties"), StandardCopyOption.REPLACE_EXISTING);
        inputStream = StorageManager.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        Files.copy(inputStream, opencgaHome.resolve("conf").resolve("storage-configuration.yml"), StandardCopyOption.REPLACE_EXISTING);

        return opencgaHome;
    }


    public static Job runStorageJob(CatalogManager catalogManager, Job storageJob, Logger logger, String sessionId) throws AnalysisExecutionException, IOException, CatalogException {
        logger.info("==========================================");
        logger.info("Executing opencga-storage");
        logger.info("==========================================");
        String[] args = Commandline.translateCommandline(storageJob.getCommandLine());
        StorageMain.privateMain((Arrays.copyOfRange(args, 1, args.length)));
        logger.info("==========================================");
        logger.info("Finish opencga-storage");
        logger.info("==========================================");

        storageJob.setCommandLine("echo 'Executing fake CLI :' " + storageJob.getCommandLine());
        AnalysisJobExecutor.execute(catalogManager, storageJob, sessionId);
        return catalogManager.getJob(storageJob.getId(), null, sessionId).first();
    }

}
