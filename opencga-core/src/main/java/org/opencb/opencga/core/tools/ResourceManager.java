package org.opencb.opencga.core.tools;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.utils.VersionUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ResourceManager  {

    private Path openCgaHome;
    private String baseurl;
    private Configuration configuration;
    private VersionUtils.Version version;

    // Locking mechanism to prevent concurrent downloads for the same analysis
    private static final Map<String, Lock> analysisLocks = new ConcurrentHashMap<>();
    private static final long LOCK_TIMEOUT = 2; // Timeout of 2 hours

    public static final String RESOURCES_TXT_FILENAME = "resources.txt";
    public static final String CONFIGURATION_FILENAME = "configuration.yml";

    public static final String CONF_DIRNAME = "conf";
    public static final String ANALYSIS_DIRNAME = "analysis";
    public static final String RESOURCES_DIRNAME = "resources";


    protected static Logger logger = LoggerFactory.getLogger(ResourceManager.class);

    public ResourceManager(Path openCgaHome) {
        this.openCgaHome = openCgaHome;
        this.version = new VersionUtils.Version(GitRepositoryState.getInstance().getBuildVersion());
    }

    public ResourceManager(Path openCgaHome, String baseurl) {
        this.openCgaHome = openCgaHome;
        this.baseurl = baseurl;
        this.version = new VersionUtils.Version(GitRepositoryState.getInstance().getBuildVersion());
    }

    public File getResourceFile(String analysisId, String resourceName) throws IOException {
        loadConfiguration();

        // Create a lock for the analysis if not present, and get it for the input analysis
        analysisLocks.putIfAbsent(analysisId, new ReentrantLock());
        Lock lock = analysisLocks.get(analysisId);

        boolean lockAcquired = false;
        try {
            // Try to acquire the lock within the specified timeout
            lockAcquired = lock.tryLock(LOCK_TIMEOUT, TimeUnit.HOURS);

            if (lockAcquired) {
                // Create the analysis resources directory if it doesn't exist
                Path analysisResourcesPath = openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME).resolve(analysisId);
                if (!Files.exists(analysisResourcesPath)) {
                    logger.info("Creating folder for '{}' resources: {}", analysisId, analysisResourcesPath);
                    Files.createDirectories(analysisResourcesPath);
                }

                // Download each file
                return downloadFile(baseurl, analysisId, resourceName, analysisResourcesPath).toFile();
            } else {
                String msg = "Could not acquire lock for analysis '" + analysisId + "' within " + LOCK_TIMEOUT + " hours. Skipping...";
                logger.error(msg);
                throw new RuntimeException(msg);
            }
        } catch (InterruptedException e) {
            // Restore interrupt status
            Thread.currentThread().interrupt();
            String msg = "Interrupted while trying to acquire lock for analysis '" + analysisId + "' resources";
            logger.error(msg);
            throw new RuntimeException(msg, e);
        } finally {
            if (lockAcquired) {
                // Release the lock
                lock.unlock();
            }
        }
    }

    public List<File> getResourceFiles(String analysisId) throws IOException {
        loadConfiguration();

        // Get resource filenames for the input analysis
        Path resourcesTxt = openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(analysisId).resolve(RESOURCES_TXT_FILENAME);
        List<String> filenames = readAllLines(resourcesTxt);

        List<File> downloadedFiles = new ArrayList<>();
        for (String filename : filenames) {
            downloadedFiles.add(getResourceFile(analysisId, filename));
        }

        return downloadedFiles;
    }

    //-------------------------------------------------------------------------
    //  P R I V A T E      M E T H O D S
    //-------------------------------------------------------------------------

    private void loadConfiguration() throws IOException {
        if (configuration == null) {
            this.configuration = Configuration.load(new FileInputStream(openCgaHome.resolve(CONF_DIRNAME)
                    .resolve(CONFIGURATION_FILENAME).toFile()));
            // Set base URL from configuration, if not set
//            if (baseurl == null) {
//                baseurl = this.configuration.getAnalysis().getResourceUrl();
//            }
        }
    }

    private List<String> readAllLines(Path path) throws IOException {
        List<String> lines;
        if (!Files.exists(path)) {
            String msg = "Filename '" + path + "' does not exist";
            logger.error(msg);
            throw new IOException(msg);
        }
        lines = Files.readAllLines(path);
        if (CollectionUtils.isEmpty(lines)) {
            String msg = "Filename '" + path + "' is empty";
            logger.error(msg);
            throw new IOException(msg);
        }
        return lines;
    }

    private Path downloadFile(String baseUrl, String analysisId, String filename, Path localPath) throws IOException {
        String fileUrl = baseUrl + analysisId + "/" + filename;
        Path localFile = localPath.resolve(filename);

        // Check if the file already exists
        if (Files.exists(localFile)) {
            logger.info("Resource file '{}' for analysis '{}' already exists, skipping download", filename, analysisId);
            return localFile;
        }

        logger.info("Downloading resource file '{}' for analysis '{}'...", filename, analysisId);
        try (BufferedInputStream in = new BufferedInputStream(new URL(fileUrl).openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(localFile.toFile())) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        }

        if (!Files.exists(localFile)) {
            String msg = "Something wrong happened, file '" + filename + "' + does not exist after downloading at '" + fileUrl + "'";
            logger.error(msg);
            throw new IOException(msg);
        }
        logger.info("Done: '{}' downloaded", filename);

        return localFile;
    }

    //-------------------------------------------------------------------------
    //  T O     S T R I N G
    //-------------------------------------------------------------------------

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResourceManager{");
        sb.append("configuration=").append(configuration);
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
    }

    //-------------------------------------------------------------------------
    //  G E T T E R S     &      S E T T E R S
    //-------------------------------------------------------------------------

    public Configuration getConfiguration() {
        return configuration;
    }

    public VersionUtils.Version getVersion() {
        return version;
    }
}
