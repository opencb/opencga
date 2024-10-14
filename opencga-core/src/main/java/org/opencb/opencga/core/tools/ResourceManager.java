package org.opencb.opencga.core.tools;

import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.utils.VersionUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.resource.AnalysisResource;
import org.opencb.opencga.core.models.resource.ResourceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.InvalidParameterException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ResourceManager  {

    public static final String OK = "Ok";
    private Path openCgaHome;
    private String baseUrl;
    private Configuration configuration;

    // Locking mechanism to prevent concurrent downloads for the same analysis
    private static final Map<String, Lock> analysisLocks = new ConcurrentHashMap<>();
    private static final long LOCK_TIMEOUT = 2; // Timeout of 2 hours

    public static final String CONFIGURATION_FILENAME = "configuration.yml";
    public static final String CONF_FOLDER_NAME = "conf";
    public static final String ANALYSIS_FOLDER_NAME = "analysis";
    public static final String RESOURCES_FOLDER_NAME = "resources";
    public static final String RELEASES_FOLDER_NAME = "releases";
    public static final String DATA_FOLDER_NAME = "data";

    protected static Logger logger = LoggerFactory.getLogger(ResourceManager.class);

    public ResourceManager(Path openCgaHome) {
        this(openCgaHome, null);
    }

    public ResourceManager(Path openCgaHome, String baseurl) {
        this.openCgaHome = openCgaHome;
        this.baseUrl = baseurl;
    }

    public void fetchAllResources(Path tmpPath, boolean overwrite) throws IOException, NoSuchAlgorithmException {
        loadConfiguration();

        // Download resources
        ResourceMetadata metadata = getResourceMetadata(true);
        for (AnalysisResource analysisResources : metadata.getAnalysisResources()) {
            getResourceFiles(analysisResources, tmpPath, overwrite);
        }

        // Move resources to installation folder
        move(tmpPath, openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME));
    }

    public List<File> getResourceFiles(String analysisId) throws IOException, NoSuchAlgorithmException {
        loadConfiguration();

        // Sanity check
        if (StringUtils.isEmpty(analysisId)) {
            throw new InvalidParameterException("Analysis ID is empty");
        }

        ResourceMetadata metadata = getResourceMetadata(false);
        for (AnalysisResource analysisResource : metadata.getAnalysisResources()) {
            if (analysisId.equalsIgnoreCase(analysisResource.getId())) {
                return getResourceFiles(analysisResource, false);
            }
        }
        throw new InvalidParameterException("Analysis ID '" + analysisId + "' not found in resource repository");
    }

    public File getResourceFile(String analysisId, String resourceName) throws IOException, NoSuchAlgorithmException {
        loadConfiguration();

        // Sanity check
        if (StringUtils.isEmpty(analysisId)) {
            throw new InvalidParameterException("Analysis ID is empty");
        }
        if (StringUtils.isEmpty(resourceName)) {
            throw new InvalidParameterException("Resource name is empty");
        }

        ResourceMetadata metadata = getResourceMetadata(false);
        for (AnalysisResource analysisResource : metadata.getAnalysisResources()) {
            if (analysisId.equalsIgnoreCase(analysisResource.getId())) {
                for (String resource : analysisResource.getResources()) {
                    String name = Paths.get(resource).getFileName().toString();
                    if (resourceName.equals(name)) {
                        Path resourcesPath = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME);
                        Path analysisResourcesPath = resourcesPath.resolve(analysisId);
                        if (!Files.exists(analysisResourcesPath)) {
                            logger.info("Creating folder for '{}' resources: {}", analysisId, analysisResourcesPath);
                            Files.createDirectories(analysisResourcesPath);
                        }
                        return downloadFile(baseUrl, analysisId, Paths.get(DATA_FOLDER_NAME, resource).toString(), resourcesPath, false)
                                .toFile();
                    }
                }
                throw new InvalidParameterException("Resource '" + resourceName + "' for '" + analysisId + "' not found in resource"
                        + " repository");
            }
        }
        throw new InvalidParameterException("Analysis ID '" + analysisId + "' not found in resource repository");
    }

    //-------------------------------------------------------------------------
    //  P R I V A T E      M E T H O D S
    //-------------------------------------------------------------------------

    private void loadConfiguration() throws IOException {
        if (configuration == null) {
            this.configuration = Configuration.load(new FileInputStream(openCgaHome.resolve(CONF_FOLDER_NAME)
                    .resolve(CONFIGURATION_FILENAME).toFile()));

            if (baseUrl == null) {
                baseUrl = configuration.getAnalysis().getResourceUrl();
            }
        }
    }

    private List<File> getResourceFiles(AnalysisResource analysisResource, boolean overwrite) throws IOException, NoSuchAlgorithmException {
        return getResourceFiles(analysisResource, openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME), overwrite);
    }

    private List<File> getResourceFiles(AnalysisResource analysisResource, Path resourcePath, boolean overwrite)
            throws IOException, NoSuchAlgorithmException {
        List<File> downloadedFiles = new ArrayList<>();

        String analysisId = analysisResource.getId();
        Path analysisResourcesPath = resourcePath.resolve(analysisId);
        if (!Files.exists(analysisResourcesPath)) {
            logger.info("Creating folder for '{}' resources: {}", analysisId, analysisResourcesPath);
            Files.createDirectories(analysisResourcesPath);
        }
        for (String resource : analysisResource.getResources()) {
            Path downloadedPath = downloadFile(baseUrl, analysisId, Paths.get(DATA_FOLDER_NAME, resource).toString(), resourcePath,
                    overwrite);
            downloadedFiles.add(downloadedPath.toFile());
        }
        return downloadedFiles;
    }

    private Path downloadFile(String baseUrl, String filename, Path resourcePath, boolean overwrite)
            throws IOException, NoSuchAlgorithmException {
        return downloadFile(baseUrl, null, filename, resourcePath, overwrite);
    }

    private Path downloadFile(String baseUrl, String analysisId, String resourceName, Path downloadedPath, boolean overwrite)
            throws IOException, NoSuchAlgorithmException {
        String cleanName = resourceName;
        if (resourceName.startsWith(RELEASES_FOLDER_NAME)) {
            cleanName = resourceName.substring(RELEASES_FOLDER_NAME.length() + 1);
        } else if (resourceName.startsWith(DATA_FOLDER_NAME)) {
            cleanName = resourceName.substring(DATA_FOLDER_NAME.length() + 1);
        }

        String fileUrl = baseUrl + resourceName;

        Path installationFile;
        Path tmpFile;
        if (StringUtils.isEmpty(analysisId)) {
            installationFile = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME).resolve(cleanName);
            tmpFile = downloadedPath.resolve(cleanName);
        } else {
            installationFile = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME).resolve(analysisId)
                    .resolve(cleanName);
            tmpFile = downloadedPath.resolve(analysisId).resolve(cleanName);
        }

        // Check if the file already exists
        if (!overwrite && Files.exists(installationFile)) {
            logger.info("Resource file '{}' already downloaded, skipping download", resourceName);
            return tmpFile;
        }

        // Create a lock for the analysis if not present, and get it for the input analysis
        analysisLocks.putIfAbsent(fileUrl, new ReentrantLock());
        Lock lock = analysisLocks.get(fileUrl);

        boolean lockAcquired = false;
        try {
            // Try to acquire the lock within the specified timeout
            lockAcquired = lock.tryLock(LOCK_TIMEOUT, TimeUnit.HOURS);

            if (lockAcquired) {
                // Download resource file
                logger.info("Downloading resource file '{}' ...", resourceName);
                donwloadFile(new URL(fileUrl), tmpFile);
                logger.info("Done: '{}' downloaded", resourceName);

                // Download MD5 for the resource file
                final String md5Ext = ".md5";
                String md5Filename = cleanName + md5Ext;
                logger.info("Downloading MD5, '{}' ...", md5Filename);
                donwloadFile(new URL(fileUrl + md5Ext), downloadedPath.resolve(md5Filename));
                logger.info("Done: '{}' MD5 downloaded", md5Filename);

                // Checking MD5
                validateMD5(tmpFile, downloadedPath.resolve(md5Filename));
            }
        } catch (InterruptedException e) {
            // Restore interrupt status
            Thread.currentThread().interrupt();
            String msg = "Interrupted while trying to acquire lock for resource '" + fileUrl + "'";
            logger.error(msg);
            throw new RuntimeException(msg, e);
        } finally {
            if (lockAcquired) {
                // Release the lock
                lock.unlock();
            }
        }

        return tmpFile;
    }

    private void donwloadFile(URL url, Path resourcePath) throws IOException {
        try (BufferedInputStream in = new BufferedInputStream(url.openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(resourcePath.toFile())) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        }

        if (!Files.exists(resourcePath)) {
            String msg = "Something wrong happened downloading '" + url + "'";
            logger.error(msg);
            throw new IOException(msg);
        }
    }

    public void validateMD5(Path filePath, Path md5filePath) throws IOException, NoSuchAlgorithmException, InputMismatchException {
        String expectedMD5 = new String(Files.readAllBytes(md5filePath)).trim();
        String actualMD5 = computeMD5(filePath);
        if (!expectedMD5.equals(actualMD5)) {
            throw new InputMismatchException("MD5 checksum mismatch! File may be corrupted.");
        }
    }

    private static String computeMD5(Path filePath) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try (FileInputStream fis = new FileInputStream(filePath.toString())) {
            byte[] dataBytes = new byte[1024];
            int bytesRead;

            while ((bytesRead = fis.read(dataBytes)) != -1) {
                md.update(dataBytes, 0, bytesRead);
            }
        }
        byte[] mdBytes = md.digest();

        // Convert byte array to hex string
        StringBuilder sb = new StringBuilder();
        for (byte b : mdBytes) {
            sb.append(String.format("%02x", b));
        }

        return sb.toString();
    }

    private ResourceMetadata getResourceMetadata(boolean overwrite) throws IOException, NoSuchAlgorithmException {
        String version = getVersion();
        String resourceName = String.format("%s/release-%s.json", RELEASES_FOLDER_NAME, version);
        Path analysisResourcesPath = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME);
        Path path = downloadFile(baseUrl, resourceName, analysisResourcesPath, overwrite);
        ObjectReader objectReader = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class);
        return objectReader.readValue(path.toFile());
    }

    public String getVersion() {
        VersionUtils.Version version = new VersionUtils.Version(GitRepositoryState.getInstance().getBuildVersion());
        return String.format("%d.%d.%d", version.getMajor(), version.getMinor(), version.getPatch());
    }

    public static void move(Path sourceDir, Path targetDir) throws IOException {
        // Ensure the target directory exists
        if (!Files.exists(targetDir)) {
            logger.info("Creating directory {} ...", targetDir.toAbsolutePath());
            Files.createDirectories(targetDir);
            logger.info(OK);
        }

        // Walk through the directory tree at sourceDir
        Files.walkFileTree(sourceDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                // Create corresponding subdirectory in targetDir
                Path targetSubDir = targetDir.resolve(sourceDir.relativize(dir));
                if (!Files.exists(targetSubDir)) {
                    logger.info("Creating directory {} ...", targetSubDir.toAbsolutePath());
                    Files.createDirectory(targetSubDir);
                    logger.info(OK);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                // Move each file to target directory
                Path targetFile = targetDir.resolve(sourceDir.relativize(file));
                logger.info("Moving {} to {} ...", file.toAbsolutePath(), targetFile.toAbsolutePath());
                Files.move(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
                logger.info(OK);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                // Delete the original directory after moving all its content
                logger.info("Deleting source directory {} ...", dir.toAbsolutePath());
                Files.delete(dir);
                logger.info(OK);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    //-------------------------------------------------------------------------
    //  T O     S T R I N G
    //-------------------------------------------------------------------------

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResourceManager{");
        sb.append("configuration=").append(configuration);
        sb.append('}');
        return sb.toString();
    }

    //-------------------------------------------------------------------------
    //  G E T T E R S     &      S E T T E R S
    //-------------------------------------------------------------------------

    public Configuration getConfiguration() {
        return configuration;
    }
}
