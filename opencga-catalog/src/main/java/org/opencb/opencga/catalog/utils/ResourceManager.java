package org.opencb.opencga.catalog.utils;

import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.utils.VersionUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.resource.AnalysisResource;
import org.opencb.opencga.core.models.resource.AnalysisResourceList;
import org.opencb.opencga.core.models.resource.ResourceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;

public class ResourceManager  {

    public static final String OK = "Ok";
    public static final String MD5_EXT = ".md5";
    public static final String RESOURCE_MSG = "Resource '";
    private Path openCgaHome;
    private String baseUrl;
    private Configuration configuration;

    // Flag to track if all resources are fetching
    private boolean isFetchingAll = false;

    public static final String CONFIGURATION_FILENAME = "configuration.yml";
    public static final String CONF_FOLDER_NAME = "conf";
    public static final String ANALYSIS_FOLDER_NAME = "analysis";
    public static final String RESOURCES_FOLDER_NAME = "resources";
    public static final String RELEASES_FOLDER_NAME = "releases";

    protected static Logger logger = LoggerFactory.getLogger(ResourceManager.class);

    public ResourceManager(Path openCgaHome) {
        this(openCgaHome, null);
    }

    public ResourceManager(Path openCgaHome, String baseurl) {
        this.openCgaHome = openCgaHome;
        this.baseUrl = baseurl;
    }

    public synchronized void fetchAllResources(Path tmpPath, CatalogManager catalogManager, String token)
            throws ResourceException {
        // Check if the resource is already being downloaded
        if (isFetchingAll) {
            throw new ResourceException("Resources are already being fetched");
        }

        // Mark fetching as started
        try {
            isFetchingAll = true;
            loadConfiguration();

            // Only installation administrators can fetch all resources
            JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
            catalogManager.getAuthorizationManager().checkIsOpencgaAdministrator(jwtPayload, "fetch all resources");

            // Download resources
            ResourceMetadata metadata = getResourceMetadata();
            for (AnalysisResourceList list : metadata.getAnalysisResourceLists()) {
                fetchResourceFiles(list, tmpPath);
            }

            // Move resources to installation folder
            move(tmpPath, openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME));
        } catch (IOException | NoSuchAlgorithmException | CatalogException e) {
            throw new ResourceException(e);
        } finally {
            // Reset the flag after fetch completes or fails
            isFetchingAll = false;
        }
    }

    public List<File> getResourceFiles(String analysisId) throws ResourceException {
        if (isFetchingAll) {
            throw new ResourceException("Resources are not ready yet; they are currently being fetched");
        }

        // Sanity check
        if (StringUtils.isEmpty(analysisId)) {
            throw new ResourceException("Analysis ID is empty");
        }

        Path resourcePath = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME);
        Path metaPath = resourcePath.resolve(getResourceMetaFilename());
        if (!Files.exists(metaPath)) {
            throw new ResourceException("Resources for analysis '" + analysisId + "' are not ready. Please fetch them first");
        }

        ObjectReader objectReader = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class);
        ResourceMetadata metadata;
        try {
            metadata = objectReader.readValue(metaPath.toFile());
        } catch (IOException e) {
            throw new ResourceException("Error parsing resource metafile '" + metaPath.toAbsolutePath() + "'", e);
        }

        List<File> resourceFiles = new ArrayList<>();
        for (AnalysisResourceList list : metadata.getAnalysisResourceLists()) {
            if (analysisId.equalsIgnoreCase(list.getAnalysisId())) {
                Path analysisResourcePath = resourcePath.resolve(analysisId);
                for (AnalysisResource resource : list.getResources()) {
                    String name = getResourceName(resource);
                    if (!Files.exists(analysisResourcePath.resolve(name))) {
                        throw new ResourceException(RESOURCE_MSG + name + "' for analysis '" + analysisId + "' is missing. Please"
                                + " fetch them first");
                    }
                    resourceFiles.add(analysisResourcePath.resolve(name).toFile());
                }
                break;
            }
        }
        if (CollectionUtils.isEmpty(resourceFiles)) {
            throw new ResourceException("No resources found for analysis ID '" + analysisId + "'");
        }
        return resourceFiles;
    }

    public File getResourceFile(String analysisId, String resourceName) throws ResourceException {
        if (isFetchingAll) {
            throw new ResourceException("Resources are not ready yet; they are currently being fetched");
        }

        // Sanity check
        if (StringUtils.isEmpty(analysisId)) {
            throw new ResourceException("Analysis ID is empty");
        }
        if (StringUtils.isEmpty(resourceName)) {
            throw new ResourceException("Resource name is empty");
        }

        Path resourcePath = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME);
        Path metaPath = resourcePath.resolve(getResourceMetaFilename());
        if (!Files.exists(metaPath)) {
            throw new ResourceException("Resources for analysis '" + analysisId + "' are not ready. Please fetch them first");
        }

        ObjectReader objectReader = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class);
        ResourceMetadata metadata = null;
        try {
            metadata = objectReader.readValue(metaPath.toFile());
        } catch (IOException e) {
            throw new ResourceException("Error parsing resource metafile '" + metaPath.toAbsolutePath() + "'", e);
        }

        for (AnalysisResourceList list : metadata.getAnalysisResourceLists()) {
            if (analysisId.equalsIgnoreCase(list.getAnalysisId())) {
                Path analysisResourcePath = resourcePath.resolve(analysisId);
                for (AnalysisResource resource : list.getResources()) {
                    String name = getResourceName(resource);
                    if (resourceName.equals(name)) {
                        if (!Files.exists(analysisResourcePath.resolve(name))) {
                            throw new ResourceException(RESOURCE_MSG + name + "' for analysis '" + analysisId + "' is missing. Please"
                                    + " fetch them first");
                        }
                        return analysisResourcePath.resolve(name).toFile();
                    }
                }
                throw new ResourceException(RESOURCE_MSG + resourceName + "' for '" + analysisId + "' not found in resource"
                        + " directory");
            }
        }
        throw new ResourceException("Analysis ID '" + analysisId + "' not found in resource directory");
    }

    public static String getVersion() {
        VersionUtils.Version version = new VersionUtils.Version(GitRepositoryState.getInstance().getBuildVersion());
        return String.format("%d.%d.%d", version.getMajor(), version.getMinor(), version.getPatch());
    }

    public static String getResourceMetaFilename() {
        return String.format("release-%s.json", getVersion());
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

    private List<File> fetchResourceFiles(AnalysisResourceList analysisResourceList, Path resourceTmpPath)
            throws IOException, NoSuchAlgorithmException {
        List<File> fetchedFiles = new ArrayList<>();

        String analysisId = analysisResourceList.getAnalysisId();
        Path analysisResourcesPath = resourceTmpPath.resolve(analysisId);
        if (!Files.exists(analysisResourcesPath)) {
            logger.info("Creating directory '{}' for analysis '{}'", analysisResourcesPath, analysisId);
            Files.createDirectories(analysisResourcesPath);
        }
        for (AnalysisResource resource : analysisResourceList.getResources()) {
            Path downloadedPath = fetchFile(baseUrl, analysisId, resource, resourceTmpPath);
            fetchedFiles.add(downloadedPath.toFile());
        }
        return fetchedFiles;
    }

    private Path fetchFile(String baseUrl, String analysisId, AnalysisResource resource, Path downloadedPath)
            throws IOException, NoSuchAlgorithmException {
        return fetchFile(baseUrl, analysisId, getResourceName(resource), resource.getPath(), downloadedPath);
    }

    private Path fetchFile(String baseUrl, String analysisId, String resourceName, String resourcePath, Path downloadedPath)
            throws IOException, NoSuchAlgorithmException {
        String fileUrl = baseUrl + resourcePath;

        Path installationFile;
        Path tmpFile;
        if (StringUtils.isEmpty(analysisId)) {
            installationFile = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME).resolve(resourceName);
            tmpFile = downloadedPath.resolve(resourceName);
        } else {
            installationFile = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME).resolve(analysisId)
                    .resolve(resourceName);
            tmpFile = downloadedPath.resolve(analysisId).resolve(resourceName);
        }

        // Download MD5 for the resource file
        Path md5File = Paths.get(tmpFile.toAbsolutePath() + MD5_EXT);
        donwloadFile(new URL(fileUrl + MD5_EXT), md5File);

        // Check if the file already exists
        if (Files.exists(installationFile)) {
            try {
                validateMD5(installationFile, md5File);
                logger.info("Resource '{}' has already been downloaded and MD5 validation passed: skipping download", resourceName);
                return installationFile;
            } catch (Exception e) {
                logger.warn("Resource '{}' has already been downloaded but MD5 validation failed: downloading again", resourceName);
            }
        }

        // Download resource file
        logger.info("Downloading resource '{}' to '{}' ...", fileUrl, tmpFile.toAbsolutePath());
        donwloadFile(new URL(fileUrl), tmpFile);
        logger.info(OK);

        // Checking MD5
        validateMD5(tmpFile, md5File);

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

    private ResourceMetadata getResourceMetadata() throws IOException, NoSuchAlgorithmException {
        Path resourcePath = Paths.get(String.format("%s/%s", RELEASES_FOLDER_NAME, getResourceMetaFilename()));
        Path analysisResourcesPath = openCgaHome.resolve(ANALYSIS_FOLDER_NAME).resolve(RESOURCES_FOLDER_NAME);
        Path path = fetchFile(baseUrl, null, resourcePath.getFileName().toString(), resourcePath.toString(), analysisResourcesPath);
        ObjectReader objectReader = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class);
        return objectReader.readValue(path.toFile());
    }

    private void move(Path sourceDir, Path targetDir) throws IOException {
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

    private String getResourceName(AnalysisResource resource) {
        return StringUtils.isNotEmpty(resource.getName())
                ? resource.getName()
                : Paths.get(resource.getPath()).getFileName().toString();
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
