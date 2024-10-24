package org.opencb.opencga.catalog.utils;

import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exceptions.ToolException;
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

import static org.opencb.opencga.core.models.resource.AnalysisResource.AnalysisResourceAction.UNZIP;

public class ResourceManager  {

    public static final String REFERENCE_GENOMES = "reference-genomes";

    public static final String OK = "Ok";
    public static final String MD5_EXT = ".md5";
    public static final String RESOURCE_MSG = "Resource '";
    public static final String FOR_ANALYSIS_MSG = "' for analysis '";

    private Path openCgaHome;
    private Configuration configuration;

    // Flag to track if all resources are fetching
    private boolean isFetchingAll = false;

    public static final String CONFIGURATION_FILENAME = "configuration.yml";
    public static final String CONF_DIRNAME = "conf";
    public static final String ANALYSIS_DIRNAME = "analysis";
    public static final String RESOURCES_DIRNAME = "resources";
    public static final String RELEASES_DIRNAME = "releases";

    protected static Logger logger = LoggerFactory.getLogger(ResourceManager.class);

    public ResourceManager(Path openCgaHome) {
        this.openCgaHome = openCgaHome;
    }


    public synchronized void fetchAllResources(Path tmpPath, CatalogManager catalogManager, String token)
            throws ResourceException {
        // Check if the resource is already being downloaded
        if (isFetchingAll) {
            throw new ResourceException("Resources are already being fetched.");
        }

        // Mark fetching as started
        try {
            isFetchingAll = true;
            loadConfiguration();

            // Only installation administrators can fetch all resources
            JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
            catalogManager.getAuthorizationManager().checkIsOpencgaAdministrator(jwtPayload, "fetch all resources");

            // Download resources
            ResourceMetadata metadata = getResourceMetadata(tmpPath);
            for (AnalysisResourceList list : metadata.getAnalysisResourceLists()) {
                fetchResourceFiles(list, tmpPath);
            }

            // Move resources to installation folder
            move(tmpPath, openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME));
        } catch (IOException | NoSuchAlgorithmException | CatalogException | ToolException e) {
            throw new ResourceException(e);
        } finally {
            // Reset the flag after fetch completes or fails
            isFetchingAll = false;
        }
    }

    public List<File> getResourceFiles(String analysisId) throws ResourceException {
        if (isFetchingAll) {
            throw new ResourceException("Resources are not ready yet; they are currently being fetched.");
        }

        // Sanity check
        if (StringUtils.isEmpty(analysisId)) {
            throw new ResourceException("Analysis ID is empty.");
        }

        Path resourcePath = openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME);
        Path metaPath = resourcePath.resolve(getResourceMetaFilename());
        if (!Files.exists(metaPath)) {
            throw new ResourceException("Resources for analysis '" + analysisId + "' are not ready. Please fetch them first.");
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
                        throw new ResourceException(RESOURCE_MSG + name + FOR_ANALYSIS_MSG + analysisId + "' is missing. Please"
                                + " fetch them first.");
                    }
                    resourceFiles.add(analysisResourcePath.resolve(name).toFile());
                }
                break;
            }
        }
        if (CollectionUtils.isEmpty(resourceFiles)) {
            throw new ResourceException("No resources found for analysis ID '" + analysisId + "'.");
        }
        return resourceFiles;
    }

    public File getResourceFile(String analysisId, String resourceName) throws ResourceException {
        if (isFetchingAll) {
            throw new ResourceException("Resources are not ready yet; they are currently being fetched.");
        }

        // Sanity check
        if (StringUtils.isEmpty(analysisId)) {
            throw new ResourceException("Analysis ID is empty.");
        }
        if (StringUtils.isEmpty(resourceName)) {
            throw new ResourceException("Resource name is empty.");
        }

        Path resourcePath = openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME);
        Path metaPath = resourcePath.resolve(getResourceMetaFilename());
        if (!Files.exists(metaPath)) {
            throw new ResourceException("Resources for analysis '" + analysisId + "' are not ready. Please fetch them first.");
        }

        ObjectReader objectReader = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class);
        ResourceMetadata metadata = null;
        try {
            metadata = objectReader.readValue(metaPath.toFile());
        } catch (IOException e) {
            throw new ResourceException("Error parsing resource metafile '" + metaPath.toAbsolutePath() + "'.", e);
        }

        for (AnalysisResourceList list : metadata.getAnalysisResourceLists()) {
            if (analysisId.equalsIgnoreCase(list.getAnalysisId())) {
                Path analysisResourcePath = resourcePath.resolve(analysisId);
                for (AnalysisResource resource : list.getResources()) {
                    String name = getResourceName(resource);
                    if (resourceName.equals(name)) {
                        if (!Files.exists(analysisResourcePath.resolve(name))) {
                            throw new ResourceException(RESOURCE_MSG + name + FOR_ANALYSIS_MSG + analysisId + "' is missing. Please"
                                    + " fetch them first.");
                        }
                        return analysisResourcePath.resolve(name).toFile();
                    }
                }
                throw new ResourceException(RESOURCE_MSG + resourceName + "' for '" + analysisId + "' not found in resource"
                        + " directory.");
            }
        }
        throw new ResourceException("Analysis ID '" + analysisId + "' not found in resource directory.");
    }

    public Path checkResourcePath(String analysisId, String resourceName) throws ResourceException {
        Path resourcePath = Paths.get(openCgaHome.toAbsolutePath().toString(), ANALYSIS_DIRNAME, RESOURCES_DIRNAME, analysisId,
                resourceName);
        if (!Files.exists(resourcePath)) {
            throw new ResourceException(RESOURCE_MSG + resourceName + FOR_ANALYSIS_MSG + analysisId + "' is missing. Please fetch"
                    + " them first.");
        }
        return resourcePath;
    }

    public static String getResourceMetaFilename() {
        return getResourceMetaFilename(GitRepositoryState.getInstance().getBuildVersion());
    }

    public static String getResourceMetaFilename(String version) {
        return String.format("release-%s.json", version);
    }

    //-------------------------------------------------------------------------
    //  P R I V A T E      M E T H O D S
    //-------------------------------------------------------------------------

    private void loadConfiguration() throws IOException {
        if (configuration == null) {
            this.configuration = Configuration.load(new FileInputStream(openCgaHome.resolve(CONF_DIRNAME)
                    .resolve(CONFIGURATION_FILENAME).toFile()));
        }
    }

    private List<File> fetchResourceFiles(AnalysisResourceList analysisResourceList, Path resourceTmpPath)
            throws IOException, NoSuchAlgorithmException, ResourceException, ToolException {
        List<File> fetchedFiles = new ArrayList<>();

        String analysisId = analysisResourceList.getAnalysisId();
        Path analysisResourcesPath = resourceTmpPath.resolve(analysisId);
        if (!Files.exists(analysisResourcesPath)) {
            logger.info("Creating directory '{}' for analysis '{}'", analysisResourcesPath, analysisId);
            Files.createDirectories(analysisResourcesPath);
        }
        for (AnalysisResource resource : analysisResourceList.getResources()) {
            Path downloadedPath = fetchFile(analysisId, resource, resourceTmpPath);
            fetchedFiles.add(downloadedPath.toFile());
        }
        return fetchedFiles;
    }

    private Path fetchFile(String analysisId, AnalysisResource resource, Path downloadedPath)
            throws IOException, NoSuchAlgorithmException, ResourceException, ToolException {
        String resourceName = getResourceName(resource);

        // First check installation directory, and check MD5 (it exists)
        Path installationFile = openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME).resolve(analysisId)
                .resolve(resourceName);
        if (Files.exists(installationFile)) {
            try {
                validateMD5(installationFile, resource.getMd5());
                logger.info("Resource '{}' has already been downloaded and MD5 validation passed: skipping download", resourceName);
                return installationFile;
            } catch (Exception e) {
                logger.warn("Resource '{}' has already been downloaded but MD5 validation failed: downloading again", resourceName);
            }
        }

        // Download resource file
        String fileUrl = resource.getUrl();
        Path fetchedFile = downloadedPath.resolve(analysisId).resolve(resourceName);
        logger.info("Downloading resource '{}' to '{}' ...", fileUrl, fetchedFile.toAbsolutePath());
        donwloadFile(new URL(fileUrl), fetchedFile);
        logger.info(OK);

        // Checking MD5
        validateMD5(fetchedFile, resource.getMd5());

        // Any action to perform ?
        if (CollectionUtils.isNotEmpty(resource.getAction())) {
            for (AnalysisResource.AnalysisResourceAction action : resource.getAction()) {
                if (UNZIP == action) {
                    unzip(fetchedFile, analysisId);
                } else {
                    throw new ResourceException("Unknown action '" + action + "'.");
                }
            }
        }
        return fetchedFile;
    }

    private Path donwloadFile(URL url, Path resourcePath) throws IOException {
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

        return resourcePath;
    }

    public void validateMD5(Path filePath, Path md5filePath) throws IOException, NoSuchAlgorithmException, InputMismatchException {
        validateMD5(filePath, new String(Files.readAllBytes(md5filePath)));
    }

    public void validateMD5(Path filePath, String md5) throws IOException, NoSuchAlgorithmException, InputMismatchException {
        String expectedMD5 = md5.trim();
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

    private ResourceMetadata getResourceMetadata(Path downloadPath) throws IOException, NoSuchAlgorithmException {
        String resourceMetaFilename = getResourceMetaFilename();
        Path resourceUrlPath = Paths.get(String.format("%s/%s", RELEASES_DIRNAME, resourceMetaFilename));

        String resourceUrl = configuration.getAnalysis().getResourceUrl();

        // Download MD5
        Path md5ResourceMetaPath = downloadPath.resolve(resourceMetaFilename + MD5_EXT);
        donwloadFile(new URL(resourceUrl + MD5_EXT), md5ResourceMetaPath);

        // Download resource metadata file
        Path resourceMetaPath = downloadPath.resolve(resourceMetaFilename);
        donwloadFile(new URL(resourceUrl), resourceMetaPath);

        // Checking MD5
        validateMD5(resourceMetaPath, md5ResourceMetaPath);

        ObjectReader objectReader = JacksonUtils.getDefaultObjectMapper().readerFor(ResourceMetadata.class);
        return objectReader.readValue(resourceMetaPath.toFile());
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
                : Paths.get(resource.getUrl()).getFileName().toString();
    }

    private void unzip(Path zipPath, String analysisId) throws ToolException, IOException {
        // Unzip
        String filename = zipPath.getFileName().toString();
        Path stdoutPath = zipPath.getParent().resolve("stdout_unzip_" + filename + ".txt");
        Path stderrPath = zipPath.getParent().resolve("stderr_unzip_" + filename + ".txt");
        try {
            logger.info("Unzipping resource file '{}' for analysis '{}'.", filename, analysisId);
            new Command("unzip -o -d " + zipPath.getParent() + " " + zipPath)
                    .setOutputOutputStream(new DataOutputStream(new FileOutputStream(stdoutPath.toFile())))
                    .setErrorOutputStream(new DataOutputStream(new FileOutputStream(stderrPath.toFile())))
                    .run();
        } catch (FileNotFoundException e) {
            throw new ToolException("Error unzipping resource file '" + filename + FOR_ANALYSIS_MSG + analysisId + "'. Check log files: "
                    + stdoutPath + ", " + stderrPath, e);
        }
        Files.delete(stdoutPath);
        Files.delete(stderrPath);
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
