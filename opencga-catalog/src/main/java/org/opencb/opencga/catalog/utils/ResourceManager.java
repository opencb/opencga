package org.opencb.opencga.catalog.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.AnalysisTool;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.Resource;
import org.opencb.opencga.core.config.ResourceFile;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.JwtPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ResourceManager  {

    public static final String REFERENCE_GENOMES = "reference-genomes";

    public static final String OK = "Ok";
    public static final String MD5_EXT = ".md5";
    public static final String RESOURCE_MSG = "Resource '";
    public static final String FOR_ANALYSIS_MSG = "' for analysis '";

    private Path openCgaHome;
    private Configuration configuration;

    // Flag to track if all resources are fetching
    private static final AtomicBoolean IS_FETCHING = new AtomicBoolean(false);

    public static final String CONFIGURATION_FILENAME = "configuration.yml";
    public static final String CONF_DIRNAME = "conf";
    public static final String ANALYSIS_DIRNAME = "analysis";
    public static final String RESOURCES_DIRNAME = "resources";
//    public static final String RELEASES_DIRNAME = "releases";

    public static final String STDOUT_UNZIP_PREFIX = "stdout_unzip_";
    public static final String STDERR_UNZIP_PREFIX = "stderr_unzip_";

    public static final Set<String> SKIPPED_PREFIXES = new HashSet<>(Arrays.asList(STDERR_UNZIP_PREFIX, STDOUT_UNZIP_PREFIX));

    protected static Logger logger = LoggerFactory.getLogger(ResourceManager.class);

    public ResourceManager(Path openCgaHome) {
        this.openCgaHome = openCgaHome;
    }

    public synchronized void fetchAllResources(Path tmpPath, CatalogManager catalogManager, String token)
            throws ResourceException, IOException {
        loadConfiguration();

        // Get all resources from the configuration file
        List<String> resources = configuration.getAnalysis().getResource().getFiles().stream().map(ResourceFile::getId)
                .collect(Collectors.toList());
        fetchResources(resources, tmpPath, catalogManager, token);
    }

    public synchronized void fetchResources(List<String> resources, Path tmpPath, CatalogManager catalogManager, String token)
            throws ResourceException, IOException {
        loadConfiguration();

        // Check if the resource is already being downloaded
        if (IS_FETCHING.compareAndSet(false, true)) {
            try {
                // Only installation administrators can fetch all resources
                JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
                catalogManager.getAuthorizationManager().checkIsOpencgaAdministrator(jwtPayload, "fetch all resources");

                // Check resources before fetching
                List<ResourceFile> resourceFiles = new ArrayList<>();
                for (String resource : resources) {
                    boolean found = false;
                    for (ResourceFile resourceFile : configuration.getAnalysis().getResource().getFiles()) {
                        if (resource.equalsIgnoreCase(resourceFile.getId())) {
                            resourceFiles.add(resourceFile);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new ResourceException("Unknown resource ID '" + resource + "'. Check the configuration file.");
                    }
                }

                // Download resources
                for (ResourceFile resourceFile: resourceFiles) {
                    fetchResourceFile(resourceFile, tmpPath);
                }

                // Move resources to installation folder
                move(tmpPath, openCgaHome.resolve(ANALYSIS_DIRNAME).resolve(RESOURCES_DIRNAME));
            } catch (CatalogException | NoSuchAlgorithmException | ToolException e) {
                throw new ResourceException(e);
            } finally {
                // Ensure the flag is reset after fetching is done
                IS_FETCHING.set(false);
            }
        } else {
            throw new ResourceException("Resources are already being fetched.");
        }
    }

    public List<File> checkResourcePaths(String analysisId) throws ResourceException, ToolException, IOException {
        return checkResourcePaths(analysisId, null);
    }

    public List<File> checkResourcePaths(String analysisId, String version) throws ResourceException, ToolException, IOException {
        loadConfiguration();

        if (IS_FETCHING.get()) {
            throw new ResourceException("Resources are not ready yet; they are currently being fetched.");
        }

        // Sanity check
        if (StringUtils.isEmpty(analysisId)) {
            throw new ResourceException("Analysis ID is empty.");
        }

        List<File> files = new ArrayList<>();
        Resource resourceConfig = configuration.getAnalysis().getResource();

        AnalysisTool analysisTool = null;
        for (AnalysisTool tool : configuration.getAnalysis().getTools()) {
            if (analysisId.equals(tool.getId())) {
                if (StringUtils.isEmpty(version) || version.equals(tool.getVersion())) {
                    analysisTool = tool;
                    break;
                }
            }
        }
        if (analysisTool == null) {
            String msg = "Missing analysis tool (ID = " + analysisId + (!StringUtils.isEmpty(version) ? (", version = " + version) : "")
                    + ") in the configuration file";
            throw new ToolException(msg);
        }

        for (String resourceId : analysisTool.getResourceIds()) {
            boolean found = false;
            for (ResourceFile resourceFile : resourceConfig.getFiles()) {
                if (resourceId.equalsIgnoreCase(resourceFile.getId())) {
                    // Found
                    Path analysisResourcePath = resourceConfig.getBasePath().resolve(resourceFile.getPath());
                    if (!Files.exists(analysisResourcePath)) {
                        throw new ResourceException(RESOURCE_MSG + resourceFile.getId() + "' is not fetched yet (file '"
                                + analysisResourcePath + "' is missing). Please fetch it first.");
                    }
                    files.add(analysisResourcePath.toFile());
                    found = true;
                    break;
                }
            }
            // Sanity check
            if (!found) {
                throw new ResourceException("Unmatched configuration: resource '" + resourceId + "' of analysis '" + analysisId + "' not"
                        + " found in the configuration file.");
            }
        }

        if (CollectionUtils.isEmpty(files)) {
            throw new ResourceException("No resources found for analysis ID '" + analysisId + "'.");
        }
        return files;
    }

    public Path checkResourcePath(String resourceId) throws ResourceException, IOException {
        loadConfiguration();

        if (IS_FETCHING.get()) {
            throw new ResourceException("Resources are not ready yet; they are currently being fetched.");
        }

        // Sanity check
        if (StringUtils.isEmpty(resourceId)) {
            throw new ResourceException("Resource ID is empty.");
        }

        Resource resourceConfig = configuration.getAnalysis().getResource();
        for (ResourceFile resourceFile : resourceConfig.getFiles()) {
            if (resourceId.equalsIgnoreCase(resourceFile.getId())) {
                // Resource found, exists?
                Path analysisResourcePath = resourceConfig.getBasePath().resolve(resourceFile.getPath());
                if (!Files.exists(analysisResourcePath)) {
                    throw new ResourceException(RESOURCE_MSG + resourceFile.getId() + "' is not fetched yet (file '" + analysisResourcePath
                            + "' is missing). Please fetch it first.");
                }
                return analysisResourcePath;
            }
        }

        // Resource not found !!
        throw new ResourceException("Unmatched configuration: resource '" + resourceId + "' not found in the configuration file.");
    }

    //-------------------------------------------------------------------------
    //  P R I V A T E      M E T H O D S
    //-------------------------------------------------------------------------

    private void loadConfiguration() throws IOException {
        if (configuration == null) {
            try (InputStream is = new FileInputStream(openCgaHome.resolve(CONF_DIRNAME).resolve(CONFIGURATION_FILENAME).toFile())) {
                this.configuration = Configuration.load(is);
            }
        }
    }

    private Path fetchResourceFile(ResourceFile resourceFile, Path downloadedPath)
            throws IOException, NoSuchAlgorithmException, ResourceException, ToolException {
        Resource resourceConfig = configuration.getAnalysis().getResource();


        boolean isExomiserResource = false;
        Path fetchedFile = downloadedPath.resolve(resourceFile.getPath());
        if (resourceFile.getId().startsWith("EXOMISER_")) {
            isExomiserResource = true;
            fetchedFile = downloadedPath.resolve(resourceFile.getPath() + ".zip");
        }

        // First check installation directory, and check MD5 (if it exists)
        Path installationFile = resourceConfig.getBasePath().resolve(resourceFile.getPath());
        if (Files.exists(installationFile)) {
            if (isExomiserResource && Files.isDirectory(installationFile)) {
                logger.info("Resource '{}' has already been downloaded: skipping download", resourceFile.getId());
                return installationFile;
            } else {
                try {
                    validateMD5(installationFile, resourceFile.getMd5());
                    logger.info("Resource '{}' has already been downloaded and MD5 validation passed: skipping download",
                            resourceFile.getId());
                    return installationFile;
                } catch (Exception e) {
                    logger.warn("Resource '{}' has already been downloaded but MD5 validation failed: downloading again",
                            resourceFile.getId());
                }
            }
        }

        // Download resource file
        String fileUrl = resourceConfig.getBaseUrl() + resourceFile.getUrl();
        logger.info("Downloading resource '{}' to '{}' ...", fileUrl, fetchedFile.toAbsolutePath());
        donwloadFile(new URL(fileUrl), fetchedFile);
        logger.info(OK);

        // Checking MD5
        validateMD5(fetchedFile, resourceFile.getMd5());

        // Any action to perform, e.g.: Exomiser resources need to be unzipped
        if (isExomiserResource) {
            unzip(fetchedFile, "exomiser");
            // Delete Exomiser files .zip, .sha256
            List<String> exts = Arrays.asList(".zip", ".sha256");
            for (String ext : exts) {
                if (Files.exists(downloadedPath.resolve(resourceFile.getPath() + ext))) {
                    logger.info("Deleting Exomiser file {} after unzipping", downloadedPath.resolve(resourceFile.getPath() + ext));
                    Files.delete(downloadedPath.resolve(resourceFile.getPath() + ext));
                }
            }
            return downloadedPath.resolve(resourceFile.getPath());
        }
        return fetchedFile;
    }

    private Path donwloadFile(URL url, Path resourcePath) throws ResourceException, IOException {
        Path parentPath = resourcePath.getParent();
        if (!Files.exists(parentPath)) {
            Files.createDirectories(parentPath);
        }

        try (BufferedInputStream in = new BufferedInputStream(url.openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(resourcePath.toFile())) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (Exception e) {
            throw new ResourceException("Error downloading " + url, e);
        }

        if (!Files.exists(resourcePath)) {
            String msg = "Something wrong happened downloading '" + url + "'; it could not be found after downloading";
            logger.error(msg);
            throw new ResourceException(msg);
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

    public static void move(Path source, Path destination) throws IOException {
        // Ensure the target directory exists
        if (!Files.exists(destination)) {
            logger.info("Creating directory {} ...", destination.toAbsolutePath());
            Files.createDirectories(destination);
            logger.info(OK);
        }

        File[] files = source.toFile().listFiles();
        if (files == null) {
            return;
        }

        for (File file : files) {
            File destFile = new File(destination.toFile(), file.getName());
            if (file.isDirectory()) {
                move(file.toPath(), destFile.toPath());
            } else {
                if (Files.exists(file.toPath())) {
                    if (!SKIPPED_PREFIXES.stream().anyMatch(file.getName()::startsWith)) {
                        logger.info("Copying {} to {} ...", file.getAbsolutePath(), destFile.getAbsolutePath());
                        FileUtils.copyFile(file, destFile);
                        logger.info(OK);
                    }
                }
            }
        }

        // Delete after copying
        for (File file : files) {
            if (file.isDirectory()) {
                deleteFolder(file);
            } else {
                try {
                    logger.info("Deleting file {} ...", file.getAbsolutePath());
                    Files.delete(file.toPath());
                    logger.info(OK);
                } catch (IOException e) {
                    logger.warn("Could not delete the file '" + file.getAbsolutePath() + "'", e);
                }
            }
        }
    }

    private static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteFolder(file);
                } else {
                    try {
                        logger.info("Deleting file {} ...", file.getAbsolutePath());
                        Files.delete(file.toPath());
                        logger.info(OK);
                    } catch (IOException e) {
                        logger.warn("Could not delete the file '" + file.getAbsolutePath() + "'", e);
                    }
                }
            }
        }

        // Delete the original directory after moving all its content
        logger.info("Deleting directory {} ...", folder.getAbsolutePath());
        try {
            Files.delete(folder.toPath());
            logger.info(OK);
        } catch (IOException e) {
            logger.warn("Could not delete the directory '" + folder.getAbsolutePath() + "'", e);
        }
    }

    private void unzip(Path zipPath, String analysisId) throws ToolException, IOException {
        // Unzip
        String filename = zipPath.getFileName().toString();
        Path stdoutPath = zipPath.getParent().resolve(STDOUT_UNZIP_PREFIX + filename + ".txt");
        Path stderrPath = zipPath.getParent().resolve(STDERR_UNZIP_PREFIX + filename + ".txt");
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

        // Deleting stdout and stderr after unzipping
        if (Files.exists(stdoutPath)) {
            try {
                logger.info("Deleting the stdout log file: {}", stdoutPath);
                Files.delete(stdoutPath);
                logger.info(OK);
            } catch (Exception e) {
                logger.warn("Could not delete the stdout log file: " + stdoutPath, e);
            }
        }
        if (Files.exists(stderrPath)) {
            try {
                logger.info("Deleting the stderr log file: {}", stderrPath);
                Files.delete(stderrPath);
                logger.info(OK);
            } catch (Exception e) {
                logger.warn("Could not delete the stderr log file: " + stderrPath, e);
            }
        }
    }

    //-------------------------------------------------------------------------
    //  G E T T E R S     &      S E T T E R S
    //-------------------------------------------------------------------------

    public Configuration getConfiguration() {
        return configuration;
    }
}
