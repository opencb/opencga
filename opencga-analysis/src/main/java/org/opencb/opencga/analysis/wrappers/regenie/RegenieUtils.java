package org.opencb.opencga.analysis.wrappers.regenie;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Random;

public final class RegenieUtils {

    public static final String OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY = "OPENCGA_REGENIE_WALKER_DOCKER_IMAGE";

    private static Logger logger = LoggerFactory.getLogger(RegenieUtils.class);

    private RegenieUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static Path checkRegenieInputFile(String fileId, boolean mandatory, String msg, String studyId, CatalogManager catalogManager,
                                             String token) throws ToolException {
        if (StringUtils.isEmpty(fileId)) {
            if (mandatory) {
                throw new ToolException(msg + " file is missing.");
            }
            return null;
        }

        try {
            File opencgaFile = catalogManager.getFileManager().get(studyId, fileId, QueryOptions.empty(), token).first();
            Path path = Paths.get(opencgaFile.getUri().getPath()).toAbsolutePath();
            if (!Files.exists(path)) {
                throw new ToolException(msg + " file does not exit: " + path);
            }
            return path;
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    public static void checkRegenieInputParameter(String value, boolean mandatory, String msg) throws ToolException {
        if (mandatory && StringUtils.isEmpty(value)) {
            throw new ToolException(msg + " is missing.");
        }
    }

    public static String buildAndPushDocker(Path dataDir, String dockerNamespace, String username, String password, Path opencgaHome)
            throws ToolException {
        String dockerRepo = "regenie-walker";
        String dockerRepoVersion = Instant.now().getEpochSecond() + "-" + (new Random().nextInt(9000) + 1000);

        Path dockerBuildScript = opencgaHome.resolve("cloud/docker/custom-tool-docker-build/custom-tool-docker-build.py");
        Command dockerBuild = new Command(new String[]{"python3", dockerBuildScript.toAbsolutePath().toString(),
                "--custom-too-dir", dataDir.toAbsolutePath().toString(),
                "--base-image", "joaquintarraga/opencga-regenie:" + GitRepositoryState.getInstance().getBuildVersion(),
                "--organization", dockerNamespace,
                "--name", dockerRepo,
                "--version", dockerRepoVersion,
                "--username", username,
                "--password", password,
                "build"
        }, Collections.emptyMap());

        logger.info("Executing command: {}", dockerBuild.getCommandLine().replace(password, "XXXXX"));
        dockerBuild.run();
        if (dockerBuild.getExitValue() != 0) {
            throw new ToolException("Error building and pushing the regenie-walker docker image");
        }

        String dockerImage = dockerNamespace + "/" + dockerRepo + ":" + dockerRepoVersion;
        return dockerImage;
    }

    public static void deleteDirectory(java.io.File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }

        // If it's a directory, delete contents recursively
        if (directory.isDirectory()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) { // Not null if directory is not empty
                for (java.io.File file : files) {
                    // Recursively delete subdirectories and files
                    deleteDirectory(file);
                }
            }
        }

        // Finally, delete the directory or file itself
        Files.delete(directory.toPath());
    }
}
