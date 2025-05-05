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
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Random;

public final class RegenieUtils {

    public static final String VCF_BASENAME = "input";
    public static final String VCF_FILENAME = VCF_BASENAME + ".vcf.gz";
    public static final String PHENO_FILENAME = "phenoFile.txt";
    public static final String COVAR_FILENAME = "covarFile.txt";
    public static final String STEP1_PRED_LIST_FILNEMANE = "step1_pred.list";
    public static final String OPT_APP_VIRTUAL_DIR = "/opt/app/";
    public static final String PREDICTION_PATH = "pred/";
    public static final String OPT_APP_PRED_VIRTUAL_DIR = OPT_APP_VIRTUAL_DIR + PREDICTION_PATH;

    public static final String REGENIE_RESULTS_FILENAME = "regenie_results.txt";

    public static final String OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY = "OPENCGA_REGENIE_WALKER_DOCKER_IMAGE";

    private static Random random = new Random();

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

    public static String checkRegenieInputParameter(String value, boolean mandatory, String msg) throws ToolException {
        if (mandatory && StringUtils.isEmpty(value)) {
            throw new ToolException(msg + " is missing.");
        }
        return value;
    }

    public static Path createDockerfile(Path dataDir, Path opencgaHome)
            throws ToolException {
        Path dockerBuildScript = opencgaHome.resolve("analysis/resources/walker/custom-tool-docker-build.py");
        Command dockerBuild = new Command(new String[]{"python3", dockerBuildScript.toAbsolutePath().toString(),
                "--custom-tool-dir", dataDir.toAbsolutePath().toString(),
                "--base-image", "joaquintarraga/opencga-regenie:" + GitRepositoryState.getInstance().getBuildVersion(),
                "dockerfile"
        }, Collections.emptyMap());

        logger.info("Executing command: {}", dockerBuild.getCommandLine());
        dockerBuild.run();
        if (dockerBuild.getExitValue() != 0) {
            throw new ToolException("Error creating regenie-walker Dockerfile");
        }

        Path dockerfile = dataDir.resolve("Dockerfile");
        if (!Files.exists(dockerfile)) {
            throw new ToolException("Dockerfile for regenie-walker not found: " + dockerfile);
        }
        return dockerfile;
    }

    public static String buildAndPushDocker(Path dataDir, String dockerName, String dockerTag, String dockerUsername, String dockerPassword,
                                            Path opencgaHome) throws ToolException {
        // Sanity check
        if (StringUtils.isEmpty(dockerName)) {
            throw new ToolException("Missing docker name");
        }
        if (!dockerName.contains("/")) {
            throw new ToolException("Invalid docker name " + dockerName + ", please, provide: namespace/repository");
        }
        if (StringUtils.isEmpty(dockerUsername)) {
            throw new ToolException("Missing docker username");
        }
        if (StringUtils.isEmpty(dockerPassword)) {
            throw new ToolException("Missing docker password");
        }

        String[] split = dockerName.split("/");
        String organisation = split[0];
        String name = split[1];

        String dockerRepoVersion = dockerTag;
        if (StringUtils.isEmpty(dockerRepoVersion)) {
            dockerRepoVersion = Instant.now().getEpochSecond() + "-" + (random.nextInt(9000) + 1000);
        }

        Path dockerBuildScript = opencgaHome.resolve("analysis/resources/walker/custom-tool-docker-build.py");
        Command dockerBuild = new Command(new String[]{"python3", dockerBuildScript.toAbsolutePath().toString(),
                "--custom-tool-dir", dataDir.toAbsolutePath().toString(),
                "--base-image", "joaquintarraga/opencga-regenie:" + GitRepositoryState.getInstance().getBuildVersion(),
                "--organisation", organisation,
                "--name", name,
                "--version", dockerRepoVersion,
                "--username", dockerUsername,
                "--password", dockerPassword,
                "push"
        }, Collections.emptyMap());

        logger.info("Executing command: {}", dockerBuild.getCommandLine());
        dockerBuild.run();
        if (dockerBuild.getExitValue() != 0) {
            throw new ToolException("Error building and pushing the regenie-walker docker image");
        }

        return dockerName + ":" + dockerRepoVersion;
    }

    public static boolean isDockerImageAvailable(String inputDockerImage, String dockerUsername, String dockerPassword) {
        String namespace;
        String imageName;
        String tagName;

        if (inputDockerImage.contains("/")) {
            String[] parts = inputDockerImage.split("/");
            namespace = parts[0];
            imageName = parts[1];
        } else {
            String msg = "Missing namespace in docker image " + inputDockerImage + ", please, provide: namespace/repository:tag";
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }

        if (imageName.contains(":")) {
            String[] parts = imageName.split(":");
            imageName = parts[0];
            tagName = parts[1];
        } else {
            // If no tag is provided, we can just check the repository exists
            String msg = "Missing tag in docker image " + inputDockerImage + ", please, provide: namespace/repository:tag";
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }

        String apiUrl = String.format("https://hub.docker.com/v2/repositories/%s/%s/tags/%s/", namespace, imageName, tagName);
        logger.info("Checking docker image {} at Docker Hub: {}", inputDockerImage, apiUrl);

        try {
            URL url = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            connection.disconnect();

            return responseCode == HttpURLConnection.HTTP_OK;
        } catch (IOException e) {
            logger.error("Error when accessing Docker Hub {}: {}", apiUrl, e.getMessage());
            return false;
        }
    }
}
