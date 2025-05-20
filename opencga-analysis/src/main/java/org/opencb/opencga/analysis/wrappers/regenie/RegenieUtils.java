package org.opencb.opencga.analysis.wrappers.regenie;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    // Regenie files options
    public static final String BGEN_OPTION = "--bgen";
    public static final String BED_OPTION = "--bed";
    public static final String PGEN_OPTION = "--pgen";
    public static final String SAMPLE_OPTION = "--sample";
    public static final String BGI_OPTION = "--bgi";
    public static final String KEEP_OPTION = "--keep";
    public static final String REMOVE_OPTION = "--remove";
    public static final String EXTRACT_OPTION = "--extract";
    public static final String EXCLUDE_OPTION = "--exclude";
    public static final String EXTRACT_OR_OPTION = "--extract-or";
    public static final String EXCLUDE_OR_OPTION = "--exclude-or";
    public static final String PHENO_FILE_OPTION = "--phenoFile";
    public static final String COVAR_FILE_OPTION = "--covarFile";
    public static final String PRED_OPTION = "--pred";
    public static final String TPHENO_FILE_OPTION = "--tpheno-file";
    public static final String SRUN_L0_OPTION = "--run-l0"; // FILE,K
    public static final String USER_NULL_FIRTH_OPTION = "--use-null-firth";
    public static final String ANNO_FILE_OPTION = "--anno-file";
    public static final String SET_LIST_OPTION = "--set-list";
    public static final String EXTRACT_SETS_OPTION = "--extract-sets";
    public static final String EXCLUDE_SETS_OPTION = "--exclude-sets";
    public static final String AAF_FILE_OPTION = "--aaf-file";
    public static final String MASK_DEF_OPTION = "--mask-def";
    public static final String LOVO_SNPLIST_OPTION = "--lovo-snplist";
    public static final String INTERACTION_FILE_OPTION = "--interaction-file"; // FORMAT,FILE
    public static final String INTERACTION_FILE_SAMPLE_OPTION = "--interaction-file-sample";
    public static final String CONDITION_LIST_OPTION = "--condition-list";
    public static final String CONDITION_FILE_OPTION = "--condition-file"; // FORMAT,FILE
    public static final String CONDITION_FILE_SAMPLE_OPTION = "--condition-file-sample";
    public static final String LD_EXTRACT_OPTION = "--ld-extract";

    public static final List<String> ALL_FILE_OPTIONS = Arrays.asList(BGEN_OPTION, BED_OPTION, PGEN_OPTION, SAMPLE_OPTION, BGI_OPTION,
            KEEP_OPTION, REMOVE_OPTION, EXTRACT_OPTION, EXCLUDE_OPTION, EXTRACT_OR_OPTION, EXCLUDE_OR_OPTION, PHENO_FILE_OPTION,
            COVAR_FILE_OPTION, PRED_OPTION, TPHENO_FILE_OPTION, SRUN_L0_OPTION, USER_NULL_FIRTH_OPTION, ANNO_FILE_OPTION, SET_LIST_OPTION,
            EXTRACT_SETS_OPTION, EXCLUDE_SETS_OPTION, AAF_FILE_OPTION, MASK_DEF_OPTION, LOVO_SNPLIST_OPTION, INTERACTION_FILE_OPTION,
            INTERACTION_FILE_SAMPLE_OPTION, CONDITION_LIST_OPTION, CONDITION_FILE_OPTION, CONDITION_FILE_SAMPLE_OPTION,
            CONDITION_FILE_SAMPLE_OPTION, LD_EXTRACT_OPTION);

    public static final List<String> SKIP_OPTIONS = Arrays.asList("--out", "--step");

    public static final String FILE_PREFIX = "file:/";

    public static final int MINIMUN_NUM_SAMPLES = 2;

    private static Random random = new Random();

    private static Logger logger = LoggerFactory.getLogger(RegenieUtils.class);

    private RegenieUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static Path checkRegenieInputFile(String entryStr, boolean mandatory, String msg, String studyId, CatalogManager catalogManager,
                                             String token) throws ToolException {
        String fileId = entryStr;
        if (StringUtils.isEmpty(fileId)) {
            if (mandatory) {
                throw new ToolException(msg + " file is missing.");
            }
            return null;
        }

        if (fileId.startsWith(FILE_PREFIX)) {
            fileId = fileId.substring(FILE_PREFIX.length());
        }


        try {
            OpenCGAResult<File> fileResults = catalogManager.getFileManager().get(studyId, fileId, QueryOptions.empty(), token);
            if (fileResults.getNumResults() == 0) {
                throw new ToolException("File " + entryStr + " not found in OpenCGA catalog");
            }
            if (fileResults.getNumResults() > 1) {
                throw new ToolException("More than one file found for " + entryStr + " in OpenCGA catalog");
            }
            File opencgaFile = fileResults.first();
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

    public static Path createDockerfile(Path dataDir, String dockerBasename, Path opencgaHome)
            throws ToolException {
        Path dockerBuildScript = opencgaHome.resolve("analysis/resources/walker/custom-tool-docker-build.py");
        Command dockerBuild = new Command(new String[]{"python3", dockerBuildScript.toAbsolutePath().toString(),
                "--custom-tool-dir", dataDir.toAbsolutePath().toString(),
                "--base-image", dockerBasename,
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

    public static String buildAndPushDocker(Path dataDir, String dockerBasename, String dockerName, String dockerTag, String dockerUsername, String dockerPassword,
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

        Path dockerBuildScript = opencgaHome.resolve("analysis/resources/common/tool-docker-builder.py");
        Command dockerBuild = new Command(new String[]{"python3", dockerBuildScript.toAbsolutePath().toString(),
                "--custom-tool-dir", dataDir.toAbsolutePath().toString(),
                "--base-image", dockerBasename,
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
