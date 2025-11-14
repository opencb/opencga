package org.opencb.opencga.analysis.tools;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.InputFileUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.ToolInfoExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public abstract class OpenCgaDockerToolScopeStudy extends OpenCgaTool {

    // Build list of inputfiles in case we need to specifically mount them in read only mode
    protected List<AbstractMap.SimpleEntry<String, String>> dockerInputBindings;
    // Directory where temporal input files will be stored
    protected Path temporalInputDir;

    protected InputFileUtils inputFileUtils;

    @Override
    protected void check() throws Exception {
        super.check();
        this.dockerInputBindings = new LinkedList<>();
        this.temporalInputDir = Files.createDirectory(getScratchDir().resolve(".opencga_input"));
        this.inputFileUtils = new InputFileUtils(catalogManager);
    }

    @Override
    protected void close() {
        super.close();
    }

    protected String processInputValue(String value, InputFileUtils inputFileUtils) throws CatalogException {
        if (inputFileUtils.isValidOpenCGAFile(value)) {
            File file = inputFileUtils.findOpenCGAFileFromPattern(study, value, token);
            if (inputFileUtils.fileMayContainReferencesToOtherFiles(file)) {
                Path outputFile = temporalInputDir.resolve(file.getName());
                List<File> files = inputFileUtils.findAndReplaceFilePathToUrisFromFile(study, file, outputFile, token);

                // Write outputFile as inputBinding
                dockerInputBindings.add(new AbstractMap.SimpleEntry<>(outputFile.toString(), outputFile.toString()));
                logger.info("Params: OpenCGA input file: '{}'", outputFile);

                // Add files to inputBindings to ensure they are also mounted (if any)
                for (File tmpFile : files) {
                    dockerInputBindings.add(new AbstractMap.SimpleEntry<>(tmpFile.getUri().getPath(), tmpFile.getUri().getPath()));
                    logger.info("Inner files from '{}': OpenCGA input file: '{}'", outputFile, tmpFile.getUri().getPath());
                }

                return outputFile.toString();
            } else {
                String path = file.getUri().getPath();
                dockerInputBindings.add(new AbstractMap.SimpleEntry<>(path, path));
                logger.info("Params: OpenCGA input file: '{}'", path);

                return path;
            }
        } else {
            return value;
        }

    }

    protected String processOutputValue(String value, InputFileUtils inputFileUtils) throws CatalogException {
        String dynamicOutputFolder;
        if (inputFileUtils.isDynamicOutputFolder(value)) {
            // If it starts with $OUTPUT/...
            dynamicOutputFolder = inputFileUtils.getDynamicOutputFolder(value, getOutDir().toAbsolutePath().toString());
        } else {
            // If it starts directly with the subpath...
            dynamicOutputFolder = inputFileUtils.appendSubpath(getOutDir().toAbsolutePath().toString(), value);
        }
        logger.info("Params: Dynamic output folder: '{}'", dynamicOutputFolder);
        return dynamicOutputFolder;
    }

    /**
     * Given a variable name, it removes the prefix '-' if present.
     * Example: --input -> input; -input -> input; input -> input
     *
     * @param variable A parameter of a command line.
     * @return the variable removing any '-' prefix.
     */
    protected String removePrefix(String variable) {
        String value = variable;
        while (value.startsWith("-")) {
            value = value.substring(1);
        }
        return value;
    }

    protected void updateJobInformation(List<String> tags, ToolInfoExecutor executor) throws CatalogException {
        if (getJobId() == null || getJobId().isEmpty()) {
            // Job id might be empty if the tool is not executed in the context of a job (e.g. in a test)
            logger.warn("Job id is empty. Cannot update job information with tags {} and executor {}", tags, executor);
            return;
        }
        ObjectMap params = new ObjectMap()
                .append(JobDBAdaptor.QueryParams.TAGS.key(), tags)
                .append(JobDBAdaptor.QueryParams.TOOL_EXTERNAL_EXECUTOR.key(), executor);
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(JobDBAdaptor.QueryParams.TAGS.key(), ParamUtils.BasicUpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        catalogManager.getJobManager().update(getStudyFqn(), getJobId(), params, options, token);
    }

    protected String runDocker(String image, List<AbstractMap.SimpleEntry<String, String>> userOutputBindings, String cmdParams,
                               Map<String, String> userDockerParams) throws IOException {
        return runDocker(image, userOutputBindings, cmdParams, userDockerParams, null, null, null);
    }

    protected String runDocker(String image, List<AbstractMap.SimpleEntry<String, String>> userOutputBindings, String cmdParams,
                               Map<String, String> userDockerParams, String registry, String username, String password) throws IOException {
        List<AbstractMap.SimpleEntry<String, String>> outputBindings = CollectionUtils.isNotEmpty(userOutputBindings)
                ? userOutputBindings
                : Collections.singletonList(new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(), getOutDir().toAbsolutePath().toString()));

        Map<String, String> dockerParams = new HashMap<>();
        // Establish working directory
        dockerParams.put("-w", getOutDir().toAbsolutePath().toString());
        dockerParams.put("--volume", "/var/run/docker.sock:/var/run/docker.sock");
        dockerParams.put("--env", "DOCKER_HOST='tcp://localhost:2375'");
        dockerParams.put("--network", "host");
        if (userDockerParams != null) {
            dockerParams.putAll(userDockerParams);
        }

        return DockerUtils.run(image, dockerInputBindings, outputBindings, cmdParams, dockerParams, registry, username, password);
    }

}
