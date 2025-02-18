package org.opencb.opencga.analysis.tools;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.InputFileUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.ToolInfoExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

public abstract class OpenCgaDockerToolScopeStudy extends OpenCgaToolScopeStudy {

    // Build list of inputfiles in case we need to specifically mount them in read only mode
    protected List<AbstractMap.SimpleEntry<String, String>> dockerInputBindings;
    // Directory where temporal input files will be stored
    protected Path temporalInputDir;

    protected InputFileUtils inputFileUtils;

    @Override
    protected void check() throws Exception {
        super.check();
        this.dockerInputBindings = new LinkedList<>();
        this.temporalInputDir = Files.createDirectory(getOutDir().resolve(".opencga_input"));
        this.inputFileUtils = new InputFileUtils(catalogManager);
    }

    @Override
    protected void close() {
        super.close();
        deleteTemporalFiles();
    }

    private void deleteTemporalFiles() {
        // Delete input files and temporal directory
        try (Stream<Path> paths = Files.walk(temporalInputDir)) {
            paths.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        } catch (IOException e) {
            logger.warn("Could not delete temporal input directory: " + temporalInputDir, e);
        }
    }

    /**
     * Process the input parameters of the command line.
     * @param value
     * @param inputFileUtils
     * @param cliParamsBuilder
     * @throws CatalogException
     */
    protected void processInputCli(String value, InputFileUtils inputFileUtils, StringBuilder cliParamsBuilder) throws CatalogException {
        if (inputFileUtils.isValidOpenCGAFile(value)) {
            File file = inputFileUtils.findOpenCGAFileFromPattern(study, value, token);
            if (inputFileUtils.fileMayContainReferencesToOtherFiles(file)) {
                Path outputFile = temporalInputDir.resolve(file.getName());
                List<File> files = inputFileUtils.findAndReplaceFilePathToUrisFromFile(study, file, outputFile, token);

                // Write outputFile as inputBinding
                dockerInputBindings.add(new AbstractMap.SimpleEntry<>(outputFile.toString(), outputFile.toString()));
                logger.info("Params: OpenCGA input file: '{}'", outputFile);
                cliParamsBuilder.append(outputFile).append(" ");

                // Add files to inputBindings to ensure they are also mounted (if any)
                for (File tmpFile : files) {
                    dockerInputBindings.add(new AbstractMap.SimpleEntry<>(tmpFile.getUri().getPath(), tmpFile.getUri().getPath()));
                    logger.info("Inner files from '{}': OpenCGA input file: '{}'", outputFile, tmpFile.getUri().getPath());
                }
            } else {
                String path = file.getUri().getPath();
                dockerInputBindings.add(new AbstractMap.SimpleEntry<>(path, path));
                logger.info("Params: OpenCGA input file: '{}'", path);
                cliParamsBuilder.append(path).append(" ");
            }
        } else {
            cliParamsBuilder.append(value).append(" ");
        }

    }

    protected void processOutputCli(String value, InputFileUtils inputFileUtils, StringBuilder cliParamsBuilder) throws CatalogException {
        String dynamicOutputFolder;
        if (inputFileUtils.isDynamicOutputFolder(value)) {
            // If it starts with $OUTPUT/...
            dynamicOutputFolder = inputFileUtils.getDynamicOutputFolder(value, getOutDir().toAbsolutePath().toString());
        } else {
            // If it starts directly with the subpath...
            dynamicOutputFolder = inputFileUtils.appendSubpath(getOutDir().toAbsolutePath().toString(), value);
        }
        logger.info("Params: Dynamic output folder: '{}'", dynamicOutputFolder);
        cliParamsBuilder.append(dynamicOutputFolder).append(" ");
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
        ObjectMap params = new ObjectMap()
                .append(JobDBAdaptor.QueryParams.TAGS.key(), tags)
                .append(JobDBAdaptor.QueryParams.TOOL_EXTERNAL_EXECUTOR.key(), executor);
        catalogManager.getJobManager().update(getStudyFqn(), getJobId(), params, QueryOptions.empty(), token);
    }

    protected void processInputParams(String commandLineParams, StringBuilder builder) throws CatalogException {
        String[] params = commandLineParams.split(" ");
        for (String param : params) {
            if (inputFileUtils.isDynamicOutputFolder(param)) {
                processOutputCli(param, inputFileUtils, builder);
            } else {
                processInputCli(param, inputFileUtils, builder);
            }
        }
    }

    protected String runDocker(String image, String cli) throws IOException {
        return runDocker(image, null, cli, null);
    }

    protected String runDocker(String image, AbstractMap.SimpleEntry<String, String> userOutputBinding, String cmdParams,
                               Map<String, String> userDockerParams) throws IOException {
        AbstractMap.SimpleEntry<String, String> outputBinding = userOutputBinding != null
                ? userOutputBinding
                : new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(), getOutDir().toAbsolutePath().toString());

        Map<String, String> dockerParams = new HashMap<>();
        // Establish working directory
        dockerParams.put("-w", getOutDir().toAbsolutePath().toString());
        dockerParams.put("--volume", "/var/run/docker.sock:/var/run/docker.sock");
        dockerParams.put("--env", "DOCKER_HOST='tcp://localhost:2375'");
        dockerParams.put("--network", "host");
        if (userDockerParams != null) {
            dockerParams.putAll(userDockerParams);
        }

        return DockerUtils.run(image, dockerInputBindings, outputBinding, cmdParams, dockerParams);
    }

}
