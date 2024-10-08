package org.opencb.opencga.analysis.wrappers.liftover;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.io.FileUtils.readLines;
import static org.opencb.opencga.core.api.FieldConstants.LIFTOVER_VCF_INPUT_FOLDER;

@ToolExecutor(id = LiftoverWrapperAnalysisExecutor.ID,
        tool = LiftoverWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class LiftoverWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = LiftoverWrapperAnalysis.ID + "-local";

    private static final String LIFTOVER_SCRIPT = "liftover.sh";

    private String study;
    private Path liftoverPath;
    private List<File> files;
    private String targetAssembly;
    private String vcfDest;
    private Path resourcePath;

    @Override
    protected void run() throws Exception {
        // Add parameters
        addParameters(getParameters());

        Path outPath = (StringUtils.isEmpty(vcfDest) ? getOutDir() : Paths.get(vcfDest));

        int numFails = 0;
        for (File file : files) {
            try {
                if (LIFTOVER_VCF_INPUT_FOLDER.equals(vcfDest)) {
                    outPath = Paths.get(file.getParent());
                }
                runLiftover(file, outPath);
            } catch (ToolExecutorException e) {
                numFails++;
                String msg = "Liftover failed for file '" + file.getName() + "'";
                addWarning(msg);
                logger.warn(msg, e);
            }
        }

        // Add resources as executor attributes
        addResources(resourcePath);

        if (numFails == files.size()) {
            throw new ToolExecutorException("Liftover failed for all input VCF files");
        }
    }

    private void runLiftover(File file, Path outPath) throws ToolExecutorException {
        try {
            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH).resolve(LIFTOVER_SCRIPT);
            inputBindings.add(new AbstractMap.SimpleEntry<>(liftoverPath.resolve(LIFTOVER_SCRIPT).toAbsolutePath().toString(),
                    virtualScriptPath.toString()));
            Path virtualVcfPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(file.getName());
            inputBindings.add(new AbstractMap.SimpleEntry<>(file.getAbsolutePath(), virtualVcfPath.toString()));
            inputBindings.add(new AbstractMap.SimpleEntry<>(resourcePath.toAbsolutePath().toString(), RESOURCES_VIRTUAL_PATH));

            // Read only input bindings
            Set<String> readOnlyInputBindings = new HashSet<>();
            readOnlyInputBindings.add(virtualScriptPath.toString());
            readOnlyInputBindings.add(virtualVcfPath.toString());

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outPath.toAbsolutePath().toString(),
                    OUTPUT_VIRTUAL_PATH);

            // Main command line and params
            String params = "bash " + virtualScriptPath
                    + " " + virtualVcfPath
                    + " " + targetAssembly
                    + " " + OUTPUT_VIRTUAL_PATH
                    + " " + RESOURCES_VIRTUAL_PATH;

            // Execute Pythong script in docker
            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();

            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBinding, params, null);
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);
            runCommandLine(dockerCli);

            // Check result file
            String outFilename = LiftoverWrapperAnalysis.getLiftoverFilename(file.getName(), targetAssembly);
            Path outFile = outPath.resolve(outFilename);
            if (!Files.exists(outFile) || outFile.toFile().length() == 0) {
                java.io.File stdoutFile = getOutDir().resolve(DockerWrapperAnalysisExecutor.STDOUT_FILENAME).toFile();
                List<String> stdoutLines = readLines(stdoutFile, Charset.defaultCharset());

                java.io.File stderrFile = getOutDir().resolve(DockerWrapperAnalysisExecutor.STDERR_FILENAME).toFile();
                List<String> stderrLines = readLines(stderrFile, Charset.defaultCharset());

                throw new ToolExecutorException("Error executing Liftover: the result file was not created.\nStdout messages: "
                        + StringUtils.join("\n", stdoutLines) + "\nStderr messages: " + StringUtils.join("\n", stderrLines));
            }
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    private ObjectMap getParameters() {
        ObjectMap params = new ObjectMap();
        params.put("liftoverPath", liftoverPath);
        params.put("files", files);
        params.put("targetAssembly", targetAssembly);
        params.put("vcfDest", vcfDest);
        params.put("resourcePath", resourcePath);
        return params;
    }

    public String getStudy() {
        return study;
    }

    public LiftoverWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Path getLiftoverPath() {
        return liftoverPath;
    }

    public LiftoverWrapperAnalysisExecutor setLiftoverPath(Path liftoverPath) {
        this.liftoverPath = liftoverPath;
        return this;
    }

    public List<File> getFiles() {
        return files;
    }

    public LiftoverWrapperAnalysisExecutor setFiles(List<File> files) {
        this.files = files;
        return this;
    }

    public String getTargetAssembly() {
        return targetAssembly;
    }

    public LiftoverWrapperAnalysisExecutor setTargetAssembly(String targetAssembly) {
        this.targetAssembly = targetAssembly;
        return this;
    }

    public String getVcfDest() {
        return vcfDest;
    }

    public LiftoverWrapperAnalysisExecutor setVcfDest(String vcfDest) {
        this.vcfDest = vcfDest;
        return this;
    }

    public Path getResourcePath() {
        return resourcePath;
    }

    public LiftoverWrapperAnalysisExecutor setResourcePath(Path resourcePath) {
        this.resourcePath = resourcePath;
        return this;
    }
}
