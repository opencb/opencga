package org.opencb.opencga.analysis.wrappers.liftover;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.io.FileUtils.readLines;
import static org.opencb.opencga.core.api.FieldConstants.LIFTOVER_VCF_INPUT_FOLDER;

@ToolExecutor(id = LiftoverWrapperAnalysisExecutor.ID,
        tool = LiftoverWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class LiftoverWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public final static String ID = LiftoverWrapperAnalysis.ID + "-local";

    private final static String VIRTUAL_SCRIPT_FOLDER = "/script";
    private final static String VIRTUAL_INPUT_FOLDER = "/input";
    private final static String VIRTUAL_OUTPUT_FOLDER = "/output";
    private final static String VIRTUAL_RESOURCES_FOLDER = "/" + LiftoverWrapperAnalysis.RESOURCES_FOLDER;

    private String study;
    private Path liftoverPath;
    private List<File> files;
    private String targetAssembly;
    private String vcfDest;
    private Path resourcePath;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    protected void run() throws Exception {
        // Add parameters
        addParameters();

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
        if (numFails == files.size()) {
            throw new ToolExecutorException("Liftover failed for all input VCF files");
        }
    }

    private void runLiftover(File file, Path outPath) throws ToolExecutorException {
        try {
            // Input binding
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(liftoverPath.toAbsolutePath().toString(), VIRTUAL_SCRIPT_FOLDER));
            inputBindings.add(new AbstractMap.SimpleEntry<>(file.getParent(), VIRTUAL_INPUT_FOLDER));
            inputBindings.add(new AbstractMap.SimpleEntry<>(resourcePath.toAbsolutePath().toString(), VIRTUAL_RESOURCES_FOLDER));

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outPath.toAbsolutePath().toString(),
                    VIRTUAL_OUTPUT_FOLDER);

            // Main command line and params
            String params = "bash " + VIRTUAL_SCRIPT_FOLDER + "/liftover.sh"
                    + " " + Paths.get(VIRTUAL_INPUT_FOLDER).resolve(file.getName())
                    + " " + targetAssembly
                    + " " + VIRTUAL_OUTPUT_FOLDER
                    + " " + VIRTUAL_RESOURCES_FOLDER;

            // Execute Pythong script in docker
            String dockerImage = "opencb/opencga-ext-tools:" + GitRepositoryState.getInstance().getBuildVersion();

            String dockerCli = buildCommandLine(dockerImage, inputBindings, outputBinding, params, null);
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);
            runCommandLine(dockerCli);

            // Check result file
            String outFilename;
            if (file.getName().endsWith(".vcf.gz")) {
                outFilename = file.getName().replace(".vcf.gz", "");
            } else if (file.getName().endsWith(".vcf")) {
                outFilename = file.getName().replace(".vcf", "");
            } else {
                throw new IllegalArgumentException("File " + file.getName() + " must end with .vcf or .vcf.gz");
            }
            outFilename += "." + targetAssembly + ".liftover.vcf.gz";
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

    private void addParameters() throws ToolException {
        addParam("liftoverPath", liftoverPath);
        addParam("files", files);
        addParam("targetAssembly", targetAssembly);
        addParam("vcfDest", vcfDest);
        addParam("resourcePath", resourcePath);
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
