package org.opencb.opencga.analysis.wrappers.executors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.io.SamtoolsFlagstatsParser;
import org.opencb.biodata.formats.sequence.fastqc.FastQc;
import org.opencb.biodata.formats.sequence.fastqc.io.FastQcParser;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.wrappers.FastqcWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.SamtoolsWrapperAnalysis;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class SamtoolsWrapperAnalysisExecutor extends OpenCgaWrapperAnalysisExecutor {

    private String command;
    private String bamFile;

    public SamtoolsWrapperAnalysisExecutor(String studyId, ObjectMap params, Path outDir, Path scratchDir, CatalogManager catalogManager,
                                           String token) {
        super(studyId, params, outDir, scratchDir, catalogManager, token);

        sep = " ";
        shortPrefix = "-";
        longPrefix = "--";
    }

    @Override
    public void run() throws ToolException {
        StringBuilder sb = initCommandLine();

        // Append mounts
        List<Pair<String, String>> inputFilenames = new ArrayList<>(Arrays.asList(new ImmutablePair<>("", bamFile)));
        Map<String, String> srcTargetMap = new HashMap<>();
        appendMounts(inputFilenames, srcTargetMap, sb);

        // Append docker image, version and command
        appendCommand("samtools " + command, sb);

        // Append input file params
        appendInputFiles(inputFilenames, srcTargetMap, sb);

        // Append other params
        Set<String> skipParams =  new HashSet<>(Arrays.asList("o", "output"));
        appendOtherParams(skipParams, sb);

        // Append output file params
        List<Pair<String, String>> outputFilenames = new ArrayList<>();
        appendOutputFiles(outputFilenames, sb);

        // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
        runCommandLine(sb.toString());
    }

    public SamtoolsFlagstats getFlagstatsResult() throws ToolException {
        try {
            Path path = getScratchDir().resolve(getId() + "." + STDOUT_FILENAME);
            return SamtoolsFlagstatsParser.parse(path);
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    @Override
    protected String getId() {
        return FastqcWrapperAnalysis.ID;
    }

    @Override
    protected String getDockerImageName() {
        return SamtoolsWrapperAnalysis.SAMTOOLS_DOCKER_IMAGE;
    }

    public String getCommand() {
        return command;
    }

    public SamtoolsWrapperAnalysisExecutor setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getBamFile() {
        return bamFile;
    }

    public SamtoolsWrapperAnalysisExecutor setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }
}
