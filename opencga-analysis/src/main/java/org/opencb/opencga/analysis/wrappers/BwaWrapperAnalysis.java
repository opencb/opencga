package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.analysis.result.FileResult;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.exception.AnalysisException;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.*;

@Analysis(id = BwaWrapperAnalysis.ID, type = Analysis.AnalysisType.ALIGNMENT,
        description = "")
public class BwaWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "bwa";
    public final static String DESCRIPTION = "BWA is a software package for mapping low-divergent sequences against a large reference"
            + " genome";

    public final static String BWA_DOCKER_IMAGE = "alexcoppe/bwa";
    public final static String OUT_NAME = "out";

    private String command;
    private String fastaFile;
    private String indexBaseFile;
    private String fastq1File;
    private String fastq2File;
    private String samFile;

    protected void check() throws Exception {
        super.check();
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("BWA command line:" + commandLine);
            try {
                Set<String> filenamesBeforeRunning = new HashSet<>(getFilenames(getOutDir()));

                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(getCommandLine())
                        .setOutputOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

                // Add the output files to the analysis result file
                List<String> outNames = getFilenames(getOutDir());
                for (String name : outNames) {
                    if (!filenamesBeforeRunning.contains(name)) {
                        if (FileUtils.sizeOf(new File(getOutDir() + "/" + name)) > 0) {
                            FileResult.FileType fileType = FileResult.FileType.TAB_SEPARATED;
                            if (name.endsWith("txt") || name.endsWith("log") || name.endsWith("sam")) {
                                fileType = FileResult.FileType.PLAIN_TEXT;
                            }  else if (name.endsWith("sa") || name.endsWith("bwt") || name.endsWith("pac")) {
                                fileType = FileResult.FileType.BINARY;
                            }
                            addFile(getOutDir().resolve(name), fileType);
                        }
                    }
                }
                // Check BWA errors by reading the stdout and stderr files
                boolean success = false;
                switch (command) {
                    case "index": {
                        if (new File(fastaFile + ".sa").exists()
                                && new File(fastaFile + ".bwt").exists()
                                && new File(fastaFile + ".pac").exists()
                                && new File(fastaFile + ".amb").exists()
                                && new File(fastaFile + ".ann").exists()) {
                            success = true;
                        }
                    }
                    case "mem": {
                        if (new File(samFile).exists()) {
                            success = true;
                        }
                    }
                }
                if (!success) {
                    File file = new File(getOutDir() + "/" + STDERR_FILENAME);
                    String msg = "Something wrong executing BWA ";
                    if (file.exists()) {
                        msg = StringUtils.join(FileUtils.readLines(file, Charset.defaultCharset()), ". ");
                    }
                    throw new AnalysisException(msg);
                }
            } catch (Exception e) {
                throw new AnalysisException(e);
            }
        });
    }

    @Override
    public String getDockerImageName() {
        return BWA_DOCKER_IMAGE;
    }

    @Override
    public String getCommandLine() {
        StringBuilder sb = new StringBuilder("docker run ");

        // Mount management
        Map<String, String> srcTargetMap = new HashMap<>();
        updateSrcTargetMap(fastaFile, sb, srcTargetMap);
        updateSrcTargetMap(indexBaseFile, sb, srcTargetMap);
        updateSrcTargetMap(fastq1File, sb, srcTargetMap);
        updateSrcTargetMap(fastq2File, sb, srcTargetMap);

        sb.append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ");

        // Docker image and version
        sb.append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // BWA command
        sb.append(" ").append(command);

        // BWA options
        for (String param : params.keySet()) {
            if (checkParam(param)) {
                String value = params.getString(param);
                sb.append(" -").append(param);
                if (StringUtils.isNotEmpty(value)) {
                    sb.append(" ").append(value);
                }
            }
        }

        switch (command) {
            case "index": {
                if (StringUtils.isNotEmpty(fastaFile)) {
                    File file = new File(fastaFile);
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
            }

            case "mem": {
                if (StringUtils.isEmpty(samFile)) {
                    samFile = DOCKER_OUTPUT_PATH + "/out.sam";
                }
                sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/").append(new File(samFile).getName());

                if (StringUtils.isNotEmpty(indexBaseFile)) {
                    File file = new File(indexBaseFile);
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }

                if (StringUtils.isNotEmpty(fastq1File)) {
                    File file = new File(fastq1File);
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }

                if (StringUtils.isNotEmpty(fastq2File)) {
                    File file = new File(fastq1File);
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
            }
        }

        return sb.toString();
    }

    private boolean checkParam(String param) {
        if (param.equals(DOCKER_IMAGE_VERSION_PARAM) || param.equals("o")) {
            return false;
        }
        return true;
    }


    public String getCommand() {
        return command;
    }

    public BwaWrapperAnalysis setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getFastaFile() {
        return fastaFile;
    }

    public BwaWrapperAnalysis setFastaFile(String fastaFile) {
        this.fastaFile = fastaFile;
        return this;
    }

    public String getIndexBaseFile() {
        return indexBaseFile;
    }

    public BwaWrapperAnalysis setIndexBaseFile(String indexBaseFile) {
        this.indexBaseFile = indexBaseFile;
        return this;
    }

    public String getFastq1File() {
        return fastq1File;
    }

    public BwaWrapperAnalysis setFastq1File(String fastq1File) {
        this.fastq1File = fastq1File;
        return this;
    }

    public String getFastq2File() {
        return fastq2File;
    }

    public BwaWrapperAnalysis setFastq2File(String fastq2File) {
        this.fastq2File = fastq2File;
        return this;
    }

    public String getSamFile() {
        return samFile;
    }

    public BwaWrapperAnalysis setSamFile(String samFile) {
        this.samFile = samFile;
        return this;
    }
}
