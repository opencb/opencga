package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Tool(id = BwaWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT,
        description = "")
public class BwaWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "bwa";
    public final static String DESCRIPTION = "BWA is a software package for mapping low-divergent sequences against a large reference"
            + " genome.";

    public final static String BWA_DOCKER_IMAGE = "alexcoppe/bwa";

    private String command;
    private String fastaFile;
    private String indexBaseFile;
    private String fastq1File;
    private String fastq2File;
    private String samFilename;

    private Map<String, URI> fileUriMap = new HashMap<>();

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(command)) {
            throw new ToolException("Missig BWA command. Supported commands are 'index' and 'mem'");
        }

        switch (command) {
            case "index":
            case "mem":
                break;
            default:
                // TODO: support fastmap, pemerge, aln, samse, sampe, bwasw, shm, fa2pac, pac2bwt, pac2bwtgen, bwtupdate, bwt2sa
                throw new ToolException("BWA command '" + command + "' is not available. Supported commands are 'index' and 'mem'");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("BWA command line:" + commandLine);
            try {
                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(commandLine)
                        .setOutputOutputStream(
                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(
                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

                // Check BWA errors by reading the stdout and stderr files
                boolean success = false;
                switch (command) {
                    case "index": {
                        File file = params.containsKey("p")
                                ? new File(params.getString("p"))
                                : new File(fileUriMap.get(fastaFile).getPath());
                        String prefix = getOutDir().toAbsolutePath() + "/" + file.getName();
                        String[] suffixes = new String[]{".sa", ".bwt", ".pac", ".amb", ".ann"};
                        success = true;
                        for (String suffix : suffixes) {
                            if (!new File(prefix + suffix).exists()) {
                                success = false;
                                break;
                            }
                        }
                        if (success) {
                            // Get catalog path
                            OpenCGAResult<org.opencb.opencga.core.models.File> fileResult;
                            try {
                                fileResult = catalogManager.getFileManager().get(getStudy(), fastaFile, QueryOptions.empty(), token);
                            } catch (CatalogException e) {
                                throw new ToolException("Error accessing file '" + fastaFile + "' of the study " + getStudy() + "'", e);
                            }
                            if (fileResult.getNumResults() <= 0) {
                                throw new ToolException("File '" + fastaFile + "' not found in study '" + getStudy() + "'");
                            }

                            String catalogPath = fileResult.getResults().get(0).getPath();
                            Path dest = new File(file.getParent()).toPath();

                            for (String suffix : suffixes) {
                                Path src = new File(prefix + suffix).toPath();

                                System.out.println("src = " + src + ", dest = " + dest + ", catalog path = " + catalogPath.concat(suffix));
                                moveFile(getStudy(), src, dest, catalogPath.concat(suffix), token);
                            }
                        }
                        break;
                    }
                    case "mem": {
                        if (new File(getOutDir() + "/" + samFilename).exists()) {
                            success = true;
                        }
                        break;
                    }
                }
                if (!success) {
                    File file = new File(getScratchDir() + "/" + STDERR_FILENAME);
                    String msg = "Something wrong executing BWA ";
                    if (file.exists()) {
                        msg = StringUtils.join(FileUtils.readLines(file, Charset.defaultCharset()), ". ");
                    }
                    throw new ToolException(msg);
                }
            } catch (Exception e) {
                throw new ToolException(e);
            }
        });
    }

    @Override
    public String getDockerImageName() {
        return BWA_DOCKER_IMAGE;
    }

    @Override
    public String getCommandLine() throws ToolException {
        StringBuilder sb = new StringBuilder("docker run ");

        // Mount management
        Map<String, String> srcTargetMap = new HashMap<>();
        updateFileMaps(fastaFile, sb, fileUriMap, srcTargetMap);
        updateFileMaps(indexBaseFile, sb, fileUriMap, srcTargetMap);
        updateFileMaps(fastq1File, sb, fileUriMap, srcTargetMap);
        updateFileMaps(fastq2File, sb, fileUriMap, srcTargetMap);

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
                File file = params.containsKey("p")
                        ? new File(params.getString("p"))
                        : new File(fileUriMap.get(fastaFile).getPath());

                sb.append(" -p ").append(DOCKER_OUTPUT_PATH).append("/").append(file.getName());

                if (StringUtils.isNotEmpty(fastaFile)) {
                    file = new File(fileUriMap.get(fastaFile).getPath());
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }

                break;
            }

            case "mem": {
                if (StringUtils.isEmpty(samFilename)) {
                    samFilename = "out.sam";
                }
                sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/").append(samFilename);

                if (StringUtils.isNotEmpty(indexBaseFile)) {
                    File file = new File(fileUriMap.get(indexBaseFile).getPath());
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }

                if (StringUtils.isNotEmpty(fastq1File)) {
                    File file = new File(fileUriMap.get(fastq1File).getPath());
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }

                if (StringUtils.isNotEmpty(fastq2File)) {
                    File file = new File(fileUriMap.get(fastq2File).getPath());
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                break;
            }
        }

        return sb.toString();
    }

    private boolean checkParam(String param) {
        if (param.equals(DOCKER_IMAGE_VERSION_PARAM)) {
            return false;
        } else if ("index".equals(command) && "p".equals(param)) {
            return false;
        } else if ("mem".equals(command) && "o".equals(param)) {
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

    public String getSamFilename() {
        return samFilename;
    }

    public BwaWrapperAnalysis setSamFilename(String samFilename) {
        this.samFilename = samFilename;
        return this;
    }
}
