package org.opencb.opencga.catalog.utils;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InputFileUtils {

    private final CatalogManager catalogManager;

    private static final Pattern OPENCGA_PATH_PATTERN = Pattern.compile("^(?i)(ocga://|opencga://|file://)(.+)$");
    private static final Pattern OPENCGA_PATH_IN_LINE_PATTERN = Pattern.compile("(?i)(ocga://|opencga://|file://)([^\\s,;]+)");

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^{}]+)}");
    private static final String OUTPUT = "$OUTPUT";
    private static final List<String> OUTPUT_LIST = Arrays.asList("$OUTPUT", "$JOB_OUTPUT");
    private static final String FLAG = "$FLAG";
    private final Logger logger = LoggerFactory.getLogger(InputFileUtils.class);

    public InputFileUtils(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public boolean isValidOpenCGAFile(String content) {
        return OPENCGA_PATH_PATTERN.matcher(content).matches();
    }

    public File findOpenCGAFileFromPattern(String study, String content, String token) throws CatalogException {
        Matcher matcher = OPENCGA_PATH_PATTERN.matcher(content);
        if (!matcher.find()) {
            throw new CatalogException("Invalid OpenCGA file format. Accepted format is 'ocga://<path>', 'opencga://<path>' or"
                    + " 'file://<path>'.");
        }
        return getOpenCGAFile(study, matcher.group(2), token);
    }

    private File getOpenCGAFile(String study, String filePath, String token) throws CatalogException {
        logger.info("Looking for file '{}'", filePath);
        OpenCGAResult<File> result = catalogManager.getFileManager().get(study, filePath, FileManager.INCLUDE_FILE_URI_PATH, token);
        if (result.getNumResults() == 0) {
            throw new CatalogException("File '" + filePath + "' not found in study '" + study + "'");
        }
        return result.first();
    }

    public boolean fileMayContainReferencesToOtherFiles(File file) {
        // Check file size is smaller than 10MB
        if (file.getSize() >= 10 * 1024 * 1024) {
            return false;
        }

        // Check if file is compressed or binary by extension
        if (isCompressedOrBinaryFile(file)) {
            return false;
        }

        return true;
    }

    private boolean isCompressedOrBinaryFile(File file) {
        String fileName = file.getName().toLowerCase();
        // Compressed files
        String[] compressedExtensions = {".gz", ".zip", ".tar", ".bz2", ".xz", ".7z", ".rar"};
        for (String ext : compressedExtensions) {
            if (fileName.endsWith(ext)) {
                return true;
            }
        }

        // Binary files common in bioinformatics
        String[] binaryExtensions = {".bam", ".cram", ".bcf", ".pdf", ".png", ".jpg", ".jpeg", ".tiff"};
        for (String ext : binaryExtensions) {
            if (fileName.endsWith(ext)) {
                return true;
            }
        }

        return false;
    }

    public List<File> findAndReplaceFilePathToUrisFromFile(String study, File file, Path outFile, String token) throws CatalogException {
        BufferedReader br = null;
        BufferedWriter bw = null;

        List<File> fileList = new LinkedList<>();
        try {
            br = new BufferedReader(new FileReader(Paths.get(file.getUri()).toFile()));
            bw = new BufferedWriter(new FileWriter(outFile.toFile()));
            String line;
            while ((line = br.readLine()) != null) {
                Matcher matcher = OPENCGA_PATH_IN_LINE_PATTERN.matcher(line);
                while (matcher.find()) {
                    File tmpFile = getOpenCGAFile(study, matcher.group(2), token);
                    fileList.add(tmpFile);
                    line = matcher.replaceFirst(tmpFile.getUri().getPath());
                    matcher = OPENCGA_PATH_IN_LINE_PATTERN.matcher(line);
                }
                bw.write(line + "\n");
            }
        } catch (Exception e) {
            throw new CatalogException(e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                throw new CatalogException(e);
            }
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException e) {
                throw new CatalogException(e);
            }
        }
        return fileList;
    }

    public boolean isDynamicOutputFolder(String file) {
        for (String prefix : OUTPUT_LIST) {
            if (file.toUpperCase().startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    public boolean isFlag(String file) {
        return file.equalsIgnoreCase(FLAG);
    }

    public String getDynamicOutputFolder(String file, String outDir) throws CatalogException {
        for (String prefix : OUTPUT_LIST) {
            if (file.toUpperCase().startsWith(prefix)) {
                return outDir + file.substring(prefix.length());
            }
        }
        throw new CatalogException("Unexpected error. File '" + file + "' is not a dynamic output folder");
    }

    public String appendSubpath(String path, String subpath) {
        if ((path.endsWith("/") && !subpath.startsWith("/")) || (!path.endsWith("/") && subpath.startsWith("/"))) {
            return path + subpath;
        } else if (path.endsWith("/") && subpath.startsWith("/")) {
            return path + subpath.substring(1);
        } else {
            return path + "/" + subpath;
        }
    }

    /**
     * Given a command line that may contain OpenCGA file paths and variables using between ${xx}, obtain the final command line.
     *
     * Example:
     *     commandLineTemplate: "run ${INPUT} -o ${OUTPUT}"
     *     params: {INPUT=ocga://user@study/1.txt, OUTPUT=/tmp/output.txt}
     * Result: "run /path/to/1.txt -o /tmp/output.txt"
     *
     * @param study               Study where the files are located.
     * @param commandLineTemplate Command line template.
     * @param params              Map with the parameters to be replaced in the command line template.
     * @param temporalInputDir    Empty directory where files that require any changes will be copied.
     * @param inputBindings       Map where it will store the input bindings that need to be mounted in the Docker.
     * @param outDir              Output directory.
     * @param token               User token.
     * @return                    Final command line with the parameters replaced.
     * @throws CatalogException   If any error occurs while processing the command line.
     */
    public String processCommandLine(String study, String commandLineTemplate, Map<String, String> params, Path temporalInputDir,
                                     List<AbstractMap.SimpleEntry<String, String>> inputBindings, String outDir, String token)
            throws CatalogException {
        // Replace variables
        Matcher matcher = VARIABLE_PATTERN.matcher(commandLineTemplate);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String variableName = matcher.group(1);
            if (!params.containsKey(variableName)) {
                throw new CatalogException("Variable '" + variableName + "' not found in the params object");
            }
            String replacement = params.get(variableName);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        String variablesReplaced = sb.toString();

        // Replace input variables
        matcher = OPENCGA_PATH_IN_LINE_PATTERN.matcher(variablesReplaced);
        sb = new StringBuffer();
        while (matcher.find()) {
            String group = matcher.group(0);
            File file = findOpenCGAFileFromPattern(study, group, token);

            if (fileMayContainReferencesToOtherFiles(file)) {
                Path outputFile = temporalInputDir.resolve(file.getName());
                List<File> files = findAndReplaceFilePathToUrisFromFile(study, file, outputFile, token);

                // Write outputFile as inputBinding
                inputBindings.add(new AbstractMap.SimpleEntry<>(outputFile.toString(), outputFile.toString()));
                logger.info("Params: OpenCGA input file: '{}'", outputFile);
                matcher.appendReplacement(sb, Matcher.quoteReplacement(outputFile.toString()));

                // Add files to inputBindings to ensure they are also mounted (if any)
                for (File tmpFile : files) {
                    inputBindings.add(new AbstractMap.SimpleEntry<>(tmpFile.getUri().getPath(), tmpFile.getUri().getPath()));
                    logger.info("Inner files from '{}': OpenCGA input file: '{}'", outputFile, tmpFile.getUri().getPath());
                }
            } else {
                String path = file.getUri().getPath();
                inputBindings.add(new AbstractMap.SimpleEntry<>(path, path));
                logger.info("Params: OpenCGA input file: '{}'", path);
                matcher.appendReplacement(sb, Matcher.quoteReplacement(path));
            }
        }
        matcher.appendTail(sb);
        String finalCli = sb.toString();

        // Replace output variables
        for (String outputVariable : OUTPUT_LIST) {
            finalCli = finalCli.replaceAll(outputVariable, outDir);
        }

        return finalCli;
    }

}
