package org.opencb.opencga.catalog.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.models.externalTool.ExternalToolVariable;
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
    private static final String FLAG = "$FLAG";

    // Precompiled patterns for output variables - matches exact variable (not $OUTPUTS, $OUTPUT1, etc.)
    // The variable must be followed by non-word character or end of string (mimics bash behavior)
    // Wrapped in a capturing group so we can get the matched variable name
    private static final Pattern OUTPUT_PATTERN = Pattern.compile("(" + Pattern.quote("$OUTPUT") + ")(?![a-zA-Z0-9_])");
    private static final Pattern JOB_OUTPUT_PATTERN = Pattern.compile("(" + Pattern.quote("$JOB_OUTPUT") + ")(?![a-zA-Z0-9_])");
    private static final List<Pattern> OUTPUT_PATTERNS = Arrays.asList(OUTPUT_PATTERN, JOB_OUTPUT_PATTERN);

    // Pattern to match at start of string for checking dynamic output folders
    private static final Pattern OUTPUT_START_PATTERN = Pattern.compile("^(" + Pattern.quote("$OUTPUT") + ")(?![a-zA-Z0-9_])");
    private static final Pattern JOB_OUTPUT_START_PATTERN = Pattern.compile("^(" + Pattern.quote("$JOB_OUTPUT") + ")(?![a-zA-Z0-9_])");
    private static final List<Pattern> OUTPUT_START_PATTERNS = Arrays.asList(OUTPUT_START_PATTERN, JOB_OUTPUT_START_PATTERN);
    private final Logger logger = LoggerFactory.getLogger(InputFileUtils.class);

    public InputFileUtils(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public static boolean isValidOpenCGAFile(String content) {
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

        // Check if file extension is whitelisted
        return isExtensionWhitelistedForEdition(file);
    }

    private boolean isExtensionWhitelistedForEdition(File file) {
        String fileName = file.getName().toLowerCase();
        // Compressed files
        String[] whitelistedExtension = {".csv", ".tsv", ".conf", ".xml", ".json", ".yaml", ".txt"};
        for (String ext : whitelistedExtension) {
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
        for (Pattern pattern : OUTPUT_START_PATTERNS) {
            if (pattern.matcher(file.toUpperCase()).find()) {
                return true;
            }
        }
        return false;
    }

    public boolean isFlag(String file) {
        return file.equalsIgnoreCase(FLAG);
    }

    public String getDynamicOutputFolder(String file, String outDir) throws CatalogException {
        for (Pattern pattern : OUTPUT_START_PATTERNS) {
            Matcher matcher = pattern.matcher(file.toUpperCase());
            if (matcher.find()) {
                // Get the matched variable name from the capturing group
                String matchedVariable = matcher.group(1);
                // Return outDir + everything after the matched variable
                return outDir + file.substring(matchedVariable.length());
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
     * @param variables           List of external tool variables.
     * @param temporalInputDir    Empty directory where files that require any changes will be copied.
     * @param inputBindings       Map where it will store the input bindings that need to be mounted in the Docker.
     * @param outDir              Output directory.
     * @param token               User token.
     * @return                    Final command line with the parameters replaced.
     * @throws CatalogException   If any error occurs while processing the command line.
     */
    public String processCommandLine(String study, String commandLineTemplate, Map<String, String> params,
                                     List<ExternalToolVariable> variables, Path temporalInputDir,
                                     List<AbstractMap.SimpleEntry<String, String>> inputBindings, String outDir, String token)
            throws CatalogException {
        Map<String, ExternalToolVariable> variableMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(variables)) {
            for (ExternalToolVariable variable : variables) {
                String variableId = removePrefix(variable.getId());
                variableMap.put(variableId, variable);
            }
        }

        Set<String> replacedParams = new HashSet<>();

        // Replace variables
        Matcher matcher = VARIABLE_PATTERN.matcher(commandLineTemplate);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String variableName = matcher.group(1);
            String replacement;
            if (params.containsKey(variableName)) {
                replacement = params.get(variableName);
            } else if (variableMap.containsKey(removePrefix(variableName))) {
                ExternalToolVariable variable = variableMap.get(removePrefix(variableName));
                if (StringUtils.isNotEmpty(variable.getDefaultValue())) {
                    replacement = variable.getDefaultValue();
                } else if (variable.isOutput()) {
                    replacement = outDir;
                } else if (variable.getType() == ExternalToolVariable.ExternalToolVariableType.FLAG) {
                    replacement = "";
                } else {
                    throw new CatalogException("Variable '" + variableName + "' is expected but could not be found in the params object "
                            + "and does not have a default value set in the tool.");
                }
            } else {
                throw new CatalogException("Variable '" + variableName + "' is expected but could not be found in the params object"
                        + " nor in the tool variables.");
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            replacedParams.add(variableName);
        }
        matcher.appendTail(sb);
        String variablesReplaced = sb.toString();

        // Replace input variables (file://, opencga://, ocga://)
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

        // Replace output variables ($OUTPUT, $JOB_OUTPUT)
        // Use precompiled regex patterns to ensure we only replace the exact variable (not $OUTPUTS, $OUTPUT1, etc.)
        // This mimics bash behavior where $OUTPUT is replaced but $OUTPUTS is not
        for (Pattern outputPattern : OUTPUT_PATTERNS) {
            Matcher outputMatcher = outputPattern.matcher(finalCli);
            finalCli = outputMatcher.replaceAll(Matcher.quoteReplacement(outDir));
        }

        // Add parameters not included in the command line template
        StringBuilder additionalParamsBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (!replacedParams.contains(entry.getKey())) {
                buildCliParams(entry.getKey(), entry.getValue(), additionalParamsBuilder);
            }
        }
        finalCli = finalCli + " " + additionalParamsBuilder;
        return finalCli;
    }

    private void buildCliParams(String key, String value, StringBuilder builder) {
        if (!key.startsWith("-")) {
            if (key.length() == 1) {
                builder.append("-"); // Single dash for single character parameters
            } else {
                builder.append("--");
            }
        }
        builder.append(key).append(" ");
        if (StringUtils.isNotEmpty(value)) {
            builder.append(value).append(" ");
        }
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

}
