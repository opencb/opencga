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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InputFileUtils {

    private final CatalogManager catalogManager;

    private static final Pattern OPENCGA_PATH_PATTERN = Pattern.compile("^(?i)(ocga://|opencga://|file://)(.+)$");
    private static final Pattern OPENCGA_PATH_IN_LINE_PATTERN = Pattern.compile("(?i)(ocga://|opencga://|file://)(\\S+)");
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
        return file.getSize() < 10 * 1024 * 1024;
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

}
