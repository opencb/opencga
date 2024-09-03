package org.opencb.opencga.analysis.utils;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InputFileUtils {

    private final CatalogManager catalogManager;

    private static final Pattern OPERATION_PATTERN = Pattern.compile("^(?i)(ocga://|opencga://)(.+)$");

    private final static Logger logger = LoggerFactory.getLogger(InputFileUtils.class);

    public InputFileUtils(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public boolean isValidOpenCGAFile(String file) {
        return OPERATION_PATTERN.matcher(file).matches();
    }

    public File getOpenCGAFile(String study, String file, String token) throws CatalogException {
        Matcher matcher = OPERATION_PATTERN.matcher(file);

        if (!matcher.find()) {
            throw new CatalogException("Invalid OpenCGA file format. Accepted format is 'ocga://<path>' or 'opencga://<path>'");
        }

        String filePath = matcher.group(2);
        logger.info("Looking for file '{}'", filePath);
        OpenCGAResult<File> result = catalogManager.getFileManager().get(study, filePath, FileManager.INCLUDE_FILE_URI_PATH, token);
        if (result.getNumResults() == 0) {
            throw new CatalogException("File '" + file + "' not found in study '" + study + "'");
        }
        return result.first();
    }

}
