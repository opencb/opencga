/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileUtils {

    private static Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private final CatalogManager catalogManager;

    public static final Map<File.Format, Pattern> FORMAT_MAP = new HashMap<>();

    static {
        FORMAT_MAP.put(File.Format.IMAGE, Pattern.compile(".*\\.(png|jpg|bmp|svg|gif|jpeg|tfg)(\\.[\\w]+)*", Pattern.CASE_INSENSITIVE));
    }

    public FileUtils(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    /**
     * Check if the fileURI related to the provided file exists, and modifies the file status if necessary.
     * <p>
     * For READY files with a non existing file, set status to MISSING. "Lost file"
     * For MISSING files who recover the file, set status to READY. "Found file"
     *
     * @param studyStr          Study corresponding to the file to be checked.
     * @param file              File to check
     * @param calculateChecksum Calculate checksum for "found files"
     * @param sessionId         User's sessionId
     * @return If there is any change, returns the modified file. Else, return the same file.
     * @throws CatalogException CatalogException
     */
    public File checkFile(String studyStr, File file, boolean calculateChecksum, String sessionId) throws CatalogException {
        if (!file.getType().equals(File.Type.FILE)) {
            return file;
        }

        File modifiedFile = file;
        switch (file.getInternal().getStatus().getName()) {
            case FileStatus.READY:
            case FileStatus.MISSING: {
                URI fileUri = catalogManager.getFileManager().getUri(file);
                IOManager ioManager;
                try {
                    ioManager = catalogManager.getIoManagerFactory().get(fileUri);
                } catch (IOException e) {
                    throw CatalogIOException.ioManagerException(fileUri, e);
                }

                if (!ioManager.exists(fileUri)) {
                    logger.warn("File { id:" + file.getPath() + ", path:\"" + file.getPath() + "\" } lost tracking from file " + fileUri);
                    if (!file.getInternal().getStatus().getName().equals(FileStatus.MISSING)) {
                        logger.info("Set status to " + FileStatus.MISSING);
                        ObjectMap params = new ObjectMap(FileDBAdaptor.UpdateParams.STATUS_NAME.key(), FileStatus.MISSING);
                        catalogManager.getFileManager().update(studyStr, file.getPath(), params, null, sessionId);
                        modifiedFile = catalogManager.getFileManager().get(studyStr, file.getPath(), null, sessionId).first();
                    }
                } else if (file.getInternal().getStatus().getName().equals(FileStatus.MISSING)) {
                    logger.info("File { path:\"" + file.getPath() + "\" } recover tracking from file " + fileUri);
                    logger.info("Set status to " + FileStatus.READY);
                    ObjectMap params = getModifiedFileAttributes(file, fileUri, calculateChecksum);
                    params.put(FileDBAdaptor.UpdateParams.STATUS_NAME.key(), FileStatus.READY);
                    catalogManager.getFileManager().update(studyStr, file.getPath(), params, QueryOptions.empty(), sessionId);
                    modifiedFile = catalogManager.getFileManager().get(studyStr, file.getPath(), null, sessionId).first();
                }
                break;
            }
//            case File.FileStatus.TRASHED: {
//                URI fileUri = catalogManager.getFileManager().getUri(file);
//                if (!catalogManager.getCatalogIOManagerFactory().get(fileUri).exists(fileUri)) {
//                    modifiedFile = file;
//                    // TODO: Change status to remove.
////                    catalogManager.modifyFile(file.getId(), new ObjectMap("status.name", File.FileStatus.DELETED), sessionId);
////                    modifiedFile = catalogManager.getFile(file.getId(), sessionId).first();
//                    break;
//                }
//            }
            default:
                break;
        }
        return modifiedFile;
    }

    /**
     * Get a ObjectMap with some fields if they have been modified.
     * size
     * modificationDate
     * checksum
     * uri
     *
     * @param file              file
     * @param fileUri           If null, calls to getFileUri()
     *                          <p>
     *                          TODO: Lazy checksum: Only calculate checksum if the size has changed.
     * @param calculateChecksum Calculate checksum to check if have changed
     * @return ObjectMap ObjectMap
     * @throws CatalogException CatalogException
     */
    public ObjectMap getModifiedFileAttributes(File file, URI fileUri, boolean calculateChecksum) throws CatalogException {
        if (fileUri == null) {
            fileUri = catalogManager.getFileManager().getUri(file);
        }
        String checksum = null;
        if (calculateChecksum) {
            try {
                checksum = catalogManager.getIoManagerFactory().get(fileUri).calculateChecksum(fileUri);
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(fileUri, e);
            }
        }
        return getModifiedFileAttributes(file, checksum, fileUri, null);
    }

    /**
     * Get a ObjectMap with some fields if they have been modified.
     * size
     * checksum
     * uri
     *
     * @throws CatalogException CatalogException
     */
    private ObjectMap getModifiedFileAttributes(File file, String checksum, URI fileUri, ObjectMap parameters) throws CatalogException {
        parameters = ParamUtils.defaultObject(parameters, ObjectMap::new);
        if (fileUri == null) {
            fileUri = catalogManager.getFileManager().getUri(file);
        }
        IOManager ioManager;
        try {
            ioManager = catalogManager.getIoManagerFactory().get(fileUri);
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(fileUri, e);
        }

        if (StringUtils.isNotEmpty(checksum)) {
            if (file.getChecksum() == null || !checksum.equals(file.getChecksum())) {
                parameters.put(FileDBAdaptor.QueryParams.CHECKSUM.key(), checksum);
            }
        }

        if (file.getUri() == null || !file.getUri().toString().equals(fileUri.toString())) {
            parameters.put(FileDBAdaptor.QueryParams.URI.key(), fileUri.toString());
        }

        try {
            long size = ioManager.getFileSize(fileUri);
            if (file.getSize() != size) {
                parameters.put(FileDBAdaptor.QueryParams.SIZE.key(), size);
            }
        } catch (CatalogIOException e) {
            e.printStackTrace();
            logger.error("Can't get fileSize", e);
        }

        return parameters;
    }

    public static File.Bioformat detectBioformat(URI uri) {
        return detectBioformat(uri, detectFormat(uri), detectCompression(uri));
    }

    public static File.Bioformat detectBioformat(URI uri, File.Format format, File.Compression compression) {
        String path = uri.getPath();
        Path source = Paths.get(uri);
        String mimeType;

        try {
            switch (format) {
                case VCF:
                case GVCF:
                case BCF:
                    return File.Bioformat.VARIANT;
                case TBI:
                    break;
                case SAM:
                case BAM:
                case CRAM:
                case CRAI:
                case BAI:
                    return File.Bioformat.ALIGNMENT;
                case FASTA:
                    return File.Bioformat.REFERENCE_GENOME;
                case BIGWIG:
                    return File.Bioformat.COVERAGE;
                case FASTQ:
                    return File.Bioformat.SEQUENCE;
                case PED:
                    return File.Bioformat.PEDIGREE;
                case TAB_SEPARATED_VALUES:
                    break;
                case COMMA_SEPARATED_VALUES:
                    break;
                case PROTOCOL_BUFFER:
                    break;
                case PLAIN:
                    break;
                case JSON:
                case AVRO:
                    String file;
                    if (compression != File.Compression.NONE) {
                        file = com.google.common.io.Files.getNameWithoutExtension(uri.getPath()); //Remove compression extension
                        file = com.google.common.io.Files.getNameWithoutExtension(file);  //Remove format extension
                    } else {
                        file = com.google.common.io.Files.getNameWithoutExtension(uri.getPath()); //Remove format extension
                    }

                    if (file.endsWith("variants")) {
                        return File.Bioformat.VARIANT;
                    } else if (file.endsWith("alignments")) {
                        return File.Bioformat.ALIGNMENT;
                    }
                    break;
                case PARQUET:
                    break;
                case IMAGE:
                case BINARY:
                case EXECUTABLE:
                case UNKNOWN:
                case XML:
                    return File.Bioformat.NONE;
                default:
                    break;
            }

            mimeType = Files.probeContentType(source);

            if (path.endsWith(".nw")) {
                return File.Bioformat.OTHER_NEWICK;
            }

            if (mimeType == null
                    || !mimeType.equalsIgnoreCase("text/plain")
                    || path.endsWith(".redirection")
                    || path.endsWith(".Rout")
                    || path.endsWith("cel_files.txt")
                    || !path.endsWith(".txt")) {
                return File.Bioformat.NONE;
            }

            try (FileInputStream fstream = new FileInputStream(path);
                 BufferedReader br = new BufferedReader(new InputStreamReader(fstream))) {

                String strLine;
                int numberOfLines = 20;
                int i = 0;
                boolean names = false;
                while ((strLine = br.readLine()) != null) {
                    if (strLine.equalsIgnoreCase("")) {
                        continue;
                    }
                    if (i == numberOfLines) {
                        break;
                    }
                    if (strLine.startsWith("#")) {
                        if (strLine.startsWith("#NAMES")) {
                            names = true;
                        } else {
                            continue;
                        }
                    } else {
                        String[] fields = strLine.split("\t");
                        if (fields.length > 2) {
                            if (names && NumberUtils.isNumber(fields[1])) {
                                return File.Bioformat.DATAMATRIX_EXPRESSION;
                            }
                        } else if (fields.length == 1) {
                            if (fields[0].split(" ").length == 1 && !NumberUtils.isNumber(fields[0])) {
                                return File.Bioformat.IDLIST;
                            }
                        } else if (fields.length == 2) {
                            if (!fields[0].contains(" ") && NumberUtils.isNumber(fields[1])) {
                                return File.Bioformat.IDLIST_RANKED;
                            }
                        }
                    }
                    i++;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return File.Bioformat.NONE;
    }

    /**
     * @param uri Existing file uri to the file
     * @return File.Format. UNKNOWN if can't detect any format.
     */
    public static File.Format detectFormat(URI uri) {
        for (Map.Entry<File.Format, Pattern> entry : FORMAT_MAP.entrySet()) {
            if (entry.getValue().matcher(uri.getPath()).matches()) {
                return entry.getKey();
            }
        }

        String path = uri.getPath();
        String extension = com.google.common.io.Files.getFileExtension(path);
        if (getCompression(extension) != File.Compression.NONE) {
            path = com.google.common.io.Files.getNameWithoutExtension(path);
            extension = com.google.common.io.Files.getFileExtension(path);
        }


        switch (extension.toLowerCase()) {
            case "vcf":
                return File.Format.VCF;
            case "gvcf":
                return File.Format.GVCF;
            case "bcf":
                return File.Format.BCF;
            case "bam":
                return File.Format.BAM;
            case "bw":
                return File.Format.BIGWIG;
            case "bai":
                return File.Format.BAI;
            case "sam":
                return File.Format.SAM;
            case "cram":
                return File.Format.CRAM;
            case "crai":
                return File.Format.CRAI;
            case "ped":
                return File.Format.PED;
            case "fastq":
                return File.Format.FASTQ;
            case "fasta":
            case "fa":
            case "fas":
            case "fsa":
                return File.Format.FASTA;
            case "tsv":
                return File.Format.TAB_SEPARATED_VALUES;
            case "csv":
                return File.Format.COMMA_SEPARATED_VALUES;
            case "txt":
            case "log":
                return File.Format.PLAIN;
            case "xml":
                return File.Format.XML;
            case "json":
                return File.Format.JSON;
            case "proto":
                return File.Format.PROTOCOL_BUFFER;
            case "avro":
                return File.Format.AVRO;
            case "parquet":
                return File.Format.PARQUET;
            case "png":
            case "bmp":
            case "svg":
            case "gif":
            case "jpeg":
            case "tif":
                return File.Format.IMAGE;
            default:
                break;
        }

        //PLAIN
        return File.Format.UNKNOWN;
    }

    public static File.Compression detectCompression(URI uri) {
        String fileExtension = com.google.common.io.Files.getFileExtension(uri.getPath());
        return getCompression(fileExtension);
    }

    public static File.Compression getCompression(String fileExtension) {
        File.Compression compression;

        // Check if fileExtension is null
        if (fileExtension == null) {
            return File.Compression.NONE;
        }

        switch (fileExtension.toLowerCase()) {
            case "gz":
            case "gzip":
                compression = File.Compression.GZIP;
                break;
            case "zip":
                compression = File.Compression.ZIP;
                break;
            case "snappy":
            case "snz":
                compression = File.Compression.SNAPPY;
                break;
            default:
                compression = File.Compression.NONE;
                break;
        }
        return compression;
    }

}
