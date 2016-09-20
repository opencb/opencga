package org.opencb.opencga.storage.core.variant.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.tools.variant.VariantFileUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.io.avro.VariantAvroReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * Created on 31/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantReaderUtils {

    public static final String MALFORMED_FILE = "malformed";
    public static final String VARIANTS_FILE = "variants";
    public static final String METADATA_FILE = "file";
    public static final String METADATA_FORMAT = "json";
    public static final String METADATA_FILE_FORMAT_GZ = METADATA_FILE + "." + METADATA_FORMAT + ".gz";

    private static final Pattern VALID_META = Pattern.compile("^.+\\." + METADATA_FILE + "\\." + METADATA_FORMAT + "\\.gz$");
    private static final Pattern VALID_VARIANTS = Pattern.compile("^.+\\." + VARIANTS_FILE + "\\.(avro|json|proto)(\\.(gz|snappy))?$");

    /**
     * Get a variant data reader depending on the type of the input file.
     *
     * @param input Stream Input variant file (avro, json, vcf)
     * @param source Optional VariantSource
     * @return  VariantReader
     * @throws StorageManagerException if the format is not valid or there is an error reading
     */
    public static VariantReader getVariantReader(Path input, VariantSource source) throws StorageManagerException {
        String fileName = input.getFileName().toString();
        if (isJson(fileName)) {
            return getVariantJsonReader(input, source);
        } else if (isAvro(fileName)) {
            return getVariantAvroReader(input, source);
        } else if (isVcf(fileName)) {
            return new VariantVcfReader(source, input.toAbsolutePath().toString());
        } else {
            throw variantInputNotSupported(input);
        }
    }

    public static StorageManagerException variantInputNotSupported(Path input) {
        return new StorageManagerException("Variants input file format not supported for file: " + input);
    }

    protected static VariantJsonReader getVariantJsonReader(Path input, VariantSource source) throws StorageManagerException {
        VariantJsonReader variantJsonReader;
        if (isJson(input.toString())) {
            String sourceFile = getMetaFromTransformedFile(input.toAbsolutePath().toString());
            variantJsonReader = new VariantJsonReader(source, input.toAbsolutePath().toString(), sourceFile);
        } else {
            throw variantInputNotSupported(input);
        }
        return variantJsonReader;
    }

    protected static VariantAvroReader getVariantAvroReader(Path input, VariantSource source) throws StorageManagerException {
        VariantAvroReader variantAvroReader;
        if (isAvro(input.toString())) {
            String sourceFile = getMetaFromTransformedFile(input.toAbsolutePath().toString());
            variantAvroReader = new VariantAvroReader(input.toAbsolutePath().toFile(), new File(sourceFile), source);
        } else {
            throw variantInputNotSupported(input);
        }
        return variantAvroReader;
    }

    public static Path getMetaFromTransformedFile(Path variantsFile) {
        return Paths.get(getMetaFromTransformedFile(variantsFile.toString()));
    }

    public static String getMetaFromTransformedFile(String variantsFile) {
        checkTransformedVariants(variantsFile);
        int idx = variantsFile.indexOf(VARIANTS_FILE);
        return new StringBuilder().append(variantsFile, 0, idx).append(METADATA_FILE_FORMAT_GZ).toString();
    }

    public static String getOriginalFromTransformedFile(String variantsFile) {
        if (isTransformedVariants(variantsFile)) {
            int idx = variantsFile.lastIndexOf(VARIANTS_FILE);
            return variantsFile.substring(0, idx - 1);
        } else if (isMetaFile(variantsFile)) {
            int idx = variantsFile.lastIndexOf(METADATA_FILE);
            return variantsFile.substring(0, idx - 1);
        } else {
            return variantsFile;
        }
    }

    /**
     * Read the VariantSource from an InputStream.
     *
     * InputStream must point to a json object.
     *
     * @param inputStream Input variant source file
     * @return Read VariantSource
     * @throws IOException if there is an error reading
     */
    public static VariantSource readVariantSource(InputStream inputStream) throws IOException {
        VariantSource source;
        source = new ObjectMapper().readValue(inputStream, VariantSource.class);
        return source;
    }

    public VariantSource readVariantSource(URI input) throws StorageManagerException {
        if (input.getScheme() == null || input.getScheme().startsWith("file")) {
            return readVariantSource(Paths.get(input.getPath()), null);
        } else {
            throw new StorageManagerException("Can not read files from " + input.getScheme());
        }
    }

    /**
     * Read the VariantSource from a variant file.
     *
     * Accepted formats: Avro, Json and VCF
     *
     * @param input Input variant file (avro, json, vcf)
     * @param source VariantSource to fill. Can be null
     * @return Read VariantSource
     * @throws StorageManagerException if the format is not valid or there is an error reading
     */
    public static VariantSource readVariantSource(Path input, VariantSource source) throws StorageManagerException {
        if (source == null) {
            source = new VariantSource(input.getFileName().toString(), "", "", "");
        }

        // If it's a sourceFile
        if (input.toString().endsWith(METADATA_FILE_FORMAT_GZ)) {
            try (InputStream inputStream = FileUtils.newInputStream(input)) {
                return VariantReaderUtils.readVariantSource(inputStream);
            } catch (IOException | RuntimeException e) {
                throw new StorageManagerException("Unable to read VariantSource", e);
            }
        }

        VariantReader reader = getVariantReader(input, source);
        try {
            source = VariantFileUtils.readVariantSource(reader, source);
        } catch (IOException e) {
            throw new StorageManagerException("Unable to read VariantSource", e);
        }

        return source;
    }

    public static boolean isAvro(String fileName) {
        return hasFormat(fileName, "avro");
    }

    public static boolean isProto(String fileName) {
        return hasFormat(fileName, "proto");
    }

    public static boolean isJson(String fileName) {
        return hasFormat(fileName, "json");
    }

    public static boolean isVcf(String fileName) {
        return hasFormat(fileName, "vcf");
    }

    public static boolean hasFormat(String fileName, String format) {
        if (fileName.endsWith("." + format)) {
            return true;
        } else if (fileName.contains(".")) {
            return fileName.substring(0, fileName.lastIndexOf('.')).endsWith("." + format);
        }
        return false;
    }

    public static void checkTransformedVariants(String file) {
        if (!isTransformedVariants(file)) {
            throw new IllegalArgumentException("Not a valid transformed variants file : " + file);
        }
    }

    public static boolean isTransformedVariants(String file) {
        return VALID_VARIANTS.matcher(file).find();
    }

    public static void checkMetaFile(String file) {
        if (!isMetaFile(file)) {
            throw new IllegalArgumentException("Not a valid transformed variants metadata file : " + file);
        }
    }

    public static boolean isMetaFile(String file) {
        return VALID_META.matcher(file).find();
    }

}
