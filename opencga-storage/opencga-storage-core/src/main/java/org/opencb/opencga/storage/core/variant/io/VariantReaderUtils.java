package org.opencb.opencga.storage.core.variant.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.tools.variant.VariantFileUtils;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.io.avro.VariantAvroReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

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
    public static final String METADATA_FILE_FORMAT = METADATA_FILE + "." + METADATA_FORMAT;

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
        if (fileName.contains("json")) {
            return getVariantJsonReader(input, source);
        } else if (fileName.contains("avro")) {
            return getVariantAvroReader(input, source);
        } else if (fileName.endsWith("vcf") || fileName.endsWith("vcf.gz")) {
            return new VariantVcfReader(source, input.toAbsolutePath().toString());
        } else {
            throw new StorageManagerException("Variants input file format not supported for file: " + input);
        }
    }

    protected static VariantJsonReader getVariantJsonReader(Path input, VariantSource source) throws StorageManagerException {
        VariantJsonReader variantJsonReader;
        if (input.toString().endsWith(".json") || input.toString().endsWith(".json.gz")
                || input.toString().endsWith(".json.snappy") || input.toString().endsWith(".json.snz")) {
            String sourceFile = input.toAbsolutePath().toString().replace(VARIANTS_FILE + ".json", METADATA_FILE_FORMAT);
            variantJsonReader = new VariantJsonReader(source, input.toAbsolutePath().toString(), sourceFile);
        } else {
            throw new StorageManagerException("Variants input file format not supported for file: " + input);
        }
        return variantJsonReader;
    }

    protected static VariantAvroReader getVariantAvroReader(Path input, VariantSource source) throws StorageManagerException {
        VariantAvroReader variantAvroReader;
        if (input.toString().matches(".*avro(\\..*)?$")) {
            String sourceFile = input.toAbsolutePath().toString().replace(VARIANTS_FILE + ".avro", METADATA_FILE_FORMAT);
            variantAvroReader = new VariantAvroReader(input.toAbsolutePath().toFile(), new File(sourceFile), source);
        } else {
            throw new StorageManagerException("Variants input file format not supported for file: " + input);
        }
        return variantAvroReader;
    }

    public static String getMetaFromInputFile(String input) {
        return input.replace(VARIANTS_FILE + ".", METADATA_FILE + ".")
                .replace(METADATA_FILE + ".avro", METADATA_FILE_FORMAT)
                .replace(METADATA_FILE + ".proto", METADATA_FILE_FORMAT);
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
            source = new VariantSource("", "", "", "");
        }

        // If it's a sourceFile
        if (input.toString().endsWith(METADATA_FILE_FORMAT_GZ) || input.toString().endsWith(METADATA_FILE_FORMAT)) {
            boolean gzip = input.toString().endsWith(METADATA_FILE_FORMAT_GZ);
            try (InputStream inputStream = gzip
                    ? new GZIPInputStream(new FileInputStream(input.toFile()))
                    : new FileInputStream(input.toFile())) {
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

}
