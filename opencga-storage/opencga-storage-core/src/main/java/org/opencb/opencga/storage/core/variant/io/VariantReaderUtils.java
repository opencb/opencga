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

package org.opencb.opencga.storage.core.variant.io;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOManagerProvider;
import org.opencb.opencga.storage.core.variant.io.avro.VariantAvroReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
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

    private final IOManagerProvider ioManagerProvider;

    public VariantReaderUtils(IOManagerProvider ioManagerProvider) {
        this.ioManagerProvider = ioManagerProvider;
    }

    /**
     * Get a variant data reader depending on the type of the input file.
     *
     * @param input Stream Input variant file (avro, json, vcf)
     * @return  VariantReader
     * @throws StorageEngineException if the format is not valid or there is an error reading
     */
    public VariantReader getVariantReader(Path input) throws StorageEngineException {
        return getVariantReader(input, null);
    }

    /**
     * Get a variant data reader depending on the type of the input file.
     *
     * @param input Stream Input variant file (avro, json, vcf)
     * @param metadata Optional VariantSource
     * @return  VariantReader
     * @throws StorageEngineException if the format is not valid or there is an error reading
     */
    public VariantReader getVariantReader(Path input, VariantStudyMetadata metadata) throws StorageEngineException {
        return getVariantReader(input, metadata, false);
    }

    /**
     * Get a variant data reader depending on the type of the input file.
     *
     * @param input Stream Input variant file (avro, json, vcf)
     * @param metadata Optional VariantSource
     * @return  VariantReader
     * @throws StorageEngineException if the format is not valid or there is an error reading
     */
    public VariantReader getVariantReader(URI input, VariantStudyMetadata metadata) throws StorageEngineException {
        return getVariantReader(input, metadata, false);
    }

    /**
     * Get a variant data reader depending on the type of the input file.
     *
     * @param input Stream Input variant file (avro, json, vcf)
     * @param metadata Optional VariantSource
     * @param stdin Indicate if the file should be read from the Standard Input
     * @return  VariantReader
     * @throws StorageEngineException if the format is not valid or there is an error reading
     */
    public VariantReader getVariantReader(Path input, VariantStudyMetadata metadata, boolean stdin) throws StorageEngineException {
        return getVariantReader(input.toUri(), metadata, stdin);
    }

    /**
     * Get a variant data reader depending on the type of the input file.
     *
     * @param input Stream Input variant file (avro, json, vcf)
     * @param metadata Optional VariantSource
     * @param stdin Indicate if the file should be read from the Standard Input
     * @return  VariantReader
     * @throws StorageEngineException if the format is not valid or there is an error reading
     */
    public VariantReader getVariantReader(URI input, VariantStudyMetadata metadata, boolean stdin) throws StorageEngineException {
        String fileName = input.getPath();
        if (metadata == null) {
            VariantFileMetadata variantFileMetadata = createEmptyVariantFileMetadata(input);
            metadata = variantFileMetadata.toVariantStudyMetadata("");
        }
        if (isJson(fileName)) {
            return getVariantJsonReader(input, metadata);
        } else if (isAvro(fileName)) {
            return getVariantAvroReader(input, metadata, stdin);
        } else if (isVcf(fileName)) {
            return getVariantVcfReader(input, metadata, stdin);
        } else {
            throw variantInputNotSupported(input);
        }
    }

    public static StorageEngineException variantInputNotSupported(URI input) {
        return new StorageEngineException("Variants input file format not supported for file: " + input);
    }

    protected VariantJsonReader getVariantJsonReader(URI input, VariantStudyMetadata metadata) throws StorageEngineException {
        VariantJsonReader variantJsonReader;
        if (isJson(input.toString())) {
            URI metaFile = getMetaFromTransformedFile(input);
            variantJsonReader = new VariantJsonReader(metadata, newInputStream(input), newInputStream(metaFile));
        } else {
            throw variantInputNotSupported(input);
        }
        return variantJsonReader;
    }

    protected VariantAvroReader getVariantAvroReader(URI input, VariantStudyMetadata metadata, boolean stdin)
            throws StorageEngineException {
        VariantAvroReader variantAvroReader;
        if (isAvro(input.toString())) {
            URI sourceFile = getMetaFromTransformedFile(input);
            if (stdin) {
                variantAvroReader = new VariantAvroReader(System.in, newInputStream(sourceFile), metadata);
            } else {
                variantAvroReader = new VariantAvroReader(newInputStream(input), newInputStream(sourceFile), metadata);
            }
        } else {
            throw variantInputNotSupported(input);
        }
        return variantAvroReader;
    }

    public VariantVcfHtsjdkReader getVariantVcfReader(Path input, VariantStudyMetadata metadata) throws StorageEngineException {
        return getVariantVcfReader(input, metadata, false);
    }

    public VariantVcfHtsjdkReader getVariantVcfReader(URI input, VariantStudyMetadata metadata) throws StorageEngineException {
        return getVariantVcfReader(input, metadata, false);
    }

    public VariantVcfHtsjdkReader getVariantVcfReader(Path input, VariantStudyMetadata metadata, boolean stdin)
            throws StorageEngineException {
        return getVariantVcfReader(input.toUri(), metadata, stdin);
    }

    public VariantVcfHtsjdkReader getVariantVcfReader(URI input, VariantStudyMetadata metadata, boolean stdin)
            throws StorageEngineException {
        if (metadata == null) {
            VariantFileMetadata variantFileMetadata = createEmptyVariantFileMetadata(input);
            metadata = variantFileMetadata.toVariantStudyMetadata("");
        }
        if (stdin) {
            return new VariantVcfHtsjdkReader(System.in, metadata);
        } else {
            return new VariantVcfHtsjdkReader(newInputStream(input), metadata);
        }
    }

    public static URI getMetaFromTransformedFile(URI variantsFile) {
        return UriUtils.replacePath(variantsFile, getMetaFromTransformedFile(variantsFile.getPath()));
    }

    public static String getMetaFromTransformedFile(String variantsFile) {
        checkTransformedVariants(variantsFile);
        int idx = variantsFile.lastIndexOf(VARIANTS_FILE);
        return new StringBuilder().append(variantsFile, 0, idx).append(METADATA_FILE_FORMAT_GZ).toString();
    }

    public static String getFileName(URI uri) {
        return UriUtils.fileName(uri);
    }

    public static String getOriginalFromTransformedFile(URI input) {
        return getOriginalFromTransformedFile(getFileName(input));
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
    public static VariantFileMetadata readVariantFileMetadataFromJson(InputStream inputStream) throws IOException {
        org.opencb.biodata.models.variant.metadata.VariantFileMetadata metadata = new ObjectMapper()
                .configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true)
                .readValue(inputStream, org.opencb.biodata.models.variant.metadata.VariantFileMetadata.class);
        return new VariantFileMetadata(metadata);

    }

    public VariantFileMetadata readVariantFileMetadata(URI input) throws StorageEngineException {
        return readVariantFileMetadata(input, null, false);
    }

    /**
     * Read the {@link VariantFileMetadata} from a variant file.
     *
     * Accepted formats: Avro, Json and VCF
     *
     * @param input Input variant file (avro, json, vcf)
     * @return Read {@link VariantFileMetadata}
     * @throws StorageEngineException if the format is not valid or there is an error reading
     */
    public VariantFileMetadata readVariantFileMetadata(Path input) throws StorageEngineException {
        return readVariantFileMetadata(input, null);
    }

    /**
     * Read the {@link VariantFileMetadata} from a variant file.
     *
     * Accepted formats: Avro, Json and VCF
     *
     * @param input Input variant file (avro, json, vcf)
     * @param metadata {@link VariantFileMetadata} to fill. Can be null
     * @return Read {@link VariantFileMetadata}
     * @throws StorageEngineException if the format is not valid or there is an error reading
     */
    public VariantFileMetadata readVariantFileMetadata(Path input, VariantFileMetadata metadata) throws StorageEngineException {
        return readVariantFileMetadata(input, metadata, false);
    }

    /**
     * Read the {@link VariantFileMetadata} from a variant file.
     *
     * Accepted formats: Avro, Json and VCF
     *
     * @param input Input variant file (avro, json, vcf)
     * @param metadata {@link VariantFileMetadata} to fill. Can be null
     * @param stdin Indicate if the file should be read from the Standard Input
     * @return Read {@link VariantFileMetadata}
     * @throws StorageEngineException if the format is not valid or there is an error reading
     */
    public VariantFileMetadata readVariantFileMetadata(Path input, VariantFileMetadata metadata, boolean stdin)
            throws StorageEngineException {
        return readVariantFileMetadata(input.toUri(), metadata, stdin);
    }

    /**
     * Read the {@link VariantFileMetadata} from a variant file.
     *
     * Accepted formats: Avro, Json and VCF
     *
     * @param input Input variant file (avro, json, vcf)
     * @param metadata {@link VariantFileMetadata} to fill. Can be null
     * @param stdin Indicate if the file should be read from the Standard Input
     * @return Read {@link VariantFileMetadata}
     * @throws StorageEngineException if the format is not valid or there is an error reading
     */
    public VariantFileMetadata readVariantFileMetadata(URI input, VariantFileMetadata metadata, boolean stdin)
            throws StorageEngineException {
        if (metadata == null) {
            metadata = createEmptyVariantFileMetadata(input);
        }

        if (isTransformedVariants(input.toString())) {
            input = getMetaFromTransformedFile(input);
        }
        // If it's a sourceFile
        if (isMetaFile(input.toString())) {
            try (InputStream inputStream = newInputStream(input)) {
                return readVariantFileMetadataFromJson(inputStream);
            } catch (IOException | RuntimeException e) {
                throw new StorageEngineException("Unable to read VariantSource", e);
            }
        }
        if (stdin) {
            markStdin();
        }
        VariantReader reader = getVariantReader(input, metadata.toVariantStudyMetadata(""), stdin);
        try {
            metadata = VariantMetadataUtils.readVariantFileMetadata(reader, metadata);
        } catch (IOException e) {
            throw new StorageEngineException("Unable to read VariantSource", e);
        }
        if (stdin) {
            resetStdin();
        }
        return metadata;
    }

    public Pair<VCFHeader, VCFHeaderVersion> readHtsHeader(URI input, boolean stdin) throws StorageEngineException {
        if (stdin) {
            markStdin();
        }
        VariantVcfHtsjdkReader vcfReader = getVariantVcfReader(input, null, stdin);
        vcfReader.open();
        vcfReader.pre();
        VCFHeader vcfHeader = vcfReader.getVCFHeader();
        VCFHeaderVersion version = vcfReader.getVCFHeaderVersion();
        vcfReader.close();

        if (stdin) {
            resetStdin();
        }
        return Pair.of(vcfHeader, version);
    }

    public static VariantFileMetadata createEmptyVariantFileMetadata(Path input) {
        return new VariantFileMetadata(input.getFileName().toString(), input.toAbsolutePath().toString());
    }

    public static VariantFileMetadata createEmptyVariantFileMetadata(URI input) {
        return new VariantFileMetadata(UriUtils.fileName(input), input.getPath());
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
        return hasFormat(fileName, "vcf") || hasFormat(fileName, "gvcf");
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

    /**
     * Mark current point in the StandardInput to reset later on.
     */
    private static void markStdin() {
        if (!System.in.markSupported()) {
            System.setIn(new BufferedInputStream(System.in));
        }
        System.in.mark(10 * 1024 * 1024); //10MB
    }

    /**
     * Reposition the stdin stream.
     *
     * Must be called after {@link VariantReaderUtils#markStdin()}
     *
     * @throws StorageEngineException If the stdin was not marked, or more than 10MB were read.
     */
    private static void resetStdin() throws StorageEngineException {
        try {
            System.in.reset();
        } catch (IOException e) {
            throw new StorageEngineException("Error resetting stdin", e);
        }
    }

    private InputStream newInputStream(URI input) throws StorageEngineException {
        try {
            return ioManagerProvider.newInputStream(input);
        } catch (IOException e) {
            throw StorageEngineException.ioException(input.toString(), e);
        }
    }

    public IOManagerProvider getIOManagerProvider() {
        return ioManagerProvider;
    }
}
