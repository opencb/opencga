package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.solr.common.StringUtils;
import org.opencb.biodata.formats.variant.cosmic.CosmicParser101;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationExtensionConfigureParams;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.VariantAnnotatorExtensionTask;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.ASSEMBLY;

public class CosmicVariantAnnotatorExtensionTask implements VariantAnnotatorExtensionTask {

    public static final String ID = "cosmic";
    public static final String COSMIC_VERSION_KEY = "version";
    public static final String COSMIC_ASSEMBLY_KEY = "assembly";
    public static final String COSMIC_INDEX_CREATION_DATE_KEY = "indexCreationDate";

    public static final String COSMIC_ANNOTATOR_INDEX_SUFFIX = "-INDEX";
    public static final String COSMIC_ANNOTATOR_CONFIG_FILENAME = ID + "-config.json";

    private String cosmicVersion;
    private String cosmicAssembly;
    private String cosmicIndexCreationDate;

    private ObjectReader objectReader;

    private RocksDB rdb = null;
    private Options dbOption = null;
    private Path dbLocation = null;


    private static final String VARIANT_STRING_PATTERN = "([ACGTN]*)|(<CNV[0-9]+>)|(<DUP>)|(<DEL>)|(<INS>)|(<INV>)";

    private static Logger logger = LoggerFactory.getLogger(CosmicVariantAnnotatorExtensionTask.class);

    public CosmicVariantAnnotatorExtensionTask(ObjectMap options) {
        if (MapUtils.isNotEmpty(options)) {
            if (options.containsKey(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key())) {
                this.dbLocation = Paths.get(options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key()));
            }
            if (options.containsKey(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key())) {
                this.cosmicVersion = options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key());
            }
            if (options.containsKey(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key())) {
                this.cosmicAssembly = options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key());
            }
            if (options.containsKey(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_INDEX_CREATION_DATE.key())) {
                this.cosmicIndexCreationDate =
                        options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_INDEX_CREATION_DATE.key());
            }
        }

        this.objectReader = JacksonUtils.getDefaultObjectMapper().readerFor(new TypeReference<List<EvidenceEntry>>() {});
    }

    public ObjectMap setup(VariantAnnotationExtensionConfigureParams configureParams, URI outDir) throws Exception {
        ObjectMapper defaultObjectMapper = JacksonUtils.getDefaultObjectMapper();

        // Sanity check
        if (!ID.equals(configureParams.getExtension())) {
            throw new ToolException("Invalid COSMIC extension: " + configureParams.getExtension());
        }

        // Check COSMIC version and assembly
        String tmpCosmicVersion = configureParams.getParams().getString(COSMIC_VERSION_KEY);
        if (StringUtils.isEmpty(tmpCosmicVersion)) {
            throw new ToolException("Missing COSMIC version");
        }
        String tmpCosmicAssembly = configureParams.getParams().getString(COSMIC_ASSEMBLY_KEY);
        if (StringUtils.isEmpty(tmpCosmicAssembly)) {
            throw new ToolException("Missing COSMIC assembly");
        }

        // Check COSMIC file
        Path cosmicFile = Paths.get(configureParams.getResources().get(0));
        logger.info("COSMIC file {}", cosmicFile.toAbsolutePath());
        FileUtils.checkFile(cosmicFile);
        if (!cosmicFile.getFileName().toString().endsWith(".tar.gz")) {
            throw new ToolException("Invalid COSMIC file format '" + cosmicFile.getFileName() + "': it must be a .tar.gz file");
        }

        Path outDirPath = cosmicFile.getParent();
        FileUtils.checkDirectory(outDirPath, true);

        Path cosmicConfigFile = outDirPath.resolve(COSMIC_ANNOTATOR_CONFIG_FILENAME);
        Path tmpDbLocation = outDirPath.resolve(cosmicFile.getFileName() + COSMIC_ANNOTATOR_INDEX_SUFFIX);
        boolean overwrite = configureParams.getOverwrite() == null ? false : configureParams.getOverwrite();
        if (Files.exists(cosmicConfigFile) && Files.exists(tmpDbLocation) && !overwrite) {
            // Load existing config file
            VariantAnnotationExtensionConfigureParams previousParams = defaultObjectMapper.readerFor(
                    VariantAnnotationExtensionConfigureParams.class).readValue(cosmicConfigFile.toFile());

            if (!cosmicFile.toAbsolutePath().toString().equals(previousParams.getResources().get(0))) {
                throw new ToolException("COSMIC file '" + cosmicFile + "' does not match the existing config file version '"
                        + previousParams.getResources().get(0) + "'. Use the overwrite flag to force the update.");
            }

            if (!tmpCosmicVersion.equals(previousParams.getParams().get(COSMIC_VERSION_KEY))) {
                throw new ToolException("COSMIC version '" + tmpCosmicVersion + "' does not match the existing config file version '"
                        + previousParams.getParams().get(COSMIC_VERSION_KEY) + "'. Use the overwrite flag to force the update.");
            }

            if (!tmpCosmicAssembly.equals(previousParams.getParams().get(COSMIC_ASSEMBLY_KEY))) {
                throw new ToolException("COSMIC assembly '" + tmpCosmicAssembly + "' does not match the existing config file version '"
                        + previousParams.getParams().get(COSMIC_ASSEMBLY_KEY) + "'. Use the overwrite flag to force the update.");
            }

            // Skipping setup but init RocksDB
            logger.info("Skipping setup since it was already done and overwrite is not set to true");
            dbLocation = tmpDbLocation;
            cosmicVersion = tmpCosmicVersion;
            cosmicAssembly = tmpCosmicAssembly;
            cosmicIndexCreationDate = previousParams.getParams().getString(COSMIC_INDEX_CREATION_DATE_KEY);

//            initRockDB(false);

            return getOptions();
        }


        String tmpCosmicIndexCreationDate = Instant.now().toString();

        // Check and decompress tarball, checking the COSMIC files: GenomeScreensMutant and Classification
        Path genomeScreensMutantFile = null;
        Path classificationFile = null;

        Path tmpPath = outDirPath.resolve("tmp");
        decompressTarBall(cosmicFile, tmpPath);
        File[] files = tmpPath.toFile().listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().contains("Classification")) {
                    classificationFile = file.toPath();
                } else if (file.getName().contains("GenomeScreensMutant")) {
                    genomeScreensMutantFile = file.toPath();
                }
            }
        }
        if (genomeScreensMutantFile == null) {
            throw new ToolException("Missing the GenomeScreensMutant file in the COSMIC tarball '" + cosmicFile.getFileName() + "'");
        }
        if (classificationFile == null) {
            throw new ToolException("Missing the Classification file in the COSMIC tarball '" + cosmicFile.getFileName() + "'");
        }

        logger.info("Setup and populate RocksDB by parsing COSMIC files (version {}, assembly {})", tmpCosmicVersion, tmpCosmicAssembly);

        // Init RocksDB
        dbLocation = tmpDbLocation;
        initRockDB(true);

        // Call COSMIC parser
        try {
            ProgressLogger progressLogger = new ProgressLogger("Preparing RocksDB for Cosmic");
            progressLogger.setBatchSize(10000);
            CosmicExtensionTaskCallback callback = new CosmicExtensionTaskCallback(rdb, progressLogger);
            CosmicParser101.parse(genomeScreensMutantFile, classificationFile, tmpCosmicVersion, ID, tmpCosmicAssembly, callback);
        } catch (IOException e) {
            throw new ToolException(e);
        }

        // Close RocksDB
        closeRocksDB();

        cosmicVersion = tmpCosmicVersion;
        cosmicAssembly = tmpCosmicAssembly;
        cosmicIndexCreationDate = tmpCosmicIndexCreationDate;

        VariantAnnotationExtensionConfigureParams newParams = new VariantAnnotationExtensionConfigureParams(configureParams);
        newParams.getParams().append(COSMIC_INDEX_CREATION_DATE_KEY, cosmicIndexCreationDate);
        defaultObjectMapper.writeValue(cosmicConfigFile.toFile(), newParams);

        // Remove temporary files
        try {
            logger.info("Removing temporary files");
            for (File file : files) {
                Files.deleteIfExists(file.toPath());
            }
            Files.delete(tmpPath);
        } catch (IOException e) {
            logger.warn("Error deleting temporary files", e);
        }


        return getOptions();
    }

    @Override
    public void check(ObjectMap options) throws IllegalArgumentException {
        if (dbLocation == null || StringUtils.isEmpty(dbLocation.toString())) {
            throw new IllegalArgumentException("Missing COSMIC file");
        }
        if (StringUtils.isEmpty(cosmicVersion)) {
            throw new IllegalArgumentException("Missing COSMIC version");
        }
        if (StringUtils.isEmpty(cosmicAssembly)) {
            throw new IllegalArgumentException("Missing COSMIC assembly");
        }

        if (MapUtils.isNotEmpty(options) && options.containsKey(ASSEMBLY.key())
                && !options.getString(ASSEMBLY.key()).equalsIgnoreCase(cosmicAssembly)) {
            throw new IllegalArgumentException("COSMIC assembly '" + cosmicAssembly + "' does not match the variant storage assembly '"
                    + options.getString(ASSEMBLY.key()) + "'");
        }
    }

    @Override
    public void checkAvailable() throws IllegalArgumentException {
        check(null);
        if (dbLocation == null || !Files.exists(dbLocation)
                || !dbLocation.toAbsolutePath().toString().endsWith(COSMIC_ANNOTATOR_INDEX_SUFFIX)) {
            throw new IllegalArgumentException("COSMIC annotator extension is not available (dbLocation = " + dbLocation + ")");
        }
    }

    @Override
    public ObjectMap getOptions() {
        checkAvailable();
        return new ObjectMap()
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key(), dbLocation.toAbsolutePath().toString())
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key(), cosmicVersion)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key(), cosmicAssembly)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_INDEX_CREATION_DATE.key(), cosmicIndexCreationDate);
    }

    @Override
    public ObjectMap getMetadata() {
        checkAvailable();
        return new ObjectMap()
                .append(NAME_KEY, ID)
                .append(COSMIC_VERSION_KEY, cosmicVersion)
                .append(COSMIC_ASSEMBLY_KEY, cosmicAssembly)
                .append(INDEX_CREATION_DATE_KEY, cosmicIndexCreationDate);
    }

    @Override
    public List<VariantAnnotation> apply(List<VariantAnnotation> list) throws Exception {
        for (VariantAnnotation variantAnnotation : list) {
            String reference = checkEmptySequence(variantAnnotation.getReference());
            String alternate = checkEmptySequence(variantAnnotation.getAlternate());
            if (!isValid(reference, alternate)) {
                logger.warn("Skipping invalid variant: chromosome = {}, start = {}, reference = {}, alternate = {}",
                        variantAnnotation.getChromosome(), variantAnnotation.getStart(), variantAnnotation.getReference(),
                        variantAnnotation.getAlternate());
                continue;
            }

            Variant variant;
            try {
                variant = new Variant(variantAnnotation.getChromosome(), variantAnnotation.getStart(), reference, alternate);
            } catch (Exception e) {
                logger.warn("Skipping variant: it could not be built", e);
                continue;
            }

            byte[] key = variant.toString().getBytes();
            byte[] dbContent = rdb.get(key);
            if (dbContent != null) {
                List<EvidenceEntry> evidenceEntryList = objectReader.readValue(dbContent);
                if (variantAnnotation.getTraitAssociation() == null) {
                    variantAnnotation.setTraitAssociation(evidenceEntryList);
                } else {
                    variantAnnotation.getTraitAssociation().addAll(evidenceEntryList);
                }
            }
        }
        return list;
    }

    @Override
    public void pre() throws Exception {
        if (rdb == null) {
            initRockDB(false);
        }
    }

    @Override
    public void post() throws Exception {
        closeRocksDB();
    }

    @Override
    public String getId() {
        return ID;
    }

    private void closeRocksDB() {
        if (rdb != null) {
            rdb.close();
            rdb = null;
        }
        if (dbOption != null) {
            dbOption.close();
            dbOption = null;
        }
    }

    private void initRockDB(boolean forceCreate) throws ToolException {
        boolean indexingNeeded = forceCreate || !Files.exists(dbLocation);
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        dbOption = new Options().setCreateIfMissing(true);

        rdb = null;
        try {
            // a factory method that returns a RocksDB instance
            if (indexingNeeded) {
                rdb = RocksDB.open(dbOption, dbLocation.toAbsolutePath().toString());
            } else {
                rdb = RocksDB.openReadOnly(dbOption, dbLocation.toAbsolutePath().toString());
            }
        } catch (RocksDBException e) {
            throw new ToolException("Error initializing RocksDB", e);
        }
    }

    protected void decompressTarBall(Path file, Path outputPath) throws IOException {
        byte[] buffer = new byte[1024];

        if (!Files.exists(outputPath)) {
            Files.createDirectories(outputPath);
        }

        try (FileInputStream fis = new FileInputStream(file.toFile());
             GZIPInputStream gis = new GZIPInputStream(fis);
             TarArchiveInputStream tarInput = new TarArchiveInputStream(gis)) {

            TarArchiveEntry entry;
            while ((entry = tarInput.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    Files.createDirectories(outputPath.resolve(entry.getName()));
                } else {
                    Path entryOutputPath = outputPath.resolve(entry.getName());
                    try (FileOutputStream fos = new FileOutputStream(entryOutputPath.toFile())) {
                        int len;
                        while ((len = tarInput.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
            }
        }
    }

    private static String checkEmptySequence(String sequence) {
        return sequence != null && !sequence.equals("-") ? sequence : "";
    }

    private boolean isValid(String reference, String alternate) {
        return (reference.matches(VARIANT_STRING_PATTERN)
                && (alternate.matches(VARIANT_STRING_PATTERN)
                && !alternate.equals(reference)));
    }
}
