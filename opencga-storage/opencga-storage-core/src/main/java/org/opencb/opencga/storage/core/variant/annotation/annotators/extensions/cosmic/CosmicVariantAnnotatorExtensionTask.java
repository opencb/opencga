package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.solr.common.StringUtils;
import org.opencb.biodata.formats.variant.cosmic.CosmicParser101;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
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
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class CosmicVariantAnnotatorExtensionTask implements VariantAnnotatorExtensionTask {

    public static final String ID = "cosmic";
    public static final String COSMIC_VERSION_KEY = "version";
    public static final String COSMIC_ASSEMBLY_KEY = "assembly";

    private String cosmicVersion;
    private String cosmicAssembly;
    private String cosmicIndexCreationDate;

    private ObjectReader objectReader;

    private RocksDB rdb = null;
    private Options dbOption = null;
    private Path dbLocation = null;

    public static final String COSMIC_ANNOTATOR_INDEX_NAME = "cosmicAnnotatorIndex";

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

    public List<URI> setup(VariantAnnotationExtensionConfigureParams configureParams, URI outDir) throws Exception {
        // Sanity check
        if (!ID.equals(configureParams.getExtension())) {
            throw new ToolException("Invalid COSMIC extension: " + configureParams.getExtension());
        }

        // Check COSMIC file
        Path cosmicFile = Paths.get(configureParams.getResources().get(0));
        logger.info("COSMIC file {}", cosmicFile.toAbsolutePath());
        FileUtils.checkFile(cosmicFile);
        if (!cosmicFile.getFileName().toString().endsWith(".tar.gz")) {
            throw new ToolException("Invalid COSMIC file format '" + cosmicFile.getFileName() + "': it must be a .tar.gz file");
        }

        Path outDirPath = Paths.get(outDir);
        FileUtils.checkDirectory(outDirPath, true);

        // Check COSMIC version and assembly
        this.cosmicVersion = configureParams.getParams().getString(COSMIC_VERSION_KEY);
        if (StringUtils.isEmpty(cosmicVersion)) {
            throw new ToolException("Missing COSMIC version");
        }
        this.cosmicAssembly = configureParams.getParams().getString(COSMIC_ASSEMBLY_KEY);
        if (StringUtils.isEmpty(cosmicAssembly)) {
            throw new ToolException("Missing COSMIC assembly");
        }
        this.cosmicIndexCreationDate = Instant.now().toString();

        // Clean and init RocksDB
        dbLocation = outDirPath.resolve(COSMIC_ANNOTATOR_INDEX_NAME);
        if (Files.exists(dbLocation)) {
            // Skipping setup but init RocksDB
            logger.info("Skipping setup, it was already done");
            initRockDB(false);
        } else {
            // Check and decompress tarball, checking the COSMIC files: GenomeScreensMutant and Classification
            Path genomeScreensMutantFile = null;
            Path classificationFile = null;

            Path tmpPath = outDirPath.resolve("tmp");
            decompressTarBall(cosmicFile, tmpPath);
            if (tmpPath.toFile().listFiles() != null) {
                for (File file : tmpPath.toFile().listFiles()) {
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

            logger.info("Setup and populate RocksDB by parsing COSMIC files (version {}, assembly {})", cosmicVersion, cosmicAssembly);

            // Init RocksDB
            initRockDB(true);

            // Call COSMIC parser
            try {
                CosmicExtensionTaskCallback callback = new CosmicExtensionTaskCallback(rdb);
                CosmicParser101.parse(genomeScreensMutantFile, classificationFile, cosmicVersion, ID, cosmicAssembly, callback);
            } catch (IOException e) {
                throw new ToolException(e);
            }
        }

        return Collections.singletonList(dbLocation.toUri());
    }

    @Override
    public void checkAvailable() throws IllegalArgumentException {
        if (dbLocation == null || !Files.exists(dbLocation)
                || !dbLocation.toAbsolutePath().toString().endsWith(COSMIC_ANNOTATOR_INDEX_NAME)) {
            throw new IllegalArgumentException("COSMIC annotator extension is not available (dbLocation = " + dbLocation + ")");
        }
    }

    @Override
    public ObjectMap getOptions() {
        checkAvailable();
        ObjectMap options = new ObjectMap()
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key(), dbLocation.toAbsolutePath().toString())
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key(), cosmicVersion)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key(), cosmicAssembly)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_INDEX_CREATION_DATE.key(), cosmicIndexCreationDate);
        return options;
    }

    @Override
    public ObjectMap getMetadata() {
        checkAvailable();
        ObjectMap metadata = new ObjectMap().append("name", ID)
                .append("version", cosmicVersion)
                .append("assembly", cosmicAssembly)
                .append("indexCreationDate", cosmicIndexCreationDate);
        return metadata;
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
        }
        if (dbOption != null) {
            dbOption.dispose();
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
