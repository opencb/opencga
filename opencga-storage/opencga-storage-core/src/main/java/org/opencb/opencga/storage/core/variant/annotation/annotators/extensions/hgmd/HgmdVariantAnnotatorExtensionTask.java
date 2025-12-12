package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.hgmd;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.solr.common.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.exceptions.NonStandardCompliantSampleField;
import org.opencb.biodata.tools.variant.VariantNormalizer;
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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.ASSEMBLY;

public class HgmdVariantAnnotatorExtensionTask implements VariantAnnotatorExtensionTask {

    public static final String ID = "hgmd";
    public static final String HGMD_VERSION_KEY = "version";
    public static final String HGMD_ASSEMBLY_KEY = "assembly";
    public static final String HGMD_INDEX_CREATION_DATE_KEY = "indexCreationDate";

    public static final String HGMD_ANNOTATOR_INDEX_SUFFIX = "-INDEX";
    public static final String HGMD_ANNOTATOR_CONFIG_FILENAME = ID + "-config.json";

    private String hgmdVersion;
    private String hgmdAssembly;
    private String hgmdIndexCreationDate;

    private final ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
    private final ObjectReader objectReader = objectMapper.readerFor(new TypeReference<List<EvidenceEntry>>() {});
    private final ObjectWriter objectWriter = objectMapper.writerFor(new TypeReference<List<EvidenceEntry>>() {});

    private RocksDB rdb = null;
    private Options dbOption = null;
    private Path dbLocation = null;

    private int numberIndexedRecords = 0;
    private int totalNumberRecords = 0;

    private static VariantNormalizer variantNormalizer = new VariantNormalizer(new VariantNormalizer.VariantNormalizerConfig()
            .setReuseVariants(true)
            .setNormalizeAlleles(true)
            .setDecomposeMNVs(false));

    private static final String VARIANT_STRING_PATTERN = "([ACGTN]*)|(<CNV\\d+>)|(<DUP>)|(<DEL>)|(<INS>)|(<INV>)";

    private static Logger logger = LoggerFactory.getLogger(HgmdVariantAnnotatorExtensionTask.class);

    public HgmdVariantAnnotatorExtensionTask(ObjectMap options) {
        if (MapUtils.isNotEmpty(options)) {
            if (options.containsKey(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_FILE.key())) {
                this.dbLocation = Paths.get(options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_FILE.key()));
            }
            if (options.containsKey(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_VERSION.key())) {
                this.hgmdVersion = options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_VERSION.key());
            }
            if (options.containsKey(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_ASSEMBLY.key())) {
                this.hgmdAssembly = options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_ASSEMBLY.key());
            }
            if (options.containsKey(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_INDEX_CREATION_DATE.key())) {
                this.hgmdIndexCreationDate =
                        options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_INDEX_CREATION_DATE.key());
            }
        }
    }

    public ObjectMap setup(VariantAnnotationExtensionConfigureParams configureParams, URI outDir) throws Exception {
        // First check HGMD input parameters: version, assembly and VCF file
        checkConfigureParameters(configureParams);

        String inputHgmdVersion = configureParams.getParams().getString(HGMD_VERSION_KEY);
        String inputHgmdAssembly = configureParams.getParams().getString(HGMD_ASSEMBLY_KEY);
        Path hgmdFile = Paths.get(configureParams.getResources().get(0));

        Path outDirPath = hgmdFile.getParent();
        FileUtils.checkDirectory(outDirPath, true);

        Path prevHgmdConfigFile = outDirPath.resolve(HGMD_ANNOTATOR_CONFIG_FILENAME);
        Path prevDbLocation = outDirPath.resolve(hgmdFile.getFileName() + HGMD_ANNOTATOR_INDEX_SUFFIX);

        // Check overwrite flag to delete the previous index and config if needed
        if (configureParams.getOverwrite() != null && configureParams.getOverwrite()) {
            cleanPreviousSetup(prevHgmdConfigFile, prevDbLocation);
        }

        if (Files.exists(prevHgmdConfigFile) && Files.exists(prevDbLocation)) {
            // Check compatibility with previous setup
            VariantAnnotationExtensionConfigureParams prevParams = checkPreviousSetup(prevHgmdConfigFile, hgmdFile, inputHgmdVersion,
                    inputHgmdAssembly);

            // Set existing parameters
            logger.info("Skipping setup since it was already done and overwrite is not set to true");
            dbLocation = prevDbLocation;
            hgmdVersion = inputHgmdVersion;
            hgmdAssembly = inputHgmdAssembly;
            hgmdIndexCreationDate = prevParams.getParams().getString(HGMD_INDEX_CREATION_DATE_KEY);
        } else {
            logger.info("Setup and populate RocksDB by parsing the HGMD file (version {}, assembly {})", inputHgmdVersion,
                    inputHgmdAssembly);

            // Init RocksDB
            dbLocation = prevDbLocation;
            initRockDB(true);

            // Parse HGMD file and populate RocksDB
            HgmdIterator hgmdIterator = new HgmdIterator(hgmdFile, inputHgmdVersion, inputHgmdAssembly);
            while (hgmdIterator.hasNext()) {
                Variant variant = hgmdIterator.next();
                boolean success = updateRocksDB(variant);
                // updateRocksDB may fail (false) if normalisation process fails
                if (success) {
                    numberIndexedRecords++;
                }
                totalNumberRecords++;
                if (totalNumberRecords % 1000 == 0) {
                    logger.info("{} variants parsed", totalNumberRecords);
                }
            }
            logger.info("HGMD indexing done. {} out of {} variants indexed.", numberIndexedRecords, totalNumberRecords);

            // Close
            hgmdIterator.close();
            closeRocksDB();

            hgmdVersion = inputHgmdVersion;
            hgmdAssembly = inputHgmdAssembly;
            hgmdIndexCreationDate = Instant.now().toString();

            // Save configuration file, and adding the creation date
            VariantAnnotationExtensionConfigureParams newParams = new VariantAnnotationExtensionConfigureParams(configureParams);
            newParams.getParams().append(HGMD_INDEX_CREATION_DATE_KEY, hgmdIndexCreationDate);
            objectMapper.writerFor(VariantAnnotationExtensionConfigureParams.class).writeValue(prevHgmdConfigFile.toFile(), newParams);
        }

        return getOptions();
    }

    private VariantAnnotationExtensionConfigureParams checkPreviousSetup(Path prevHgmdConfigFile, Path hgmdFile, String inputHgmdVersion,
                                                                         String inputHgmdAssembly) throws ToolException {
        // Load existing config file
        VariantAnnotationExtensionConfigureParams previousParams;
        try {
            previousParams = objectMapper.readerFor(VariantAnnotationExtensionConfigureParams.class).readValue(prevHgmdConfigFile.toFile());
        } catch (IOException e) {
            throw new ToolException("Error reading the previous HGMD extension configuration", e);
        }

        // Check compatibility
        if (!hgmdFile.toAbsolutePath().toString().equals(previousParams.getResources().get(0))) {
            throw new ToolException("HGMD file '" + hgmdFile + "' does not match the existing config file version '"
                    + previousParams.getResources().get(0) + "'. Use the overwrite flag to force the update.");
        }

        if (!inputHgmdVersion.equals(previousParams.getParams().get(HGMD_VERSION_KEY))) {
            throw new ToolException("HGMD version '" + inputHgmdVersion + "' does not match the existing config file version '"
                    + previousParams.getParams().get(HGMD_VERSION_KEY) + "'. Use the overwrite flag to force the update.");
        }

        if (!inputHgmdAssembly.equals(previousParams.getParams().get(HGMD_ASSEMBLY_KEY))) {
            throw new ToolException("HGMD assembly '" + inputHgmdAssembly + "' does not match the existing config file version '"
                    + previousParams.getParams().get(HGMD_ASSEMBLY_KEY) + "'. Use the overwrite flag to force the update.");
        }

        return previousParams;
    }

    private static void cleanPreviousSetup(Path prevHgmdConfigFile, Path prevDbLocation) throws IOException {
        if (Files.exists(prevHgmdConfigFile)) {
            logger.info("Deleting existing HGMD config file: {}", prevHgmdConfigFile.toAbsolutePath());
            Files.delete(prevHgmdConfigFile);
        }
        if (Files.exists(prevDbLocation)) {
            logger.info("Deleting existing HGMD RocksDB folder: {}", prevDbLocation.toAbsolutePath());
            if (Files.exists(prevDbLocation)) {
                logger.info("Deleting existing HGMD RocksDB folder: {}", prevDbLocation.toAbsolutePath());
                try (Stream<Path> paths = Files.walk(prevDbLocation)) {
                    paths.sorted(Comparator.reverseOrder())
                            .forEach(path -> {
                                try {
                                    Files.delete(path);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                }
            }
        }
    }

    @Override
    public void check(ObjectMap options) throws IllegalArgumentException {
        if (dbLocation == null || StringUtils.isEmpty(dbLocation.toString())) {
            throw new IllegalArgumentException("Missing HGMD file");
        }
        if (StringUtils.isEmpty(hgmdVersion)) {
            throw new IllegalArgumentException("Missing HGMD version");
        }
        if (StringUtils.isEmpty(hgmdAssembly)) {
            throw new IllegalArgumentException("Missing HGMD assembly");
        }

        if (MapUtils.isNotEmpty(options) && options.containsKey(ASSEMBLY.key())
                && !options.getString(ASSEMBLY.key()).equalsIgnoreCase(hgmdAssembly)) {
            throw new IllegalArgumentException("HGMD assembly '" + hgmdAssembly + "' does not match the variant storage assembly '"
                    + options.getString(ASSEMBLY.key()) + "'");
        }
    }

    @Override
    public void checkAvailable() throws IllegalArgumentException {
        check(null);
        if (dbLocation == null || !Files.exists(dbLocation)
                || !dbLocation.toAbsolutePath().toString().endsWith(HGMD_ANNOTATOR_INDEX_SUFFIX)) {
            throw new IllegalArgumentException("HGMD annotator extension is not available (dbLocation = " + dbLocation + ")");
        }
    }

    @Override
    public ObjectMap getOptions() {
        checkAvailable();
        return new ObjectMap()
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_FILE.key(), dbLocation.toAbsolutePath().toString())
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_VERSION.key(), hgmdVersion)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_ASSEMBLY.key(), hgmdAssembly)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_HGMD_INDEX_CREATION_DATE.key(), hgmdIndexCreationDate);
    }

    @Override
    public ObjectMap getMetadata() {
        checkAvailable();
        return new ObjectMap()
                .append(NAME_KEY, ID)
                .append(HGMD_VERSION_KEY, hgmdVersion)
                .append(HGMD_ASSEMBLY_KEY, hgmdAssembly)
                .append(INDEX_CREATION_DATE_KEY, hgmdIndexCreationDate);
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
            } else {
                Variant variant;
                try {
                    variant = new Variant(variantAnnotation.getChromosome(), variantAnnotation.getStart(), reference, alternate);
                } catch (Exception e) {
                    logger.warn("Skipping variant: chromosome = {}, start = {}, reference = {}, alternate = {}: it could not be built",
                            variantAnnotation.getChromosome(), variantAnnotation.getStart(), variantAnnotation.getReference(),
                            variantAnnotation.getAlternate(), e);
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
        // the Options class contains a set of configurable DB options that determines the behavior of a database,
        // and Options are closed in the closeRocksDB method
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


    private static String checkEmptySequence(String sequence) {
        return sequence != null && !sequence.equals("-") ? sequence : "";
    }

    private boolean isValid(String reference, String alternate) {
        return (reference.matches(VARIANT_STRING_PATTERN)
                && (alternate.matches(VARIANT_STRING_PATTERN)
                && !alternate.equals(reference)));
    }

    private boolean updateRocksDB(Variant variant)
            throws NonStandardCompliantSampleField, RocksDBException, IOException {
        if (variant.getAnnotation() == null || CollectionUtils.isEmpty(variant.getAnnotation().getTraitAssociation())) {
            // No evidence entries to add
            return false;
        }

        // Add evidence entries in the RocksDB
        List<String> normalisedVariantStringList = getNormalisedVariantString(variant);
        if (CollectionUtils.isNotEmpty(normalisedVariantStringList)) {
            List<EvidenceEntry> evidenceEntries;
            for (String normalisedVariantString : normalisedVariantStringList) {
                byte[] dbContent = rdb.get(normalisedVariantString.getBytes());
                if (dbContent != null) {
                    evidenceEntries = objectReader.readValue(dbContent);
                    evidenceEntries.addAll(variant.getAnnotation().getTraitAssociation());
                } else {
                    evidenceEntries = variant.getAnnotation().getTraitAssociation();
                }
                rdb.put(normalisedVariantString.getBytes(), objectWriter.writeValueAsBytes(evidenceEntries));
            }

            return true;
        }
        return false;
    }

    protected List<String> getNormalisedVariantString(Variant variant) throws NonStandardCompliantSampleField {
        // Checks no weird characters are part of the reference & alternate alleles
        if (isValid(variant)) {
            List<Variant> normalizedVariantList = variantNormalizer.normalize(Collections.singletonList(variant), true);
            return normalizedVariantList.stream().map(Variant::toString).collect(Collectors.toList());
        }

        logger.warn("Variant {} is not valid: skipping it!", variant);
        return Collections.emptyList();
    }

    protected boolean isValid(Variant variant) {
        return (variant.getReference().matches(VARIANT_STRING_PATTERN)
                && (variant.getAlternate().matches(VARIANT_STRING_PATTERN)
                && !variant.getAlternate().equals(variant.getReference())));
    }

    private void checkConfigureParameters(VariantAnnotationExtensionConfigureParams configureParams) throws ToolException, IOException {
        // Sanity check
        if (!ID.equals(configureParams.getExtension())) {
            throw new ToolException("Invalid HGMD annotator extension ID: " + configureParams.getExtension());
        }

        // Check HGMD version and assembly
        String tmpHgmdVersion = configureParams.getParams().getString(HGMD_VERSION_KEY);
        if (StringUtils.isEmpty(tmpHgmdVersion)) {
            throw new ToolException("Missing HGMD version");
        }
        String tmpHgmdAssembly = configureParams.getParams().getString(HGMD_ASSEMBLY_KEY);
        if (StringUtils.isEmpty(tmpHgmdAssembly)) {
            throw new ToolException("Missing HGMD assembly");
        }

        // Check HGMD file
        Path hgmdFile = Paths.get(configureParams.getResources().get(0));
        logger.info("HGMD file {}", hgmdFile.toAbsolutePath());
        FileUtils.checkFile(hgmdFile);
        if (!hgmdFile.getFileName().toString().endsWith(".vcf.gz") && !hgmdFile.getFileName().toString().endsWith(".vcf")) {
            throw new ToolException("Invalid HGMD file format '" + hgmdFile.getFileName() + "': it must be a .vcf.gz or .vcf file");
        }
    }
}
