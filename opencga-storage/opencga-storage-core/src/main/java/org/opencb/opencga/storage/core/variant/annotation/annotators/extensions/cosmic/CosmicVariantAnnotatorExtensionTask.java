package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.variant.cosmic.CosmicParser;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
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
import java.util.Collections;
import java.util.List;

public class CosmicVariantAnnotatorExtensionTask implements VariantAnnotatorExtensionTask {

    public static final String ID = "cosmic";

    private ObjectMap options;

    private String cosmicVersion;
    private String assembly;

    private ObjectReader objectReader;

    private RocksDB rdb = null;
    private Options dbOption = null;
    private Path dbLocation = null;

    public static final String COSMIC_ANNOTATOR_INDEX_NAME = "cosmicAnnotatorIndex";
    public static final String COSMIC_VERSION_FILENAME  = "cosmicVersion.json";

    private static Logger logger = LoggerFactory.getLogger(CosmicVariantAnnotatorExtensionTask.class);

    public CosmicVariantAnnotatorExtensionTask(ObjectMap options) {
        this.options = options;
        this.objectReader = JacksonUtils.getDefaultObjectMapper().readerFor(new TypeReference<List<EvidenceEntry>>() {});
    }

    @Override
    public List<URI> setup(URI output) throws Exception {
        // Sanity check
        Path cosmicFile = Paths.get(options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key()));
        if (!Files.exists(cosmicFile)) {
            throw new IllegalArgumentException("COSMIC file " + cosmicFile + " does not exist");
        }
        cosmicVersion = (String) options.getOrDefault(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key(), null);
        if (StringUtils.isEmpty(cosmicVersion)) {
            throw new IllegalArgumentException("Missing COSMIC version");
        }
        assembly = (String) options.getOrDefault(VariantStorageOptions.ASSEMBLY.key(), null);
        if (StringUtils.isEmpty(assembly)) {
            throw new IllegalArgumentException("Missing assembly");
        }

        // Clean and init RocksDB
        dbLocation = Paths.get(output.getPath()).toAbsolutePath().resolve(COSMIC_ANNOTATOR_INDEX_NAME);
        if (Files.exists(dbLocation)) {
            // Skipping setup but init RocksDB
            logger.info("Skipping setup, it was already done");
            initRockDB(false);
        } else {
            logger.info("Setup and populate RocksDB");

            // Init RocksDB
            initRockDB(true);

            // Call COSMIC parser
            try {
                CosmicExtensionTaskCallback callback = new CosmicExtensionTaskCallback(rdb);
                CosmicParser.parse(cosmicFile, cosmicVersion, ID, assembly, callback);
            } catch (IOException e) {
                throw new ToolException(e);
            }
        }
        return Collections.singletonList(dbLocation.toUri());
    }

    @Override
    public void checkAvailable() throws IllegalArgumentException {
        if (dbLocation == null || !Files.exists(dbLocation)) {
            throw new IllegalArgumentException("COSMIC annotator extension is not available");
        }
    }

    @Override
    public ObjectMap getOptions() {
        return options;
    }

    @Override
    public ObjectMap getMetadata() {
        return new ObjectMap("data", ID)
                .append("version", cosmicVersion)
                .append("assembly", assembly);
    }

    @Override
    public List<VariantAnnotation> apply(List<VariantAnnotation> list) throws Exception {
        for (VariantAnnotation variantAnnotation : list) {
            Variant variant;
            try {
                variant = new Variant(variantAnnotation.getChromosome(), variantAnnotation.getStart(), variantAnnotation.getReference(),
                        variantAnnotation.getAlternate());
            } catch (Exception e) {
                logger.warn("Skipping variant: " + e.getMessage());
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
    public void post() throws Exception {
        closeRocksDB();
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
            // Do some error handling
            throw new ToolException("", e);
        }
    }
}
