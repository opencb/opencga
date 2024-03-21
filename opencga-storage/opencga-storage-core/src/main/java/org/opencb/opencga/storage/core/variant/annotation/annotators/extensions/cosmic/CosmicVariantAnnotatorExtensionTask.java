package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.biodata.formats.variant.cosmic.CosmicParser;
import org.opencb.biodata.models.common.DataVersion;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.VariantAnnotatorExtensionTask;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

public class CosmicVariantAnnotatorExtensionTask implements VariantAnnotatorExtensionTask {

    private ObjectMap options;

    private Path cosmicFolder;

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
        // Check input path
        cosmicFolder = Paths.get(output.getPath());
        if (cosmicFolder == null || !Files.exists(cosmicFolder)) {
            throw new IllegalArgumentException("Path " + output + " does not exist");
        }
        if (!cosmicFolder.toFile().isDirectory()) {
            throw new IllegalArgumentException("Path " + output + " must be a directory with two files: the raw COSMIC file and the"
                    + " metadata file 'cosmicVersion.json'");
        }

        // Clean and init RocksDB
        dbLocation = cosmicFolder.toAbsolutePath().resolve(COSMIC_ANNOTATOR_INDEX_NAME);
        if (Files.exists(dbLocation)) {
            // Skipping setup but init RocksDB
            logger.info("Skipping setup, it was already done");
            initRockDB(false);
        } else {
            logger.info("Setup and populate RocksDB");
            File versionFile = cosmicFolder.resolve(COSMIC_VERSION_FILENAME).toFile();
            if (!versionFile.exists()) {
                throw new IllegalArgumentException("Path " + output + " does not contain the COSMIC metadata file: "
                        + COSMIC_VERSION_FILENAME);
            }
            DataVersion dataVersion;
            try {
                dataVersion = JacksonUtils.getDefaultObjectMapper().readValue(versionFile, DataVersion.class);
            } catch (IOException e) {
                throw new IllegalArgumentException("Error parsing the metadata file " + versionFile.getAbsolutePath(), e);
            }
            String cosmicFilename;
            try {
                cosmicFilename = dataVersion.getFiles().get(0);
            } catch (Exception e) {
                throw new IllegalArgumentException("Error getting the COSMIC file from the metadata file "
                        + versionFile.getAbsolutePath(), e);
            }
            File cosmicFile = cosmicFolder.resolve(cosmicFilename).toFile();
            if (!cosmicFile.exists()) {
                throw new IllegalArgumentException("COSMIC file " + cosmicFile.getAbsolutePath() + " does not exist");
            }

            // Init RocksDB
            initRockDB(true);

            // Call COSMIC parser
            try {
                CosmicExtensionTaskCallback callback = new CosmicExtensionTaskCallback(rdb);
                CosmicParser.parse(cosmicFile.toPath(), dataVersion.getVersion(), dataVersion.getName(), dataVersion.getAssembly(),
                        callback);
            } catch (IOException e) {
                throw new ToolException(e);
            }
        }
        return Collections.singletonList(dbLocation.toUri());
    }

    @Override
    public void checkAvailable() throws IllegalArgumentException {
        if (!isAvailable()) {
            throw new IllegalArgumentException("COSMIC annotator extension is not available");
        }
    }

    @Override
    public boolean isAvailable() {
        return (dbLocation != null && Files.exists(dbLocation));
    }

    @Override
    public ObjectMap getOptions() {
        return options;
    }

    @Override
    public ObjectMap getMetadata() {
        File versionFile = cosmicFolder.resolve(COSMIC_VERSION_FILENAME).toFile();
        if (!versionFile.exists()) {
            throw new IllegalArgumentException("Metadata file " + versionFile + " does not exist");
        }
        try {
            return JacksonUtils.getDefaultObjectMapper().readValue(versionFile, ObjectMap.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public List<VariantAnnotation> apply(List<VariantAnnotation> list) throws Exception {
        for (VariantAnnotation variantAnnotation : list) {
            Variant variant = new Variant(variantAnnotation.getChromosome(), variantAnnotation.getStart(), variantAnnotation.getReference(),
                    variantAnnotation.getAlternate());
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
