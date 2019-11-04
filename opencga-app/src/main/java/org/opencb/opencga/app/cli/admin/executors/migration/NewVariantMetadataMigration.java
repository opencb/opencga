package org.opencb.opencga.app.cli.admin.executors.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.base.Throwables;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import org.apache.avro.generic.GenericRecord;
import org.bson.Document;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.legacy.VariantGlobalStats;
import org.opencb.biodata.models.variant.avro.legacy.VariantSource;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.models.variant.stats.VariantSetStats;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.app.cli.admin.options.MigrationCommandOptions;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.storage.core.metadata.VariantSourceToVariantFileMetadataConverter;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.transform.VariantTransformTask;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantFileMetadataConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_FILES;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.COLLECTION_STUDIES;

/**
 * Executes all migration scripts related with the issue #673
 * Created on 13/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class NewVariantMetadataMigration {

    protected static final String BACKUP_COLLECTION_SUFIX = "_bk_1_1_x";
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(NewVariantMetadataMigration.class);
    private final StorageConfiguration storageConfiguration;
    private final CatalogManager catalogManager;
    protected static final QueryOptions UPSER_OPTIONS = new QueryOptions(MongoDBCollection.UPSERT, true).append(MongoDBCollection.REPLACE, true);
    protected static final QueryOptions REPLACE_OPTIONS = new QueryOptions(MongoDBCollection.REPLACE, true);
    private final boolean skipDiskFiles;
    private boolean createBackup;

    public NewVariantMetadataMigration(StorageConfiguration storageConfiguration, CatalogManager catalogManager,
                                       MigrationCommandOptions.MigrateV1_3_0CommandOptions options) {
        this.storageConfiguration = storageConfiguration;
        this.catalogManager = catalogManager;
        objectMapper = new ObjectMapper()
                .addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        createBackup = options.createBackup;
        skipDiskFiles = options.skipDiskFiles;
    }

    /**
     * Migrate related with issue #673
     *
     *  - VariantSource and VariantGlobalStats at Catalog File documents
     *  - VariantSource intermediate files
     *  - VariantSource stored at Variant Databases
     *  - VariantStudyMetadata to VariantFileHeader at StudyConfiguration
     *
     * @param sessionId      Admin session id
     */
    public void migrate(String sessionId) throws CatalogException, IOException {
        VariantSourceToVariantFileMetadataConverter converter = new VariantSourceToVariantFileMetadataConverter();
        StorageEngineFactory.configure(storageConfiguration);

        Query vcfFilesQuery = new Query()
                .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.VCF)
                .append(FileDBAdaptor.QueryParams.ATTRIBUTES.key() + '.' + FileMetadataReader.VARIANT_SOURCE + '.' + "fileId", "~.*");
        Query metadataFilesQuery = new Query()
                .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.JSON)
                .append(FileDBAdaptor.QueryParams.NAME.key(), '~' + VariantReaderUtils.METADATA_FILE_FORMAT_GZ + '$');

        List<Project> projects = catalogManager.getProjectManager().get(new Query(), new QueryOptions(
                QueryOptions.INCLUDE, Arrays.asList(
                ProjectDBAdaptor.QueryParams.NAME.key(),
                ProjectDBAdaptor.QueryParams.ID.key()
        )), sessionId).getResult();

        Set<DataStore> dataStores = new HashSet<>();
        for (Project project : projects) {
            logger.info("Migrating project " + project.getName());
            for (Study study : project.getStudies()) {
                logger.info("Migrating study " + study.getName());

                // Migrate catalog metadata information from file entries
                migrateCatalogFileMetadata(sessionId, converter, vcfFilesQuery, study);

                if (!skipDiskFiles) {
                    // Migrate metadata files from FileSystem
                    migrateMetadataFiles(sessionId, metadataFilesQuery, study);
                }

                DataStore dataStore = StorageOperation.getDataStore(catalogManager, study.getFqn(), File.Bioformat.VARIANT, sessionId);
                dataStores.add(dataStore);
            }
        }

        StorageEtlConfiguration etlConfiguration = storageConfiguration.getStorageEngine(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID).getVariant();
        ObjectMap options = etlConfiguration.getOptions();
        DatabaseCredentials database = etlConfiguration.getDatabase();

        MongoCredentials credentials;
        try {
            credentials = new MongoCredentials(database, null);
        } catch (IllegalOpenCGACredentialsException e) {
            throw Throwables.propagate(e);
        }
        try (MongoDataStoreManager manager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses())) {
            for (DataStore dataStore : dataStores) {
                if (dataStore.getStorageEngine().equals(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID)) {
                    MongoDataStore mongoDataStore = manager.get(dataStore.getDbName(), credentials.getMongoDBConfiguration());

                    migrateFilesCollection(mongoDataStore, options);

                    migrateStudiesCollection(mongoDataStore, options);
                }
            }
        }
    }

    private void migrateCatalogFileMetadata(String sessionId, VariantSourceToVariantFileMetadataConverter converter, Query vcfFilesQuery, Study study) throws IOException, CatalogException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, "name,id,path,uri"
                + ",attributes." + FileMetadataReader.VARIANT_SOURCE
                + ",attributes." + FileMetadataReader.VARIANT_FILE_METADATA
                + ",stats." + FileMetadataReader.VARIANT_STATS
                + ",stats." + FileMetadataReader.VARIANT_FILE_STATS)
                .append("lazy", true);
        int alreadyMigratedFile = 0;
        int migratedFiles = 0;
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study.getId(), vcfFilesQuery, options, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                logger.info("Migrating file " + file.getName());

                ObjectMap parameters = new ObjectMap();
                if (!file.getAttributes().containsKey(FileMetadataReader.VARIANT_FILE_METADATA)) {
                    VariantSource variantSource = getObject(file.getAttributes(), FileMetadataReader.VARIANT_SOURCE, VariantSource.class);
                    VariantFileMetadata fileMetadata = converter.convert(variantSource);

                    parameters.append(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), new ObjectMap(FileMetadataReader.VARIANT_FILE_METADATA, fileMetadata));
                }

                if (!file.getStats().containsKey(FileMetadataReader.VARIANT_FILE_STATS)) {
                    VariantGlobalStats globalStats = getObject(file.getStats(), FileMetadataReader.VARIANT_STATS, VariantGlobalStats.class);
                    if (globalStats != null) {
                        VariantSetStats variantSetStats = converter.convertStats(globalStats);
                        parameters.append(FileDBAdaptor.QueryParams.STATS.key(), new ObjectMap(FileMetadataReader.VARIANT_FILE_STATS, variantSetStats));
                    }
                }

                if (parameters.isEmpty()) {
                    alreadyMigratedFile++;
                } else {
                    migratedFiles++;
                    catalogManager.getFileManager().update(null, String.valueOf(file.getId()), parameters, null, sessionId);
                }
            }
        }
        if (migratedFiles == 0) {
            logger.info("Nothing to do!");
        } else {
            logger.info("Number of migrated files: " + migratedFiles +
                    (alreadyMigratedFile == 0 ? "" : ". Number of already migrated files (skipped): " + alreadyMigratedFile));
        }
    }

    private void migrateMetadataFiles(String sessionId, Query metadataFilesQuery, Study study) throws IOException, CatalogException {
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study.getId(), metadataFilesQuery, new QueryOptions("lazy", true), sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                Path metaFile = Paths.get(file.getUri());
                migrateVariantFileMetadataFile(metaFile);
            }
        }
    }

    public void migrateVariantFileMetadataFile(Path metaFile) throws IOException {
        if (!VariantReaderUtils.isMetaFile(metaFile.toString())) {
            return;
        }
        if (!metaFile.toFile().exists()) {
            logger.warn("File " + metaFile + " not found! Skip this file");
            return;
        }
        logger.info("Migrating file " + metaFile);

        VariantSourceToVariantFileMetadataConverter converter = new VariantSourceToVariantFileMetadataConverter();

        Path backupFile = Paths.get(metaFile.toString() + ".bk");

        // Check backup already exists!
        // If so, read backup file!
        Path inputFile;
        if (backupFile.toFile().exists()) {
            inputFile = backupFile;
        } else {
            inputFile = metaFile;
        }

        VariantSource variantSource;
        try (InputStream inputStream = new GZIPInputStream(new FileInputStream(inputFile.toFile()))) {
            variantSource = objectMapper.readValue(inputStream, VariantSource.class);
        } catch (UnrecognizedPropertyException e) {
            try (InputStream inputStream = new GZIPInputStream(new FileInputStream(inputFile.toFile()))) {
                objectMapper.readValue(inputStream, VariantFileMetadata.class);
                // The file is already a VariantFileMetadata! Skip this file
                logger.info("File " + metaFile + " already migrated!");
                return;
            } catch (Exception ex) {
                // Some error occurred. Throw original exception!
                throw e;
            }
        }

        VariantFileMetadata fileMetadata = converter.convert(variantSource);

        // Crate backup!
        Files.copy(metaFile, backupFile, StandardCopyOption.REPLACE_EXISTING);

        // Write new model
        try (OutputStream os = new GZIPOutputStream(new FileOutputStream(metaFile.toFile()))) {
            VariantTransformTask.writeVariantFileMetadata(fileMetadata, os);
        }

        // Delete backup!
        if (!createBackup) {
            Files.deleteIfExists(backupFile);
        }
    }

    private void migrateFilesCollection(MongoDataStore mongoDataStore, ObjectMap options) {
        String filesCollectionName = options.getString(COLLECTION_FILES.key(), COLLECTION_FILES.defaultValue());
        MongoDBCollection filesCollection = mongoDataStore.getCollection(filesCollectionName);
        MongoDBCollection filesBkCollection = mongoDataStore.getCollection(filesCollectionName + BACKUP_COLLECTION_SUFIX);

        GenericDocumentComplexConverter<VariantSource> variantSourceConverter = new GenericDocumentComplexConverter<>(VariantSource.class);
        VariantSourceToVariantFileMetadataConverter variantFileMetadataConverter = new VariantSourceToVariantFileMetadataConverter();
        DocumentToVariantFileMetadataConverter documentToVariantFileMetadataConverter = new DocumentToVariantFileMetadataConverter();

        variantSourceConverter.getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);

        logger.info("Migrate '" + filesCollectionName + "' collection from db " + mongoDataStore.getDatabaseName());
        for (Document document : filesCollection.nativeQuery().find(exists("fileId", true), new QueryOptions())) {
            logger.info("Migrate VariantSource " + document.getString("studyName"));

            // Save backup
            filesBkCollection.update(eq("_id", document.get("_id")), document, UPSER_OPTIONS);

            // Migrate
            VariantSource variantSource = variantSourceConverter.convertToDataModelType(document);
            VariantFileMetadata variantFileMetadata = variantFileMetadataConverter.convert(variantSource);
            Document newDocument = documentToVariantFileMetadataConverter.convertToStorageType(variantSource.getStudyId(), variantFileMetadata);

            // Save new version
            filesCollection.update(eq("_id", newDocument.get("_id")), newDocument, REPLACE_OPTIONS);
        }
    }

    private void migrateStudiesCollection(MongoDataStore mongoDataStore, ObjectMap options) {
        String studiesCollectionName = options.getString(COLLECTION_STUDIES.key(), COLLECTION_STUDIES.defaultValue());
        MongoDBCollection studiesCollection = mongoDataStore.getCollection(studiesCollectionName);
        MongoDBCollection studiesBkCollection = mongoDataStore.getCollection(studiesCollectionName + BACKUP_COLLECTION_SUFIX);

        GenericDocumentComplexConverter<VariantStudyMetadata> variantStudyMetadataConverter = new GenericDocumentComplexConverter<>(VariantStudyMetadata.class);
        GenericDocumentComplexConverter<VariantFileHeader> variantFileHeaderConverter = new GenericDocumentComplexConverter<>(VariantFileHeader.class);
        variantFileHeaderConverter.getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);

        logger.info("Migrate '" + studiesCollectionName + "' collection from db " + mongoDataStore.getDatabaseName());
        for (Document studyConfigurationDocument : studiesCollection.nativeQuery().find(exists("variantMetadata", true), new QueryOptions())) {
            Document variantMetadataObject = studyConfigurationDocument.get("variantMetadata", Document.class);
            if (variantMetadataObject == null) {
                continue;
            }
            logger.info("Migrate StudyConfiguration " + studyConfigurationDocument.getString("studyName"));
            // Save backup
            studiesBkCollection.update(eq("_id", studyConfigurationDocument.get("_id")), studyConfigurationDocument, UPSER_OPTIONS);

            //Migrate model
            VariantStudyMetadata variantStudyMetadata = variantStudyMetadataConverter.convertToDataModelType(variantMetadataObject);
            VariantFileHeader variantFileHeader = new VariantFileHeader(null, new LinkedList<>(), new LinkedList<>());
            for (VariantStudyMetadata.VariantMetadataRecord record : variantStudyMetadata.info.values()) {
                variantFileHeader.getComplexLines().add(toComplexLine("INFO", record));
            }
            for (VariantStudyMetadata.VariantMetadataRecord record : variantStudyMetadata.format.values()) {
                variantFileHeader.getComplexLines().add(toComplexLine("FORMAT", record));
            }
            if (!variantStudyMetadata.format.containsKey("GT")
                    && !studyConfigurationDocument.get("attributes", Document.class).getBoolean("exclude&#46;genotypes", true)) {
                variantFileHeader.getComplexLines().add(new VariantFileHeaderComplexLine(
                        "FORMAT",
                        VariantMerger.GT_KEY,
                        "Genotype",
                        "1",
                        VCFHeaderLineType.String.toString(), null));
            }
            variantStudyMetadata.alternates.forEach((alt, description)
                    -> variantFileHeader.getComplexLines().add(
                            new VariantFileHeaderComplexLine("ALT", alt, description, null, null, Collections.emptyMap())));
            variantStudyMetadata.filter.forEach((filter, description)
                    -> variantFileHeader.getComplexLines().add(
                            new VariantFileHeaderComplexLine("FILTER", filter, description, null, null, Collections.emptyMap())));
            variantStudyMetadata.contig.forEach((contig, length)
                    -> variantFileHeader.getComplexLines().add(
                            new VariantFileHeaderComplexLine("contig", contig, null, null, null, Collections.singletonMap("length", String.valueOf(length)))));

            // Save new version
            studyConfigurationDocument.remove("variantMetadata");
            studyConfigurationDocument.put("variantHeader", variantFileHeaderConverter.convertToStorageType(variantFileHeader));
            studiesCollection.update(eq("_id", studyConfigurationDocument.get("_id")), studyConfigurationDocument, REPLACE_OPTIONS);
        }
    }

    private VariantFileHeaderComplexLine toComplexLine(String key, VariantStudyMetadata.VariantMetadataRecord record) {
        String number;
        switch (record.numberType) {
            case INTEGER:
                number = record.number.toString();
                break;
            case UNBOUNDED:
                number = ".";
                break;
            default:
                number = record.numberType.toString();
                break;
        }
        return new VariantFileHeaderComplexLine(key, record.id, record.description, number, record.type.toString(), Collections.emptyMap());
    }

    private <T> T getObject(Map<String, Object> attributes, String key, Class<T> clazz) throws java.io.IOException {
        Object value = attributes.get(key);
        if (value == null) {
            return null;
        } else {
            return objectMapper.readValue(objectMapper.writeValueAsString(value), clazz);
        }
    }


    public static class VariantStudyMetadata {
        public Map<String, VariantMetadataRecord> info = new HashMap<>();    // Map from ID to VariantMetadataRecord
        public Map<String, VariantMetadataRecord> format = new HashMap<>();  // Map from ID to VariantMetadataRecord

        public Map<String, String> alternates = new HashMap<>(); // Symbolic alternates
        public Map<String, String> filter = new HashMap<>();
        public LinkedHashMap<String, Long> contig = new LinkedHashMap<>();

        public static class VariantMetadataRecord {
            public String id;
            public VCFHeaderLineCount numberType;
            public Integer number;
            public VCFHeaderLineType type;
            public String description;
        }
    }
}
