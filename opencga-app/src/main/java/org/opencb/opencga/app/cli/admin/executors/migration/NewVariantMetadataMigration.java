package org.opencb.opencga.app.cli.admin.executors.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.base.Throwables;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.legacy.VariantGlobalStats;
import org.opencb.biodata.models.variant.avro.legacy.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
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
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.storage.core.metadata.VariantSourceToVariantFileMetadataConverter;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.transform.VariantTransformTask;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Executes all migration scripts related with the issue #673
 * Created on 13/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class NewVariantMetadataMigration {

    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(NewVariantMetadataMigration.class);
    private final StorageConfiguration storageConfiguration;
    private final CatalogManager catalogManager;

    public NewVariantMetadataMigration(StorageConfiguration storageConfiguration, CatalogManager catalogManager, MigrationCommandOptions.MigrateV1_3_0CommandOptions options) {
        this.storageConfiguration = storageConfiguration;
        this.catalogManager = catalogManager;
        objectMapper = new ObjectMapper()
                .addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
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
//        VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));

        Query vcfFilesQuery = new Query()
                .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.VCF)
                .append(FileDBAdaptor.QueryParams.ATTRIBUTES.key() + '.' + FileMetadataReader.VARIANT_SOURCE + '.' + "fileId", "~.*");
        Query metadataFilesQuery = new Query()
                .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.JSON)
                .append(FileDBAdaptor.QueryParams.NAME.key(), '~' + VariantReaderUtils.METADATA_FILE_FORMAT_GZ + '$');

        List<Project> projects = catalogManager.getProjectManager().get(new Query(), new QueryOptions(
                QueryOptions.INCLUDE, Arrays.asList(
                ProjectDBAdaptor.QueryParams.NAME.key(),
                ProjectDBAdaptor.QueryParams.ALIAS.key()
        )), sessionId).getResult();

        Set<DataStore> dataStores = new HashSet<>();
        for (Project project : projects) {
            logger.info("Migrating project " + project.getName());
            for (Study study : project.getStudies()) {
                logger.info("Migrating study " + study.getName());

                // Migrate catalog metadata information from file entries
                migrateCatalogFileMetadata(sessionId, converter, vcfFilesQuery, study);

                // Migrate metadata files from FileSystem
                migrateMetadataFiles(sessionId, metadataFilesQuery, study);

                DataStore dataStore = StorageOperation.getDataStore(catalogManager, study, File.Bioformat.VARIANT, sessionId);
                dataStores.add(dataStore);
            }
        }

        StorageEtlConfiguration etlConfiguration = storageConfiguration.getStorageEngine(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID).getVariant();
        ObjectMap options = etlConfiguration.getOptions();
        DatabaseCredentials database = etlConfiguration.getDatabase();

        MongoCredentials credentials = null;
        try {
            credentials = new MongoCredentials(database, "");
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
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study.getId(), vcfFilesQuery, new QueryOptions(), sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                logger.info("Migrating file " + file.getName());

                VariantSource variantSource = getObject(file.getAttributes(), FileMetadataReader.VARIANT_SOURCE, VariantSource.class);
                VariantFileMetadata fileMetadata = converter.convert(variantSource);

                ObjectMap parameters = new ObjectMap()
                        .append(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), new ObjectMap(FileMetadataReader.VARIANT_FILE_METADATA, fileMetadata));

                VariantGlobalStats globalStats = getObject(file.getStats(), FileMetadataReader.VARIANT_STATS, VariantGlobalStats.class);
                if (globalStats != null) {
                    VariantSetStats variantSetStats = converter.convertStats(globalStats);
                    parameters.append(FileDBAdaptor.QueryParams.STATS.key(), new ObjectMap(FileMetadataReader.VARIANT_FILE_STATS, variantSetStats));
                }

                catalogManager.getFileManager().update(null, String.valueOf(file.getId()), parameters, null, sessionId);
            }
        }
    }

    private void migrateMetadataFiles(String sessionId, Query metadataFilesQuery, Study study) throws IOException, CatalogException {
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study.getId(), metadataFilesQuery, new QueryOptions(), sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                Path metaFile = Paths.get(file.getUri());
                logger.info("Migrating file " + metaFile);
                migrateVariantFileMetadataFile(metaFile);
            }
        }
    }

    private void migrateVariantFileMetadataFile(Path metaFile) throws IOException {
        if (!VariantReaderUtils.isMetaFile(metaFile.toString())) {
            return;
        }

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
                logger.info("File already migrated!");
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
        VariantTransformTask.writeVariantFileMetadata(fileMetadata, metaFile);

        // Delete backup!
        Files.deleteIfExists(backupFile);
    }

    private void migrateFilesCollection(MongoDataStore mongoDataStore, ObjectMap options) {

    }

    private void migrateStudiesCollection(MongoDataStore mongoDataStore, ObjectMap options) {

    }

    private <T> T getObject(Map<String, Object> attributes, String key, Class<T> clazz) throws java.io.IOException {
        Object value = attributes.get(key);
        if (value == null) {
            return null;
        } else {
            return objectMapper.readValue(objectMapper.writeValueAsString(value), clazz);
        }
    }
}
