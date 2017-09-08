package org.opencb.opencga.app.cli.admin.executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.legacy.VariantGlobalStats;
import org.opencb.biodata.models.variant.avro.legacy.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.admin.options.MigrationCommandOptions;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.storage.core.metadata.VariantSourceToVariantFileMetadataConverter;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.transform.VariantTransformTask;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created on 08/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MigrationCommandExecutor extends AdminCommandExecutor {

    private final MigrationCommandOptions migrationCommandOptions;
    private final ObjectMapper objectMapper;

    public MigrationCommandExecutor(MigrationCommandOptions migrationCommandOptions) {
        super(migrationCommandOptions.getCommonOptions());
        this.migrationCommandOptions = migrationCommandOptions;
        objectMapper = new ObjectMapper()
                .addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing migration command line");

        String subCommandString = migrationCommandOptions.getSubCommand();
        switch (subCommandString) {
//            case "latest":
            case "v1.3.0":
                v1_3_0();
                break;
            default:
                logger.error("Subcommand '{}' not valid", subCommandString);
                break;
        }
    }

    private void v1_3_0() throws Exception {
        logger.info("MIGRATING v1.3.0");
        MigrationCommandOptions.MigrateV1_3_0CommandOptions options = migrationCommandOptions.getMigrateV130CommandOptions();

        setCatalogDatabaseCredentials(options, options.commonOptions);

        try (CatalogManager catalogManager = new CatalogManager(configuration)) {
            String sessionId = catalogManager.getUserManager().login("admin", options.commonOptions.adminPassword);

            migrate673(catalogManager, sessionId);
        }
    }

    /**
     * Migrate related with issue #673
     *
     *  - VariantSource and VariantGlobalStats at Catalog File documents
     *  - VariantSource intermediate files
     *  - VariantSource stored at Variant Databases
     *  - VariantStudyMetadata to VariantFileHeader at StudyConfiguration
     *
     * @param catalogManager Catalog manager
     * @param sessionId      Admin session id
     */
    private void migrate673(CatalogManager catalogManager, String sessionId) throws CatalogException, IOException {
        VariantSourceToVariantFileMetadataConverter converter = new VariantSourceToVariantFileMetadataConverter();

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

        for (Project project : projects) {
            logger.info("Migrating project " + project.getName());
            for (Study study : project.getStudies()) {
                logger.info("Migrating study " + study.getName());

                try (DBIterator<File> iterator = catalogManager.getFileManager()
                        .iterator(study.getId(), vcfFilesQuery, new QueryOptions(), sessionId)) {
                    while (iterator.hasNext()) {
                        File file = iterator.next();
                        logger.info("Migrating file " + file.getName());

                        VariantSource variantSource = getObject(file.getAttributes(), FileMetadataReader.VARIANT_SOURCE, VariantSource.class);
                        VariantFileMetadata fileMetadata = converter.convert(variantSource);

                        VariantGlobalStats globalStats = getObject(file.getStats(), FileMetadataReader.VARIANT_STATS, VariantGlobalStats.class);
                        VariantSetStats variantSetStats = converter.convertStats(globalStats);

                        ObjectMap parameters = new ObjectMap()
                                .append(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), new ObjectMap(FileMetadataReader.VARIANT_FILE_METADATA, fileMetadata))
                                .append(FileDBAdaptor.QueryParams.STATS.key(), new ObjectMap(FileMetadataReader.VARIANT_FILE_STATS, variantSetStats));
                        catalogManager.getFileManager().update(null, String.valueOf(file.getId()), parameters, null, sessionId);
                    }
                }

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

    private <T> T getObject(Map<String, Object> attributes, String key, Class<T> clazz) throws java.io.IOException {
        Object value = attributes.get(key);
        if (value == null) {
            return null;
        } else {
            return objectMapper.readValue(objectMapper.writeValueAsString(value), clazz);
        }
    }
}
