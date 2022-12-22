package org.opencb.opencga.storage.hadoop.variant.migration.v2_4_13;

import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.tools.ToolParams;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.hadoop.app.AbstractMain;
import org.opencb.opencga.storage.hadoop.utils.CopyHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.DEFAULT_TABLE_TYPE;

public class ConvertIntoVirtual extends AbstractMain {

    private static Logger logger = LoggerFactory.getLogger(ConvertIntoVirtual.class);

    public static class ConvertIntoVirtualParams extends ToolParams {
        protected String study;
        protected String dbName;
        protected String storageConfigurationFile = "/opt/opencga/conf/storage-configuration.yml";
        protected String virtualFile;
        protected List<String> files;
        protected int limit;
        protected boolean help;
        protected boolean fixPhoenix;
    }

    @Override
    public void run(String[] args) throws Exception {
        int step = 0;
        int steps = 5;
        ConvertIntoVirtualParams params = getArgsMap(args, 0, new ConvertIntoVirtualParams());
        if (params.help) {
            println(params.toCliHelp());
            return;
        }
        try {
            Objects.requireNonNull(params.study, "--study");
            Objects.requireNonNull(params.dbName, "--dbName");
            Objects.requireNonNull(params.storageConfigurationFile, "--storageConfigurationFile");
            Objects.requireNonNull(params.virtualFile, "--virtualFile");
            Objects.requireNonNull(params.files, "--files");
        } catch (NullPointerException e) {
            println(params.toCliHelp());
            throw e;
        }

        StorageConfiguration storageConfiguration;
        try (InputStream is = Files.newInputStream(Paths.get(params.storageConfigurationFile))) {
            storageConfiguration = StorageConfiguration.load(is);
        }
        HadoopVariantStorageEngine engine = new HadoopVariantStorageEngine();
        engine.setConfiguration(storageConfiguration, HadoopVariantStorageEngine.STORAGE_ENGINE_ID, params.dbName);


        VariantStorageMetadataManager mm = engine.getMetadataManager();
        int studyId = mm.getStudyId(params.study);

        logger.info("[" + ++step + "/" + steps + "] Check virtual file");
        FileMetadata virtualFile = mm.registerVirtualFile(studyId, params.virtualFile);

        logger.info("[" + ++step + "/" + steps + "] Obtain input files");
        if (params.files.size() == 1 && params.files.get(0).equals(ParamConstants.ALL)) {
            params.files = new ArrayList<>(params.limit);
            for (Integer indexedFile : mm.getIndexedFiles(studyId)) {
                if (virtualFile.getId() != indexedFile) {
                    params.files.add(mm.getFileName(studyId, indexedFile));
                    if (params.files.size() == params.limit) {
                        break;
                    }
                }
            }
        }


        logger.info("[" + ++step + "/" + steps + "] Launch MR to copy {} columns", params.files.size());
        // MR - copy
        // Include TYPE column, so it scans ALL rows. Avoid timeouts by executing a full table scan.
        List<String> columnsToInclude = Collections.singletonList(VariantPhoenixSchema.VariantColumn.TYPE.fullColumn());
        Map<String, String> columnsToCopyMap = new HashMap<>();
        String targetColumn = VariantPhoenixSchema.getFileColumn(studyId, virtualFile.getId()).fullColumn();
        List<CharSequence> phoenixColumnsToDrop = new ArrayList<>(params.files.size());
        for (String file : params.files) {
            PhoenixHelper.Column fileColumn = VariantPhoenixSchema.getFileColumn(studyId, mm.getFileId(studyId, file));
            columnsToCopyMap.put(fileColumn.fullColumn(), targetColumn);
            phoenixColumnsToDrop.add(fileColumn.column());
        }

        engine.getMRExecutor().run(CopyHBaseColumnDriver.class, CopyHBaseColumnDriver.buildArgs(
                engine.getDBAdaptor().getTableNameGenerator().getVariantTableName(),
                columnsToCopyMap,
                columnsToInclude,
                engine.getOptions()), "Copy partial columns into virtual file");

        if (params.fixPhoenix) {
            mm.fileMetadataIterator(studyId).forEachRemaining(fileMetadata -> {
                if (fileMetadata.getType() == FileMetadata.Type.PARTIAL) {
                    PhoenixHelper.Column fileColumn = VariantPhoenixSchema.getFileColumn(studyId, fileMetadata.getId());
                    phoenixColumnsToDrop.add(fileColumn.column());
                }
            });
        }

        logger.info("[" + ++step + "/" + steps + "] Drop columns from phoenix");
        // Phoenix - drop columns
        Connection connection = engine.getDBAdaptor().openJdbcConnection();
        PhoenixHelper phoenixHelper = new PhoenixHelper(engine.getDBAdaptor().getConfiguration());
        phoenixHelper.dropColumns(connection, engine.getVariantTableName(), phoenixColumnsToDrop, DEFAULT_TABLE_TYPE);
        connection.commit();

        VariantPhoenixSchemaManager schemaManager = new VariantPhoenixSchemaManager(engine.getDBAdaptor());
        schemaManager.registerNewFiles(studyId, Collections.singletonList(virtualFile.getId()));
        schemaManager.close();

        logger.info("[" + ++step + "/" + steps + "] Associate virtual files");
        // Metadata
        mm.associatePartialFiles(studyId, params.virtualFile, params.files);
    }
}
