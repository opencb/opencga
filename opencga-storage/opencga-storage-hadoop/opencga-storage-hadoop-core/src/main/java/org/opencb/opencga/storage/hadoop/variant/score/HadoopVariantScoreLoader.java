package org.opencb.opencga.storage.hadoop.variant.score;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.ParallelTaskRunner.Config;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnector;
import org.opencb.opencga.storage.core.io.plain.StringDataReader;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.opencb.opencga.storage.core.variant.score.VariantScoreLoader;
import org.opencb.opencga.storage.core.variant.score.VariantScoreParser;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

public class HadoopVariantScoreLoader extends VariantScoreLoader {

    private final VariantHadoopDBAdaptor dbAdaptor;

    public HadoopVariantScoreLoader(VariantHadoopDBAdaptor dbAdaptor, IOConnector ioConnector) {
        super(dbAdaptor.getMetadataManager(), ioConnector);
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    protected void load(URI scoreFile, VariantScoreMetadata scoreMetadata, VariantScoreFormatDescriptor descriptor, ObjectMap options)
            throws ExecutionException, IOException {
        StringDataReader stringReader = getDataReader(scoreFile);

        VariantScoreParser parser = newParser(scoreMetadata, descriptor);
        VariantScoreToHBaseConverter converter = new VariantScoreToHBaseConverter(
                GenomeHelper.COLUMN_FAMILY_BYTES,
                scoreMetadata.getStudyId(),
                scoreMetadata.getId());

        HBaseDataWriter<Put> hbaseWriter = new HBaseDataWriter<>(dbAdaptor.getHBaseManager(), dbAdaptor.getVariantTable());

        int numTasks = 4;
        ParallelTaskRunner<String, Put> ptr = new ParallelTaskRunner<>(
                stringReader,
                parser.then(converter),
                hbaseWriter,
                Config.builder().setBatchSize(100).setNumTasks(numTasks).build());

        ptr.run();
    }

    @Override
    protected VariantScoreMetadata postLoad(VariantScoreMetadata scoreMetadata, boolean success) throws StorageEngineException {
        if (success) {
            VariantPhoenixSchemaManager schemaManager = new VariantPhoenixSchemaManager(dbAdaptor);
            schemaManager.registerNewScore(scoreMetadata.getStudyId(), scoreMetadata.getId());
        }

        return super.postLoad(scoreMetadata, success);
    }
}
