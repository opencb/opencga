package org.opencb.opencga.storage.hadoop.variant.score;

import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.score.VariantScoreRemover;
import org.opencb.opencga.storage.hadoop.utils.DeleteHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.VariantsTableDeleteColumnMapper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;

import java.sql.SQLException;
import java.util.Collections;

public class HadoopVariantScoreRemover extends VariantScoreRemover {

    private final VariantHadoopDBAdaptor dbAdaptor;
    private final MRExecutor mrExecutor;

    public HadoopVariantScoreRemover(VariantHadoopDBAdaptor dbAdaptor, MRExecutor mrExecutor) {
        super(dbAdaptor.getMetadataManager());
        this.dbAdaptor = dbAdaptor;
        this.mrExecutor = mrExecutor;
    }

    @Override
    protected void remove(VariantScoreMetadata scoreMetadata, ObjectMap options) throws StorageEngineException {
        PhoenixHelper.Column column = VariantPhoenixSchema.getVariantScoreColumn(scoreMetadata.getStudyId(), scoreMetadata.getId());

        options = new ObjectMap(options);
        options.put(DeleteHBaseColumnDriver.DELETE_HBASE_COLUMN_MAPPER_CLASS, VariantsTableDeleteColumnMapper.class.getName());

        String[] args = DeleteHBaseColumnDriver.buildArgs(
                dbAdaptor.getVariantTable(),
                Collections.singletonList(Bytes.toString(GenomeHelper.COLUMN_FAMILY_BYTES) + ":" + column.column()),
                options);

        mrExecutor.run(DeleteHBaseColumnDriver.class, args, options);
    }

    @Override
    protected void postRemove(VariantScoreMetadata scoreMetadata, boolean success) throws StorageEngineException {
        if (success) {
            VariantPhoenixSchemaManager schemaManager = new VariantPhoenixSchemaManager(dbAdaptor);
            try {
                schemaManager.dropScore(scoreMetadata.getStudyId(), Collections.singletonList(scoreMetadata.getId()));
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }

        super.postRemove(scoreMetadata, success);
    }
}
