package org.opencb.opencga.storage.hadoop.variant.migration.v2_0_0;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.migration.v2_0_0.VariantStorage200MigrationToolExecutor;
import org.opencb.opencga.core.tools.migration.v2_0_0.VariantStorage200MigrationToolParams;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantStorageToolExecutor;
import org.opencb.opencga.storage.hadoop.variant.annotation.pending.AnnotationPendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.core.metadata.models.TaskMetadata.Status;

@ToolExecutor(id = "hbase-mapreduce", tool = "variant-storage-migration-2.0.0",
        framework = ToolExecutor.Framework.MAP_REDUCE,
        source = ToolExecutor.Source.HBASE)
public class HadoopVariantStorage200MigrationToolExecutor extends VariantStorage200MigrationToolExecutor
        implements HadoopVariantStorageToolExecutor {

    public static final String MIGRATION_KEY = "migration_2.0.0";
    public static final int MIGRATION_VALUE = 1;

    private Logger logger = LoggerFactory.getLogger(HadoopVariantStorage200MigrationToolExecutor.class);

    @Override
    protected void run() throws Exception {
        VariantStorage200MigrationToolParams toolParams = getToolParams();

        HadoopVariantStorageEngine engine = getHadoopVariantStorageEngine();
        if (!engine.getMetadataManager().exists()) {
            logger.info("Project '{}' not defined in the Variant Storage. Nothing to do!", toolParams.getProject());
            return;
        }
        ProjectMetadata projectMetadata = engine.getMetadataManager().getProjectMetadata();

        int migration = projectMetadata.getAttributes().getInt(MIGRATION_KEY, 0);
        if (toolParams.isForceMigration() || migration < MIGRATION_VALUE) {
            boolean partialMigration = StringUtils.isNotEmpty(toolParams.getRegion()) || !toolParams.isRemoveSpanDeletions();
            ObjectMap objectMap = new ObjectMap(engine.getOptions());
            objectMap.put(VariantMigration200Driver.REMOVE_SPAN_DELETIONS, toolParams.isRemoveSpanDeletions());
            objectMap.putIfNotEmpty(VariantQueryParam.REGION.key(), toolParams.getRegion());

            engine.getMRExecutor().run(VariantMigration200Driver.class, VariantMigration200Driver.buildArgs(
                    engine.getVariantTableName(), objectMap), "Variant storage migration v2.0.0");

            if (partialMigration) {
                logger.info("Partial migration");
                addAttribute("partialMigration", true);
            } else {
                // If span deletion table is not empty, invalidate sample index
                String deletedSpanDeletionsTable = engine.getVariantTableName() + "_old_span_del";
                boolean hadSpanDeletionVariants = engine.getDBAdaptor().getHBaseManager().act(deletedSpanDeletionsTable, table -> {
                    ResultScanner scanner = table.getScanner(new Scan().setBatch(1));
                    Result result = scanner.next();
                    scanner.close();
                    return result != null;
                });
                if (hadSpanDeletionVariants) {
                    VariantStorageMetadataManager metadataManager = engine.getMetadataManager();
                    for (Map.Entry<String, Integer> entry : metadataManager.getStudies().entrySet()) {
                        List<Integer> samplesToModify = new ArrayList<>(1024);
                        Integer studyId = entry.getValue();
                        metadataManager.sampleMetadataIterator(studyId).forEachRemaining(sampleMetadata -> {
                            if (sampleMetadata.isIndexed()
                                    && (SampleIndexDBAdaptor.getSampleIndexStatus(sampleMetadata, 1).equals(Status.READY)
                                    || SampleIndexDBAdaptor.getSampleIndexAnnotationStatus(sampleMetadata, 1).equals(Status.READY))) {
                                samplesToModify.add(sampleMetadata.getId());
                            }
                        });
                        for (Integer sampleId : samplesToModify) {
                            metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                                SampleIndexDBAdaptor.setSampleIndexStatus(sampleMetadata, Status.NONE, 0);
                                SampleIndexDBAdaptor.setSampleIndexAnnotationStatus(sampleMetadata, Status.NONE, 0);
                                return sampleMetadata;
                            });
                        }
                    }
                    AnnotationPendingVariantsManager pendingVariantsManager = new AnnotationPendingVariantsManager(engine.getDBAdaptor());
                    pendingVariantsManager.deleteTable();
                }

                // Only update if executing the full migration
                engine.getMetadataManager().updateProjectMetadata(p -> {
                    p.getAttributes().put(MIGRATION_KEY, MIGRATION_VALUE);
                    return p;
                });
            }
        }
    }
}

