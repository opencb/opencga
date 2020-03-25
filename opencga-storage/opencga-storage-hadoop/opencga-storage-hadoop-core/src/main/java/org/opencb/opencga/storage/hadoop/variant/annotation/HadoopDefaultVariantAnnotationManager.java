/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PTableType;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.hadoop.utils.CopyHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.utils.DeleteHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.annotation.pending.DiscoverPendingVariantsToAnnotateDriver;
import org.opencb.opencga.storage.hadoop.variant.annotation.pending.PendingVariantsToAnnotateReader;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexDBLoader;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexAnnotationLoader;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopDefaultVariantAnnotationManager extends DefaultVariantAnnotationManager {

    /**
     * Skip MapReduce operation to discover pending variants to annotate.
     */
    public static final String SKIP_DISCOVER_PENDING_VARIANTS_TO_ANNOTATE = "skipDiscoverPendingVariantsToAnnotate";
    /**
     * Do not use the pending variants to annotate table.
     */
    public static final String SKIP_PENDING_VARIANTS_TO_ANNOTATE_TABLE = "skipPendingVariantsToAnnotateTable";

    private final VariantHadoopDBAdaptor dbAdaptor;
    private final ObjectMap baseOptions;
    private final MRExecutor mrExecutor;
    private Query query;

    public HadoopDefaultVariantAnnotationManager(VariantAnnotator variantAnnotator, VariantHadoopDBAdaptor dbAdaptor,
                                                 MRExecutor mrExecutor, ObjectMap options, IOConnectorProvider ioConnectorProvider) {
        super(variantAnnotator, dbAdaptor, ioConnectorProvider);
        this.mrExecutor = mrExecutor;
        this.dbAdaptor = dbAdaptor;
        baseOptions = options;
    }

    @Override
    public long annotate(Query query, ObjectMap params) throws VariantAnnotatorException, IOException, StorageEngineException {
        this.query = query == null ? new Query() : query;

        // Do not allow FILE filter when annotating.
        this.query.remove(VariantQueryParam.FILE.key());

        if (!skipPendingVariantsToAnnotateTable(params)) {
            params.put(QueryOptions.SKIP_COUNT, true);
        }

        return super.annotate(this.query, params);
    }

    @Override
    protected void preAnnotate(Query query, boolean doCreate, boolean doLoad, ObjectMap params) throws StorageEngineException {
        super.preAnnotate(query, doCreate, doLoad, params);

        if (doCreate) {
            Set<VariantQueryParam> queryParams = VariantQueryUtils.validParams(query, true);
            queryParams.remove(VariantQueryParam.ANNOTATION_EXISTS);
            boolean annotateAll = queryParams.isEmpty();

            if (skipDiscoverPendingVariantsToAnnotate(params)) {
                logger.info("Skip MapReduce to discover variants to annotate.");
            } else {
                ProjectMetadata projectMetadata = dbAdaptor.getMetadataManager().getProjectMetadata();
                long lastLoadedFileTs = projectMetadata.getAttributes()
                        .getLong(HadoopVariantStorageEngine.LAST_LOADED_FILE_TS);
                long lastVariantsToAnnotateUpdateTs = projectMetadata.getAttributes()
                        .getLong(HadoopVariantStorageEngine.LAST_VARIANTS_TO_ANNOTATE_UPDATE_TS);

                // Skip MR if no file has been loaded since the last execution
                if (lastVariantsToAnnotateUpdateTs > lastLoadedFileTs) {
                    logger.info("Skip MapReduce to discover variants to annotate. List of pending annotations to annotate is updated");
                } else {
                    long ts = System.currentTimeMillis();

                    // Append all query to params to use the same filter also at the MR
                    params = new ObjectMap(params).appendAll(query);
                    mrExecutor.run(DiscoverPendingVariantsToAnnotateDriver.class,
                            DiscoverPendingVariantsToAnnotateDriver.buildArgs(dbAdaptor.getVariantTable(), params),
                            params, "Prepare variants to annotate");

                    if (annotateAll) {
                        dbAdaptor.getMetadataManager().updateProjectMetadata(pm -> {
                            pm.getAttributes().put(HadoopVariantStorageEngine.LAST_VARIANTS_TO_ANNOTATE_UPDATE_TS, ts);
                            return pm;
                        });
                    }
                }
            }
        }
    }

    @Override
    protected DataReader<Variant> getVariantDataReader(Query query, QueryOptions iteratorQueryOptions, ObjectMap params) {
        if (skipPendingVariantsToAnnotateTable(params)) {
            logger.info("Reading variants to annotate from variants table");
            return super.getVariantDataReader(query, iteratorQueryOptions, params);
        } else {
            logger.info("Reading variants to annotate from pending variants to annotate");
            return new PendingVariantsToAnnotateReader(dbAdaptor, query);
        }
    }

    protected long countVariantsToAnnotate(Query query, ObjectMap params) {
        if (skipPendingVariantsToAnnotateTable(params)) {
            return super.countVariantsToAnnotate(query, params);
        } else {
            throw new UnsupportedOperationException("");
        }
    }

    private boolean skipDiscoverPendingVariantsToAnnotate(ObjectMap params) {
        // Skip if overwriting annotations, or if specific param
        return params.getBoolean(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false)
                || params.getBoolean(SKIP_DISCOVER_PENDING_VARIANTS_TO_ANNOTATE, false);
    }

    private boolean skipPendingVariantsToAnnotateTable(ObjectMap params) {
        // Skip if overwriting annotations, or if specific param
        return params.getBoolean(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false)
                || params.getBoolean(SKIP_PENDING_VARIANTS_TO_ANNOTATE_TABLE, false);
    }

    @Override
    protected ParallelTaskRunner<VariantAnnotation, ?> buildLoadAnnotationParallelTaskRunner(
            DataReader<VariantAnnotation> reader, ParallelTaskRunner.Config config, ProgressLogger progressLogger, ObjectMap params) {

        if (VariantPhoenixHelper.DEFAULT_TABLE_TYPE == PTableType.VIEW
                || params.getBoolean(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), false)) {
            int currentAnnotationId = dbAdaptor.getMetadataManager().getProjectMetadata()
                    .getAnnotation().getCurrent().getId();
            VariantAnnotationToHBaseConverter hBaseConverter =
                    new VariantAnnotationToHBaseConverter(progressLogger, currentAnnotationId);
            AnnotationIndexDBLoader annotationIndexDBLoader = new AnnotationIndexDBLoader(
                    dbAdaptor.getHBaseManager(), dbAdaptor.getTableNameGenerator().getAnnotationIndexTableName());

            Task<VariantAnnotation, Put> task = Task.join(hBaseConverter, annotationIndexDBLoader.asTask(true));

            VariantAnnotationHadoopDBWriter writer = new VariantAnnotationHadoopDBWriter(
                    dbAdaptor.getHBaseManager(),
                    dbAdaptor.getTableNameGenerator(),
                    GenomeHelper.COLUMN_FAMILY_BYTES);
            return new ParallelTaskRunner<>(reader, task, writer, config);
        } else {
            return new ParallelTaskRunner<>(reader,
                    () -> dbAdaptor.newAnnotationLoader(new QueryOptions(params))
                            .setProgressLogger(progressLogger), null, config);
        }
    }

    @Override
    public void loadVariantAnnotation(URI uri, ObjectMap params) throws IOException, StorageEngineException {
        super.loadVariantAnnotation(uri, params);

        updateSampleIndexAnnotation(params);
    }

    protected void updateSampleIndexAnnotation(ObjectMap params) throws IOException, StorageEngineException {
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        SampleIndexAnnotationLoader indexAnnotationLoader = new SampleIndexAnnotationLoader(
                dbAdaptor.getHBaseManager(),
                dbAdaptor.getTableNameGenerator(),
                metadataManager, mrExecutor);

        List<Integer> studies = VariantQueryProjectionParser.getIncludeStudies(query, null, metadataManager);

        boolean skipSampleIndexAnnotation = params.getBoolean("skipSampleIndexAnnotation");

        if (skipSampleIndexAnnotation) {
            logger.info("Skip Sample Index Annotation");
            // Nothing to do!
            return;
        } else {
            // Run on all pending samples
            for (Integer studyId : studies) {
                Set<Integer> samplesToUpdate = new HashSet<>();
                for (Integer file : filesToBeAnnotated.getOrDefault(studyId, Collections.emptyList())) {
                    samplesToUpdate.addAll(metadataManager.getFileMetadata(studyId, file).getSamples());
                }
                for (Integer file : alreadyAnnotatedFiles.getOrDefault(studyId, Collections.emptyList())) {
                    samplesToUpdate.addAll(metadataManager.getFileMetadata(studyId, file).getSamples());
                }
                if (!samplesToUpdate.isEmpty()) {
                    indexAnnotationLoader.updateSampleAnnotation(studyId, new ArrayList<>(samplesToUpdate), params);
                }
            }
        }
    }

    @Override
    protected QueryOptions getIteratorQueryOptions(Query query, ObjectMap params) {
        QueryOptions iteratorQueryOptions = super.getIteratorQueryOptions(query, params);
        if (!VariantQueryUtils.isValidParam(query, VariantQueryParam.FILE)
                || !VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOTATION_EXISTS)) {
            iteratorQueryOptions.putIfAbsent(VariantHadoopDBAdaptor.NATIVE, true);
        }
        return iteratorQueryOptions;
    }

    @Override
    public void saveAnnotation(String name, ObjectMap inputOptions) throws StorageEngineException, VariantAnnotatorException {
        QueryOptions options = getOptions(inputOptions);

        ProjectMetadata projectMetadata = dbAdaptor.getMetadataManager().updateProjectMetadata(project -> {
            registerNewAnnotationSnapshot(name, variantAnnotator, project);
            return project;
        });

        ProjectMetadata.VariantAnnotationMetadata annotationMetadata = projectMetadata.getAnnotation().getSaved(name);


        String columnFamily = Bytes.toString(GenomeHelper.COLUMN_FAMILY_BYTES);
        String targetColumn = VariantPhoenixHelper.getAnnotationSnapshotColumn(annotationMetadata.getId());
        Map<String, String> columnsToCopyMap = Collections.singletonMap(
                columnFamily + ':' + VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.column(),
                columnFamily + ':' + targetColumn);
        String[] args = CopyHBaseColumnDriver.buildArgs(
                dbAdaptor.getTableNameGenerator().getVariantTableName(),
                columnsToCopyMap, options);

        mrExecutor.run(CopyHBaseColumnDriver.class, args, options, "Create new annotation snapshot with name '" + name + '\'');
    }

    @Override
    public void deleteAnnotation(String name, ObjectMap inputOptions) throws StorageEngineException, VariantAnnotatorException {
        QueryOptions options = getOptions(inputOptions);

        ProjectMetadata.VariantAnnotationMetadata saved = dbAdaptor.getMetadataManager().getProjectMetadata()
                .getAnnotation().getSaved(name);

        String columnFamily = Bytes.toString(GenomeHelper.COLUMN_FAMILY_BYTES);
        String targetColumn = VariantPhoenixHelper.getAnnotationSnapshotColumn(saved.getId());

        String[] args = DeleteHBaseColumnDriver.buildArgs(
                dbAdaptor.getTableNameGenerator().getVariantTableName(),
                Collections.singletonList(columnFamily + ':' + targetColumn), options);

        mrExecutor.run(DeleteHBaseColumnDriver.class, args, options, "Delete annotation snapshot '" + name + '\'');

        dbAdaptor.getMetadataManager().updateProjectMetadata(project -> {
            removeAnnotationSnapshot(name, project);
            return project;
        });
    }

    public QueryOptions getOptions(ObjectMap inputOptions) {
        QueryOptions options = new QueryOptions(baseOptions);
        if (inputOptions != null) {
            options.putAll(inputOptions);
        }
        return options;
    }
}
