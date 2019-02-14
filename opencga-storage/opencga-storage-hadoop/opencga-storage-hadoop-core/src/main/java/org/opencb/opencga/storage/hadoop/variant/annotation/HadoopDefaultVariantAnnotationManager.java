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
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.hadoop.utils.CopyHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.utils.DeleteHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopDefaultVariantAnnotationManager extends DefaultVariantAnnotationManager {

    public static final String SKIP_DISCOVER_VARIANTS_TO_ANNOTATE = "skipDiscoverVariantsToAnnotate";
    public static final String SKIP_PENDING_ANNOTATIONS_TABLE = "skipPendingAnnotationsTable";

    private final VariantHadoopDBAdaptor dbAdaptor;
    private final ObjectMap baseOptions;
    private final MRExecutor mrExecutor;
    private Query query;

    public HadoopDefaultVariantAnnotationManager(VariantAnnotator variantAnnotator, VariantHadoopDBAdaptor dbAdaptor,
                                                 MRExecutor mrExecutor, ObjectMap options) {
        super(variantAnnotator, dbAdaptor);
        this.mrExecutor = mrExecutor;
        this.dbAdaptor = dbAdaptor;
        baseOptions = options;
    }

    @Override
    public void annotate(Query query, ObjectMap params) throws VariantAnnotatorException, IOException, StorageEngineException {
        this.query = query == null ? new Query() : query;

        // Do not allow FILE filter when annotating.
        this.query.remove(VariantQueryParam.FILE.key());

        super.annotate(this.query, params);
    }

    @Override
    protected void preAnnotate(Query query, boolean doCreate, boolean doLoad, ObjectMap params) throws StorageEngineException {
        super.preAnnotate(query, doCreate, doLoad, params);

        if (doCreate) {
            if (params.getBoolean(SKIP_DISCOVER_VARIANTS_TO_ANNOTATE, false)) {
                logger.info("Skip run MapReduce to discover variants to annotate.");
            } else {
                mrExecutor.run(DiscoverPendingVariantsToAnnotateDriver.class,
                        DiscoverPendingVariantsToAnnotateDriver.buildArgs(dbAdaptor.getVariantTable(), params),
                        params, "Prepare variants to annotate");
            }
        }
    }

    @Override
    protected DataReader<Variant> getVariantDataReader(Query query, QueryOptions iteratorQueryOptions, ObjectMap params) {
        if (params.getBoolean(SKIP_PENDING_ANNOTATIONS_TABLE, false)) {
            return super.getVariantDataReader(query, iteratorQueryOptions, params);
        } else {
            return new PendingVariantsToAnnotateReader(dbAdaptor, query);
        }
    }

    protected long countVariantsToAnnotate(Query query, ObjectMap params) {
        if (params.getBoolean(SKIP_PENDING_ANNOTATIONS_TABLE, false)) {
            return super.countVariantsToAnnotate(query, params);
        } else {
            return 0;
        }
    }

    @Override
    protected ParallelTaskRunner<VariantAnnotation, ?> buildLoadAnnotationParallelTaskRunner(
            DataReader<VariantAnnotation> reader, ParallelTaskRunner.Config config, ProgressLogger progressLogger, ObjectMap params) {

        if (VariantPhoenixHelper.DEFAULT_TABLE_TYPE == PTableType.VIEW
                || params.getBoolean(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, false)) {
            int currentAnnotationId = dbAdaptor.getMetadataManager().getProjectMetadata()
                    .getAnnotation().getCurrent().getId();
            VariantAnnotationToHBaseConverter hBaseConverter =
                    new VariantAnnotationToHBaseConverter(dbAdaptor.getGenomeHelper(), progressLogger, currentAnnotationId);
            AnnotationIndexDBLoader annotationIndexDBLoader = new AnnotationIndexDBLoader(
                    dbAdaptor.getHBaseManager(), dbAdaptor.getTableNameGenerator().getAnnotationIndexTableName());

            Task<VariantAnnotation, Put> task = Task.join(hBaseConverter, annotationIndexDBLoader.asTask(true));

            VariantAnnotationHadoopDBWriter writer = new VariantAnnotationHadoopDBWriter(
                    dbAdaptor.getHBaseManager(),
                    dbAdaptor.getVariantTable(),
                    dbAdaptor.getGenomeHelper().getColumnFamily());
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

        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        SampleIndexAnnotationLoader indexAnnotationLoader = new SampleIndexAnnotationLoader(
                dbAdaptor.getGenomeHelper(),
                dbAdaptor.getHBaseManager(),
                dbAdaptor.getTableNameGenerator(),
                metadataManager);

        List<Integer> studies = VariantQueryUtils.getIncludeStudies(query, null, metadataManager);

        List<String> samples = params.getAsStringList("sampleIndexAnnotation");

        if (samples.size() == 1 && (samples.get(0).equals(VariantQueryUtils.NONE) || samples.get(0).equals("skip"))) {
            // Nothing to do!
            return;
        } else if (samples.isEmpty() || samples.size() == 1 && samples.get(0).equals(VariantQueryUtils.ALL)) {
            // Run on all pending samples
            for (Integer studyId : studies) {
                List<Integer> indexedSamples = metadataManager.getIndexedSamples(studyId);
                if (!indexedSamples.isEmpty()) {
                    indexAnnotationLoader.updateSampleAnnotation(studyId, indexedSamples);
                }
            }
        } else if (samples.size() == 1 && samples.get(0).equals("force_all")) {
            // Run on all indexed samples
            for (Integer studyId : studies) {
                List<Integer> indexedSamples = metadataManager.getIndexedSamples(studyId);
                if (!indexedSamples.isEmpty()) {
                    indexAnnotationLoader.updateSampleAnnotation(studyId, indexedSamples);
                }
            }
        } else {
            for (Integer studyId : studies) {
                List<Integer> sampleIds = new ArrayList<>(samples.size());
                for (String sample : samples) {
                    Integer sampleId = metadataManager.getSampleId(studyId, sample);
                    if (sampleId != null) {
                        sampleIds.add(sampleId);
                    }
                }
                if (!sampleIds.isEmpty()) {
                    indexAnnotationLoader.updateSampleAnnotation(studyId, sampleIds);
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

        ProjectMetadata projectMetadata = dbAdaptor.getMetadataManager().lockAndUpdateProject(project -> {
            registerNewAnnotationSnapshot(name, variantAnnotator, project);
            return project;
        });

        ProjectMetadata.VariantAnnotationMetadata annotationMetadata = projectMetadata.getAnnotation().getSaved(name);


        String columnFamily = Bytes.toString(dbAdaptor.getGenomeHelper().getColumnFamily());
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

        String columnFamily = Bytes.toString(dbAdaptor.getGenomeHelper().getColumnFamily());
        String targetColumn = VariantPhoenixHelper.getAnnotationSnapshotColumn(saved.getId());

        String[] args = DeleteHBaseColumnDriver.buildArgs(
                dbAdaptor.getTableNameGenerator().getVariantTableName(),
                Collections.singletonList(columnFamily + ':' + targetColumn), options);

        mrExecutor.run(DeleteHBaseColumnDriver.class, args, options, "Delete annotation snapshot '" + name + '\'');

        dbAdaptor.getMetadataManager().lockAndUpdateProject(project -> {
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
