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
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.hadoop.utils.CopyHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopDefaultVariantAnnotationManager extends DefaultVariantAnnotationManager {

    private final VariantHadoopDBAdaptor dbAdaptor;
    private final ObjectMap baseOptions;
    private final MRExecutor mrExecutor;

    public HadoopDefaultVariantAnnotationManager(VariantAnnotator variantAnnotator, VariantHadoopDBAdaptor dbAdaptor,
                                                 MRExecutor mrExecutor, ObjectMap options) {
        super(variantAnnotator, dbAdaptor);
        this.mrExecutor = mrExecutor;
        this.dbAdaptor = dbAdaptor;
        baseOptions = options;
    }

    @Override
    protected ParallelTaskRunner<VariantAnnotation, ?> buildLoadAnnotationParallelTaskRunner(
            DataReader<VariantAnnotation> reader, ParallelTaskRunner.Config config, ProgressLogger progressLogger, ObjectMap params) {

        if (VariantPhoenixHelper.DEFAULT_TABLE_TYPE == PTableType.VIEW
                || params.getBoolean(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, false)) {
            VariantAnnotationToHBaseConverter task =
                    new VariantAnnotationToHBaseConverter(dbAdaptor.getGenomeHelper(), progressLogger);
            HBaseDataWriter<Put> writer = new HBaseDataWriter<>(dbAdaptor.getHBaseManager(), dbAdaptor.getVariantTable());
            return new ParallelTaskRunner<>(reader, task, writer, config);
        } else {
            return new ParallelTaskRunner<>(reader,
                    () -> dbAdaptor.newAnnotationLoader(new QueryOptions(params))
                            .setProgressLogger(progressLogger), null, config);
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
    public void createAnnotationSnapshot(String name, ObjectMap inputOptions) throws StorageEngineException {
        QueryOptions options = new QueryOptions(baseOptions);
        if (inputOptions != null) {
            options.putAll(inputOptions);
        }
        String hadoopRoute = options.getString(HadoopVariantStorageEngine.HADOOP_BIN, "hadoop");
        String jar = HadoopVariantStorageEngine.getJarWithDependencies(options);

        Class execClass = CopyHBaseColumnDriver.class;
        String executable = hadoopRoute + " jar " + jar + ' ' + execClass.getName();
        String columnFamily = Bytes.toString(dbAdaptor.getGenomeHelper().getColumnFamily());
        String targetColumn = VariantPhoenixHelper.getAnnotationSnapshotColumn(name);
        Map<String, String> columnsToCopyMap = Collections.singletonMap(
                columnFamily + ':' + VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.column(),
                columnFamily + ':' + targetColumn);
        String[] args = CopyHBaseColumnDriver.buildArgs(
                dbAdaptor.getTableNameGenerator().getVariantTableName(),
                columnsToCopyMap, options);

        long startTime = System.currentTimeMillis();
        logger.info("------------------------------------------------------");
        logger.info("Copy current annotation into " + targetColumn);
        logger.debug(executable + ' ' + Arrays.toString(args));
        logger.info("------------------------------------------------------");
        int exitValue = mrExecutor.run(executable, args);
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
        if (exitValue != 0) {
            throw new StorageEngineException("Error creating snapshot of current annotation. "
                    + "Exception while copying column " + VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.column()
                    + " into " + targetColumn);
        }

    }

    @Override
    public void deleteAnnotationSnapshot(String name, ObjectMap options) throws StorageEngineException {
        super.deleteAnnotationSnapshot(name, options);
    }
}
