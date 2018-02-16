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
import org.apache.phoenix.schema.PTableType;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopDefaultVariantAnnotationManager extends DefaultVariantAnnotationManager {

    private final VariantHadoopDBAdaptor hadoopDBAdaptor;

    public HadoopDefaultVariantAnnotationManager(VariantAnnotator variantAnnotator, VariantDBAdaptor dbAdaptor) {
        super(variantAnnotator, dbAdaptor);
        hadoopDBAdaptor = (VariantHadoopDBAdaptor) this.dbAdaptor;
    }

    @Override
    protected ParallelTaskRunner<VariantAnnotation, ?> buildLoadAnnotationParallelTaskRunner(
            DataReader<VariantAnnotation> reader, ParallelTaskRunner.Config config, ProgressLogger progressLogger, ObjectMap params) {

        if (VariantPhoenixHelper.DEFAULT_TABLE_TYPE == PTableType.VIEW
                || params.getBoolean(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, false)) {
            VariantAnnotationToHBaseConverter task =
                    new VariantAnnotationToHBaseConverter(hadoopDBAdaptor.getGenomeHelper(), progressLogger);
            HBaseDataWriter<Put> writer = new HBaseDataWriter<>(hadoopDBAdaptor.getHBaseManager(), hadoopDBAdaptor.getVariantTable());
            return new ParallelTaskRunner<>(reader, task, writer, config);
        } else {
            return new ParallelTaskRunner<>(reader,
                    () -> hadoopDBAdaptor.newAnnotationLoader(new QueryOptions(params))
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
}
