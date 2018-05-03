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

package org.opencb.opencga.storage.hadoop.variant.annotation.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.AbstractHBaseVariantMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by mh719 on 15/12/2016.
 */
public class AnalysisAnnotateMapper extends AbstractHBaseVariantMapper<NullWritable, PhoenixVariantAnnotationWritable> {
    private Logger logger = LoggerFactory.getLogger(AnalysisAnnotateMapper.class);
    public static final String CONFIG_VARIANT_TABLE_ANNOTATE_FORCE = "opencga.variant.table.annotate.force";

    private VariantAnnotator variantAnnotator;
    private boolean forceAnnotation;
    private VariantAnnotationToPhoenixConverter annotationConverter;
    private VariantPhoenixHelper.VariantColumn[] columnsOrdered;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.forceAnnotation = context.getConfiguration().getBoolean(CONFIG_VARIANT_TABLE_ANNOTATE_FORCE, false);

        /* Annotation -> Phoenix converter */
        annotationConverter = new VariantAnnotationToPhoenixConverter(getHelper().getColumnFamily());
        columnsOrdered = VariantPhoenixHelper.VariantColumn.values();

        /* Annotator config */
        String configFile = "storage-configuration.yml";
        String storageEngine = "hadoop"; //
        ObjectMap options = new ObjectMap(); // empty
        try {
            StorageConfiguration storageConfiguration = StorageConfiguration.load(
                StorageConfiguration.class.getClassLoader().getResourceAsStream(configFile));
            this.variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, storageEngine, options);
        } catch (Exception e) {
            throw new IllegalStateException("Problems loading storage configuration from " + configFile, e);
        }
    }
    private final CopyOnWriteArrayList<Variant> variantsToAnnotate = new CopyOnWriteArrayList<>();

    private void annotateVariants(Context context, boolean force) throws IOException, InterruptedException, VariantAnnotatorException {
        if (this.variantsToAnnotate.isEmpty()) {
            return;
        }
        // not enough data
        if (this.variantsToAnnotate.size() < 200 && !force) {
            return;
        }
        long start = System.nanoTime();
        logger.info("Annotate {} variants ... ", this.variantsToAnnotate.size());
        List<VariantAnnotation> annotate = this.variantAnnotator.annotate(this.variantsToAnnotate);
        logger.info("Submit {} [annot time: {}] ... ", annotate.size(), System.nanoTime() - start);
        start = System.nanoTime();
        for (VariantAnnotation annotation : annotate) {
            Map<PhoenixHelper.Column, ?> columnMap = annotationConverter.convert(annotation);
            List<Object> orderedValues = toOrderedList(columnMap);
            PhoenixVariantAnnotationWritable writeable = new PhoenixVariantAnnotationWritable(orderedValues);
            context.getCounter("opencga", "variant.annotate.submit").increment(1);
            context.write(NullWritable.get(), writeable);
        }
        logger.info("Done [submit time: {}] ... ", System.nanoTime() - start);
        this.variantsToAnnotate.clear();
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);
        try {
            while (context.nextKeyValue()) {
                this.map(context.getCurrentKey(), context.getCurrentValue(), context);
                annotateVariants(context, false);
            }
            annotateVariants(context, true);
        } catch (VariantAnnotatorException e) {
            throw new RuntimeException(e);
        } finally {
            this.cleanup(context);
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {
        String hexBytes = Bytes.toHex(key.get());
        Cell[] cells = value.rawCells();
        try {
            if (cells.length < 2) {
                context.getCounter("opencga", "row.empty").increment(1);
                return;
            }
            context.getCounter("opencga", "variant.read").increment(1);
            logger.info("Convert ... ");
            long start = System.nanoTime();
            Variant variant = this.getHbaseToVariantConverter().convert(value);
            if (!requireAnnotation(variant)) {
                context.getCounter("opencga", "variant.no-annotation-required").increment(1);
                return; // No annotation needed
            }
            logger.info("Add to annotate set {} [convert time: {}] ... ", variant, System.nanoTime() - start);
            variantsToAnnotate.add(variant);

        } catch (Exception e) {
            throw new IllegalStateException("Problems with row [hex:" + hexBytes + "] for cells " + cells.length, e);
        }
    }

    private List<Object> toOrderedList(Map<PhoenixHelper.Column, ?> columnMap) {
        List<Object> orderedValues = new ArrayList<>(columnsOrdered.length);
        for (VariantPhoenixHelper.VariantColumn column : columnsOrdered) {
            Object columnValue = columnMap.get(column);
            if (columnValue != null) {
                if (column.getPDataType().isArrayType()) {
                    if (columnValue instanceof Collection) {
                        columnValue = toArray(column.getPDataType(), (Collection) columnValue);
                    } else {
                        throw new IllegalArgumentException("Column " + column + " is not a collection " + columnValue);
                    }
                }
                orderedValues.add(columnValue);
            } else {
                orderedValues.add(column.getPDataType().getSqlType());
            }
        }
        return orderedValues;
    }

    private Array toArray(PDataType elementDataType, Collection<?> input) {
        if (elementDataType.isArrayType()) {
            elementDataType = PDataType.arrayBaseType(elementDataType);
        }
        return new PhoenixArray(elementDataType, input.toArray(new Object[input.size()]));
    }

    private boolean requireAnnotation(Variant variant) {
        if (this.forceAnnotation) {
            return true;
        }
        VariantAnnotation annotation = variant.getAnnotation();
        if (annotation == null) {
            return true;
        }
        // Chromosome not set -> require annotation !!!!
        return StringUtils.isEmpty(annotation.getChromosome());
    }

    private boolean isEmpty(Collection<?> collection) {
        if (null == collection) {
            return true;
        }
        return collection.isEmpty();
    }
}
