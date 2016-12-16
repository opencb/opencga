package org.opencb.opencga.storage.hadoop.variant.annotation;

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
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotator;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.sql.Array;
import java.util.*;

/**
 * Created by mh719 on 15/12/2016.
 */
public class AnalysisAnnotateMapper extends AbstractHBaseMapReduce<NullWritable, PhoenixVariantAnnotationWritable> {
    public static final String CONFIG_VARIANT_TABLE_ANNOTATE_FORCE = "opencga.variant.table.annotate.force";

    private VariantAnnotator variantAnnotator;
    private byte[] studiesRow;
    private boolean forceAnnotation;
    private VariantAnnotationToHBaseConverter annotationConverter;
    private VariantPhoenixHelper.VariantColumn[] columnsOrdered;

//    static { // get Driver log
//        DriverManager.setLogWriter(new PrintWriter(System.err));
//    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.forceAnnotation = context.getConfiguration().getBoolean(CONFIG_VARIANT_TABLE_ANNOTATE_FORCE, false);
        studiesRow = getHelper().generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);

        /* Annotation -> Phoenix converter */
        annotationConverter = new VariantAnnotationToHBaseConverter(getHelper());
        columnsOrdered = VariantPhoenixHelper.VariantColumn.values();

        /* Annotator config */
        String configFile = "storage-configuration.yml";
        String storageEngine = "hadoop"; //
        ObjectMap options = new ObjectMap(); // empty
        try {
            StorageConfiguration storageConfiguration = StorageConfiguration.load(
                StorageConfiguration.class.getClassLoader().getResourceAsStream(configFile));
            this.variantAnnotator = VariantAnnotationManager.buildVariantAnnotator(storageConfiguration, storageEngine, options);
        } catch (Exception e) {
            throw new IllegalStateException("Problems loading storage configuration from " + configFile, e);
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
            if (!Bytes.startsWith(value.getRow(), this.studiesRow)) { // ignore _METADATA row
                context.getCounter("opencga", "variant.read").increment(1);
                Variant variant = this.getHbaseToVariantConverter().convert(value);
                if (!requireAnnotation(variant)) {
                    context.getCounter("opencga", "variant.no-annotation-required").increment(1);
                    return; // No annotation needed
                }
                List<VariantAnnotation> annotate = this.variantAnnotator.annotate(Collections.singletonList(variant));
                for (VariantAnnotation annotation : annotate) {
                    Map<PhoenixHelper.Column, ?> columnMap = annotationConverter.convert(annotation);
                    List<Object> orderedValues = toOrderedList(columnMap);
                    PhoenixVariantAnnotationWritable writeable = new PhoenixVariantAnnotationWritable(orderedValues);
                    context.getCounter("opencga", "variant.annotate.submit").increment(1);
                    context.write(NullWritable.get(), writeable);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Problems with row [hex:" + hexBytes + "] for cells " + cells.length, e);
        }
    }

    private List<Object> toOrderedList(Map<PhoenixHelper.Column, ?> columnMap) {
        List<Object> orderedValues = new ArrayList<>(columnsOrdered.length);
        for (VariantPhoenixHelper.VariantColumn column : columnsOrdered) {
            Object columnValue = columnMap.get(column);
            if (column.getPDataType().isArrayType()) {
                if (columnValue instanceof Collection) {
                    columnValue = toArray(column.getPDataType(), (Collection) columnValue);
                } else {
                    throw new IllegalArgumentException("Column " + column + " is not a collection " + columnValue);
                }
            }
            orderedValues.add(columnValue);
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
