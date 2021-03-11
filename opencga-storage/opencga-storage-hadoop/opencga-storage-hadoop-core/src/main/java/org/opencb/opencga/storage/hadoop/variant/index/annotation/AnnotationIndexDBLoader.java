package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jacobo on 04/01/19.
 */
public class AnnotationIndexDBLoader extends AbstractHBaseDataWriter<VariantAnnotation, Put> {

    private final AnnotationIndexConverter converter;

    public AnnotationIndexDBLoader(HBaseManager hBaseManager, String tableName, SampleIndexSchema schema) {
        super(hBaseManager, tableName);
        converter = new AnnotationIndexConverter(schema);
    }

    @Override
    public boolean open() {
        super.open();

        try {
            AnnotationIndexDBAdaptor.createTableIfNeeded(hBaseManager, tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return true;
    }

    @Override
    protected List<Put> convert(List<VariantAnnotation> batch) {
        List<Put> puts = new ArrayList<>(batch.size());
        for (VariantAnnotation variantAnnotation : batch) {
            puts.add(converter.convertToPut(variantAnnotation));
        }
        return puts;
    }
}
