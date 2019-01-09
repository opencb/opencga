package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.ANNOTATION_INDEX_TABLE_COMPRESSION;

/**
 * Created by jacobo on 04/01/19.
 */
public class AnnotationIndexDBLoader extends AbstractHBaseDataWriter<VariantAnnotation, Put> {

    private final AnnotationIndexConverter converter;

    public AnnotationIndexDBLoader(HBaseManager hBaseManager, String tableName) {
        super(hBaseManager, tableName);
        converter = new AnnotationIndexConverter();
    }

    @Override
    public boolean open() {
        super.open();

        try {
            hBaseManager.createTableIfNeeded(tableName, AnnotationIndexConverter.COLUMN_FMAILY,
                    Compression.getCompressionAlgorithmByName(
                            hBaseManager.getConf().get(ANNOTATION_INDEX_TABLE_COMPRESSION, Compression.Algorithm.SNAPPY.getName())));
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
