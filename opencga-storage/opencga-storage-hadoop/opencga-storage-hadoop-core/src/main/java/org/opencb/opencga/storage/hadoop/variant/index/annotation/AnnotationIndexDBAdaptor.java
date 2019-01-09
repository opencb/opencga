package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by jacobo on 04/01/19.
 */
public class AnnotationIndexDBAdaptor {

    private final HBaseManager hBaseManager;
    private final String tableName;

    public AnnotationIndexDBAdaptor(HBaseManager hBaseManager, String tableName) {
        this.hBaseManager = hBaseManager;
        this.tableName = tableName;
    }

    public List<Pair<Variant, Byte>> get(String chromosome, int start, int end) throws IOException {
        Iterator<Pair<Variant, Byte>> iterator = nativeIterator(new Scan(
                VariantPhoenixKeyFactory.generateVariantRowKey(chromosome, start),
                VariantPhoenixKeyFactory.generateVariantRowKey(chromosome, end)));
        List<Pair<Variant, Byte>> list = new LinkedList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    public Iterator<Pair<Variant, Byte>> iterator() throws IOException {
        return nativeIterator(new Scan());
    }

    private Iterator<Pair<Variant, Byte>> nativeIterator(Scan scan) throws IOException {
        return hBaseManager.act(tableName, table -> {
            return Iterators.transform(table.getScanner(scan).iterator(), AnnotationIndexConverter::getVariantBytePair);
        });
    }

}
