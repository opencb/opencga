package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.schema.types.*;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.util.Collection;

/**
 * Created on 07/07/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractPhoenixConverter {

    protected abstract GenomeHelper getGenomeHelper();

    protected <T> void addNotNull(Collection<T> collection, T value) {
        if (value != null) {
            collection.add(value);
        }
    }

    protected <T> void addAllNotNull(Collection<T> collection, Collection<T> values) {
        if (values != null) {
            collection.addAll(values);
        }
    }

    protected void addVarcharArray(Put put, byte[] column, Collection<String> collection) {
        addArray(put, column, collection, PVarcharArray.INSTANCE);
    }

    protected void addIntegerArray(Put put, byte[] column, Collection<Integer> collection) {
        addArray(put, column, collection, PIntegerArray.INSTANCE);
    }

    protected void addFloatArray(Put put, byte[] column, Collection<Float> collection) {
        addArray(put, column, collection, PFloatArray.INSTANCE);
    }

    protected void addArray(Put put, byte[] column, Collection collection, PArrayDataType arrayType) {
        if (collection.size() == 0) {
            return;
        }
        byte[] arrayBytes = VariantPhoenixHelper.toBytes(collection, arrayType);
        put.addColumn(getGenomeHelper().getColumnFamily(), column, arrayBytes);
    }

    @SuppressWarnings("unchecked")
    protected void add(Put put, VariantPhoenixHelper.Column column, Object value) {
        add(put, column.bytes(), value, column.getPDataType());
    }

    protected <T> void add(Put put, byte[] column, T value, PDataType<T> dataType) {
        if (dataType.isArrayType()) {
            throw new IllegalArgumentException("Not expected array phoenix data type");
        }
        byte[] bytes = dataType.toBytes(value);
        put.addColumn(getGenomeHelper().getColumnFamily(), column, bytes);
    }

}
