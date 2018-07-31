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

package org.opencb.opencga.storage.hadoop.variant.converters;

import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.schema.types.*;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;

import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created on 07/07/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractPhoenixConverter {

    protected final byte[] columnFamily;

    public AbstractPhoenixConverter(byte[] columnFamily) {
        this.columnFamily = columnFamily;
    }

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
        byte[] arrayBytes = PhoenixHelper.toBytes(collection, arrayType);
        put.addColumn(columnFamily, column, arrayBytes);
    }

    @SuppressWarnings("unchecked")
    protected void add(Put put, PhoenixHelper.Column column, Object value) {
        add(put, column.bytes(), value, column.getPDataType());
    }

    protected <T> void add(Put put, byte[] column, T value, PDataType<T> dataType) {
        if (dataType.isArrayType()) {
            if (value instanceof Collection) {
                addArray(put, column, ((Collection) value), ((PArrayDataType) dataType));
                return;
            }
            if (!(value instanceof Array)) {
                throw new IllegalArgumentException("Not expected array phoenix data type");
            }
        }
        byte[] bytes = dataType.toBytes(value);
        put.addColumn(columnFamily, column, bytes);
    }

    public static boolean startsWith(byte[] bytes, byte[] startsWith) {
        if (bytes.length < startsWith.length) {
            return false;
        }
        for (int i = 0; i < startsWith.length; i++) {
            if (startsWith[i] != bytes[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean endsWith(byte[] bytes, byte[] endsWith) {
        if (bytes.length < endsWith.length) {
            return false;
        }
        for (int i = endsWith.length - 1, f = bytes.length - 1; i >= 0; i--, f--) {
            if (endsWith[i] != bytes[f]) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> toModifiableList(Array value) {
        return toModifiableList(value, 0, -1);
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> toModifiableList(Array value, int from, int to) {
        try {
            T[] array = (T[]) value.getArray();
            ArrayList<T> list = new ArrayList<>(array.length);
            to = to > 0 ? to : array.length;
            for (int i = from; i < to; i++) {
                T t = array[i];
                list.add(t);
            }
            return list;
//            return Arrays.asList((T[]) value.getArray());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> toList(Array value) {
        try {
            return Arrays.asList((T[]) value.getArray());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
