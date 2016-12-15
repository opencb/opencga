package org.opencb.opencga.storage.hadoop.variant.annotation;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by mh719 on 15/12/2016.
 */
public class PhoenixVariantAnnotationWritable implements DBWritable, Writable {
    private final List<Object> orderedValues;

    public PhoenixVariantAnnotationWritable(final List<Object> orderedValues) {
        this.orderedValues = orderedValues;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // do nothing
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // do nothing
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        Integer i = 1;
        for (Object value : this.orderedValues) {
            preparedStatement.setObject(i++, value);
        }
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        // do nothing
    }
}
