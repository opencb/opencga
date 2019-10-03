package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ExposedResultSetDBWritable implements DBWritable {

    private ResultSet resultSet;

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.resultSet = resultSet;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }
}
