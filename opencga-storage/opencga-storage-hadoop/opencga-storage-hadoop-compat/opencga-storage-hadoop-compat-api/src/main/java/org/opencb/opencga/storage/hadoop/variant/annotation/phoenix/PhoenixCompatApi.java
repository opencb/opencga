package org.opencb.opencga.storage.hadoop.variant.annotation.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.mapreduce.PhoenixRecordReader;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;

import java.sql.SQLException;
import java.util.List;

public interface PhoenixCompatApi {

    PTable makePTable(List<PColumn> columns) throws SQLException;

    <T extends DBWritable> PhoenixRecordReader<T> newPhoenixRecordReader(
            Class<T> inputClass, Configuration configuration, QueryPlan queryPlan);

    boolean isDropColumnFromViewSupported();
}
